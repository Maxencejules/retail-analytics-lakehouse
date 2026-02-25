"""Airflow DAG: daily batch ETL orchestration with provider-native operators."""

from __future__ import annotations

from datetime import timedelta
import logging
import os
from pathlib import Path
from urllib.parse import urlparse

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.context import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from common.datasets import (
    GOLD_CUSTOMER_LTV_DATASET,
    GOLD_DAILY_REVENUE_DATASET,
    GOLD_TOP_PRODUCTS_DATASET,
    RAW_TRANSACTIONS_DATASET,
)
from common.notifications import sla_miss_callback, task_failure_callback
from common.run_metadata import (
    build_metadata_path,
    collect_deployment_metadata,
    derive_overall_status,
    summarize_task_states,
    write_json_record,
)

LOGGER = logging.getLogger("airflow.batch_etl_orchestration")

INGESTION_DATE_EXPR = (
    "{{ dag_run.conf.get('ingestion_date', ds) if dag_run and dag_run.conf else ds }}"
)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": task_failure_callback,
    "sla": timedelta(hours=2),
}


def _template_with_ingestion_date(path_template: str) -> str:
    return path_template.replace("{ds}", INGESTION_DATE_EXPR)


def _path_exists(path_value: str) -> bool:
    if path_value.startswith("s3://"):
        parsed = urlparse(path_value)
        bucket = parsed.netloc
        prefix = parsed.path.lstrip("/")
        if not bucket or not prefix:
            return False

        try:
            import boto3
        except ImportError as exc:  # pragma: no cover
            raise AirflowFailException(
                "boto3 is required to validate s3:// dependencies."
            ) from exc

        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return response.get("KeyCount", 0) > 0

    return Path(path_value).exists()


def _ensure_gold_outputs(base_path: str) -> None:
    required = (
        f"{base_path.rstrip('/')}/gold/daily_revenue_by_store",
        f"{base_path.rstrip('/')}/gold/top_10_products_by_day",
        f"{base_path.rstrip('/')}/gold/customer_lifetime_value",
    )

    for dataset_path in required:
        if dataset_path.startswith("s3://"):
            if not _path_exists(dataset_path):
                raise AirflowFailException(f"Missing Gold dataset: {dataset_path}")
            continue

        path = Path(dataset_path)
        parquet_files = list(path.rglob("*.parquet"))
        if not parquet_files:
            raise AirflowFailException(
                f"Gold dataset exists but has no parquet files: {dataset_path}"
            )


def _build_dependency_sensor(task_id: str, input_template: str):
    rendered_input = _template_with_ingestion_date(input_template)

    if rendered_input.startswith("s3://"):
        parsed = urlparse(rendered_input)
        bucket_name = parsed.netloc
        bucket_key = parsed.path.lstrip("/")
        return S3KeySensor(
            task_id=task_id,
            bucket_name=bucket_name,
            bucket_key=bucket_key,
            wildcard_match=True,
            poke_interval=300,
            timeout=60 * 60,
            mode="reschedule",
        )

    return FileSensor(
        task_id=task_id,
        filepath=rendered_input,
        fs_conn_id="fs_default",
        poke_interval=120,
        timeout=60 * 60,
        mode="reschedule",
    )


def _openlineage_spark_conf() -> dict[str, str]:
    enabled = os.getenv("OPENLINEAGE_ENABLED", "false").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not enabled:
        return {}

    return {
        "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
        "spark.openlineage.transport.type": "http",
        "spark.openlineage.transport.url": os.getenv(
            "OPENLINEAGE_URL", "http://marquez:5000/api/v1/lineage"
        ).strip(),
        "spark.openlineage.namespace": os.getenv(
            "OPENLINEAGE_NAMESPACE", "retail-analytics-lakehouse"
        ).strip(),
        "spark.openlineage.parentJobNamespace": "airflow",
        "spark.openlineage.parentJobName": "{{ dag.dag_id }}",
        "spark.openlineage.parentRunId": "{{ run_id }}",
    }


def _publish_run_metadata() -> None:
    context = get_current_context()
    dag_run = context["dag_run"]

    ingestion_date = context["ds"]
    if dag_run.conf and dag_run.conf.get("ingestion_date"):
        ingestion_date = str(dag_run.conf["ingestion_date"])

    task_states = summarize_task_states(
        dag_run,
        exclude_task_ids={"start", "finish", "publish_run_metadata"},
    )
    status = derive_overall_status(task_states)

    metadata_template = os.getenv(
        "AIRFLOW_RUN_METADATA_PATH_TEMPLATE",
        "data/ops/run_metadata/{dag_id}/ds={ds}/{run_id_safe}.json",
    )
    metadata_path = build_metadata_path(metadata_template, context)

    input_template = os.getenv(
        "AIRFLOW_RAW_INPUT_PATH_TEMPLATE",
        "data/generated/transactions.csv.gz",
    )
    output_base_path = os.getenv("AIRFLOW_OUTPUT_BASE_PATH", "data/lakehouse")
    data_interval_start = context.get("data_interval_start")
    data_interval_end = context.get("data_interval_end")
    logical_date = context.get("logical_date")

    record = {
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "status": status,
        "event_timestamp_utc": pendulum.now("UTC").isoformat(),
        "logical_date_utc": logical_date.isoformat() if logical_date else None,
        "data_interval_start_utc": (
            data_interval_start.isoformat() if data_interval_start else None
        ),
        "data_interval_end_utc": data_interval_end.isoformat() if data_interval_end else None,
        "ingestion_date": ingestion_date,
        "input_path": input_template.replace("{ds}", ingestion_date),
        "output_base_path": output_base_path,
        "task_states": task_states,
        "deployment": collect_deployment_metadata(),
    }
    write_json_record(record, metadata_path)
    LOGGER.info(
        "Published run metadata status=%s output_path=%s",
        status,
        metadata_path,
    )


repo_root = os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
python_bin = os.getenv("AIRFLOW_PYTHON_BIN", "python")
input_template = os.getenv(
    "AIRFLOW_RAW_INPUT_PATH_TEMPLATE", "data/generated/transactions.csv.gz"
)
input_path = _template_with_ingestion_date(input_template)
input_format = os.getenv("AIRFLOW_RAW_INPUT_FORMAT", "csv")
output_target = os.getenv("AIRFLOW_OUTPUT_TARGET", "local")
output_base_path = os.getenv("AIRFLOW_OUTPUT_BASE_PATH", "data/lakehouse")
table_format = os.getenv("AIRFLOW_TABLE_FORMAT", "parquet")

with DAG(
    dag_id="retail_batch_etl_orchestration",
    schedule=[RAW_TRANSACTIONS_DATASET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args=DEFAULT_ARGS,
    sla_miss_callback=sla_miss_callback,
    tags=["batch", "etl", "lakehouse", "dataset-aware"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    dependency_check = _build_dependency_sensor(
        task_id="dependency_check",
        input_template=input_template,
    )

    run_batch_etl = SparkSubmitOperator(
        task_id="run_batch_etl",
        conn_id=os.getenv("AIRFLOW_SPARK_CONN_ID", "spark_default"),
        application=f"{repo_root}/spark/batch/run_pipeline.py",
        name="retail_batch_etl_{{ ds_nodash }}",
        env_vars={"PYTHONPATH": repo_root},
        application_args=[
            "--input-path",
            input_path,
            "--input-format",
            input_format,
            "--output-target",
            output_target,
            "--output-base-path",
            output_base_path,
            "--ingestion-date",
            INGESTION_DATE_EXPR,
            "--table-format",
            table_format,
        ],
        conf=_openlineage_spark_conf(),
        verbose=False,
        outlets=[
            GOLD_DAILY_REVENUE_DATASET,
            GOLD_TOP_PRODUCTS_DATASET,
            GOLD_CUSTOMER_LTV_DATASET,
        ],
    )

    validate_gold_outputs = PythonOperator(
        task_id="validate_gold_outputs",
        python_callable=_ensure_gold_outputs,
        op_kwargs={"base_path": output_base_path},
    )

    run_data_quality_scan = BashOperator(
        task_id="run_data_quality_scan",
        retries=1,
        retry_delay=timedelta(minutes=5),
        cwd=repo_root,
        bash_command=(
            f"{python_bin} scripts/run_soda_scan.py "
            "--checks quality/soda/checks/gold_quality.yml "
            "--target {{ params.target_env }}"
        ),
        params={"target_env": os.getenv("AIRFLOW_TARGET_ENV", "dev")},
    )

    publish_run_metadata = PythonOperator(
        task_id="publish_run_metadata",
        python_callable=_publish_run_metadata,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> dependency_check >> run_batch_etl >> validate_gold_outputs >> run_data_quality_scan
    [dependency_check, run_batch_etl, validate_gold_outputs, run_data_quality_scan] >> publish_run_metadata >> finish
