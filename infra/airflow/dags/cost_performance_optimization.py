"""Airflow DAG for Phase 3 cost/performance automation controls."""

from __future__ import annotations

from datetime import timedelta
import os

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.context import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from common.datasets import (
    GOLD_CUSTOMER_LTV_DATASET,
    GOLD_DAILY_REVENUE_DATASET,
    GOLD_TOP_PRODUCTS_DATASET,
    PHASE3_COMPACTION_DATASET,
)
from common.notifications import sla_miss_callback, task_failure_callback
from common.run_metadata import (
    build_metadata_path,
    collect_deployment_metadata,
    derive_overall_status,
    summarize_task_states,
    write_json_record,
)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "on_failure_callback": task_failure_callback,
    "sla": timedelta(hours=4),
}


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

    record = {
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"],
        "status": status,
        "event_timestamp_utc": pendulum.now("UTC").isoformat(),
        "logical_date_utc": context["logical_date"].isoformat(),
        "task_states": task_states,
        "deployment": collect_deployment_metadata(),
    }
    write_json_record(record, metadata_path)


repo_root = os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
python_bin = os.getenv("AIRFLOW_PYTHON_BIN", "python")
base_path = os.getenv("AIRFLOW_OUTPUT_BASE_PATH", "data/lakehouse")
table_format = os.getenv("AIRFLOW_TABLE_FORMAT", "parquet")
datasets = os.getenv(
    "AIRFLOW_COMPACTION_DATASETS",
    "silver/transactions,gold/daily_revenue_by_store,gold/top_10_products_by_day,gold/customer_lifetime_value",
)
target_size_mb = os.getenv("AIRFLOW_COMPACTION_TARGET_FILE_SIZE_MB", "256")

with DAG(
    dag_id="retail_cost_performance_optimization",
    schedule=[
        GOLD_DAILY_REVENUE_DATASET,
        GOLD_TOP_PRODUCTS_DATASET,
        GOLD_CUSTOMER_LTV_DATASET,
    ],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=sla_miss_callback,
    tags=["phase3", "cost", "performance", "optimization", "dataset-aware"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    validate_phase3_policies = BashOperator(
        task_id="validate_phase3_policies",
        cwd=repo_root,
        bash_command=f"{python_bin} scripts/validate_phase3_policies.py --repo-root .",
    )

    run_lakehouse_compaction = SparkSubmitOperator(
        task_id="run_lakehouse_compaction",
        conn_id=os.getenv("AIRFLOW_SPARK_CONN_ID", "spark_default"),
        application=f"{repo_root}/spark/optimization/compact_tables.py",
        name="retail_phase3_compaction_{{ ds_nodash }}",
        env_vars={"PYTHONPATH": repo_root},
        application_args=[
            "--base-path",
            base_path,
            "--table-format",
            table_format,
            "--datasets",
            datasets,
            "--target-file-size-mb",
            target_size_mb,
        ],
        conf=_openlineage_spark_conf(),
        verbose=False,
        outlets=[PHASE3_COMPACTION_DATASET],
    )

    publish_run_metadata = PythonOperator(
        task_id="publish_run_metadata",
        python_callable=_publish_run_metadata,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> validate_phase3_policies >> run_lakehouse_compaction
    [validate_phase3_policies, run_lakehouse_compaction] >> publish_run_metadata >> finish
