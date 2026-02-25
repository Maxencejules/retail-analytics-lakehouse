"""Airflow DAG for automated PyTorch sales model training/retraining."""

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
from airflow.utils.context import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from common.datasets import GOLD_DAILY_REVENUE_DATASET, ML_SALES_MODEL_DATASET
from common.notifications import sla_miss_callback, task_failure_callback
from common.run_metadata import (
    build_metadata_path,
    collect_deployment_metadata,
    derive_overall_status,
    summarize_task_states,
    write_json_record,
)

LOGGER = logging.getLogger("airflow.ml_sales_retraining")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": task_failure_callback,
    "sla": timedelta(hours=3),
}


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
                "boto3 is required for validating s3:// model training paths."
            ) from exc

        s3 = boto3.client("s3")
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        return response.get("KeyCount", 0) > 0

    return Path(path_value).exists()


def _ensure_gold_daily_revenue_dataset(path_value: str) -> None:
    if path_value.startswith("s3://"):
        if not _path_exists(path_value):
            raise AirflowFailException(
                f"Missing Gold dataset for ML training: {path_value}"
            )
        return

    path = Path(path_value)
    parquet_files = list(path.rglob("*.parquet"))
    if not parquet_files:
        raise AirflowFailException(
            f"Gold dataset exists but has no parquet files: {path_value}"
        )


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
        "ml_training": {
            "gold_path": gold_daily_revenue_path,
            "model_output_dir": model_output_dir,
            "prediction_output_file": prediction_output_file,
        },
    }
    write_json_record(record, metadata_path)
    LOGGER.info("Published ML retraining metadata path=%s", metadata_path)


repo_root = os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
python_bin = os.getenv("AIRFLOW_PYTHON_BIN", "python")
output_base_path = os.getenv("AIRFLOW_OUTPUT_BASE_PATH", "data/lakehouse")

gold_daily_revenue_path = os.getenv(
    "AIRFLOW_ML_GOLD_DAILY_REVENUE_PATH",
    f"{output_base_path.rstrip('/')}/gold/daily_revenue_by_store",
)
model_output_dir = os.getenv(
    "AIRFLOW_ML_MODEL_OUTPUT_DIR",
    "artifacts/models/sales_revenue_predictor",
)
prediction_output_file = os.getenv(
    "AIRFLOW_ML_PREDICTION_OUTPUT_FILE",
    "artifacts/models/predictions/sales_predictions_{{ ds_nodash }}.jsonl",
)
train_epochs = os.getenv("AIRFLOW_ML_TRAIN_EPOCHS", "30")
train_batch_size = os.getenv("AIRFLOW_ML_TRAIN_BATCH_SIZE", "64")
train_learning_rate = os.getenv("AIRFLOW_ML_TRAIN_LEARNING_RATE", "0.001")
train_validation_ratio = os.getenv("AIRFLOW_ML_TRAIN_VALIDATION_RATIO", "0.2")

with DAG(
    dag_id="retail_ml_sales_retraining",
    schedule=[GOLD_DAILY_REVENUE_DATASET],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=sla_miss_callback,
    tags=["ml", "training", "pytorch", "dataset-aware"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    validate_gold_dataset = PythonOperator(
        task_id="validate_gold_dataset",
        python_callable=_ensure_gold_daily_revenue_dataset,
        op_kwargs={"path_value": gold_daily_revenue_path},
    )

    train_sales_model = BashOperator(
        task_id="train_sales_model",
        cwd=repo_root,
        bash_command=(
            f"{python_bin} models/train_sales_predictor.py "
            f"--gold-path {gold_daily_revenue_path} "
            f"--output-dir {model_output_dir} "
            f"--epochs {train_epochs} "
            f"--batch-size {train_batch_size} "
            f"--learning-rate {train_learning_rate} "
            f"--validation-ratio {train_validation_ratio} "
            "--run-tag {{ ds_nodash }}"
        ),
        outlets=[ML_SALES_MODEL_DATASET],
    )

    score_sales_model = BashOperator(
        task_id="score_sales_model",
        cwd=repo_root,
        bash_command=(
            f"{python_bin} models/score_sales_predictor.py "
            f"--gold-path {gold_daily_revenue_path} "
            f"--model-root {model_output_dir} "
            f"--output-file {prediction_output_file}"
        ),
    )

    publish_run_metadata = PythonOperator(
        task_id="publish_run_metadata",
        python_callable=_publish_run_metadata,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> validate_gold_dataset >> train_sales_model >> score_sales_model
    [validate_gold_dataset, train_sales_model, score_sales_model] >> publish_run_metadata >> finish

