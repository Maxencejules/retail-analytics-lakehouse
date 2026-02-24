"""Airflow DAG: batch ETL orchestration with retries, backfill, and quality gates."""

from __future__ import annotations

from datetime import timedelta
import logging
import os
from pathlib import Path
import subprocess
from typing import Any
from urllib.parse import urlparse

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.utils.context import get_current_context

from common.notifications import sla_miss_callback, task_failure_callback

LOGGER = logging.getLogger("airflow.batch_etl_orchestration")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": task_failure_callback,
    "sla": timedelta(hours=2),
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


@dag(
    dag_id="retail_batch_etl_orchestration",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=4),
    default_args=DEFAULT_ARGS,
    sla_miss_callback=sla_miss_callback,
    tags=["batch", "etl", "lakehouse"],
)
def retail_batch_etl_orchestration() -> None:
    """
    Orchestrate batch ETL with:
    - dependency checks
    - retries
    - backfill support (catchup=True)
    - SLA miss notifications
    - output validation and quality scan integration
    """

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    @task(task_id="dependency_check")
    def dependency_check() -> str:
        context = get_current_context()
        ds = context["ds"]

        input_template = os.getenv(
            "AIRFLOW_RAW_INPUT_PATH_TEMPLATE",
            "data/generated/transactions.csv.gz",
        )
        input_path = input_template.format(ds=ds)
        if not _path_exists(input_path):
            raise AirflowFailException(
                f"Upstream dependency missing for ingestion date {ds}: {input_path}"
            )
        return input_path

    @task(task_id="run_batch_etl")
    def run_batch_etl(input_path: str) -> None:
        context = get_current_context()
        ds = context["ds"]

        repo_root = Path(
            os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
        )
        if not repo_root.exists():
            repo_root = Path.cwd()

        python_bin = os.getenv("AIRFLOW_PYTHON_BIN", "python")
        input_format = os.getenv("AIRFLOW_RAW_INPUT_FORMAT", "csv")
        output_target = os.getenv("AIRFLOW_OUTPUT_TARGET", "local")
        output_base_path = os.getenv("AIRFLOW_OUTPUT_BASE_PATH", "data/lakehouse")
        table_format = os.getenv("AIRFLOW_TABLE_FORMAT", "parquet")

        command = [
            python_bin,
            "spark/batch/run_pipeline.py",
            "--input-path",
            input_path,
            "--input-format",
            input_format,
            "--output-target",
            output_target,
            "--output-base-path",
            output_base_path,
            "--ingestion-date",
            ds,
            "--table-format",
            table_format,
        ]
        LOGGER.info("Running batch ETL command: %s", " ".join(command))
        subprocess.run(command, cwd=str(repo_root), check=True)

    @task(task_id="validate_gold_outputs")
    def validate_gold_outputs() -> None:
        output_base_path = os.getenv("AIRFLOW_OUTPUT_BASE_PATH", "data/lakehouse")
        _ensure_gold_outputs(output_base_path)

    @task(task_id="run_data_quality_scan", retries=1, retry_delay=timedelta(minutes=5))
    def run_data_quality_scan() -> None:
        repo_root = Path(
            os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
        )
        if not repo_root.exists():
            repo_root = Path.cwd()

        python_bin = os.getenv("AIRFLOW_PYTHON_BIN", "python")
        scan_command = [
            python_bin,
            "scripts/run_soda_scan.py",
            "--checks",
            "quality/soda/checks/gold_quality.yml",
        ]
        LOGGER.info("Running quality scan command: %s", " ".join(scan_command))
        subprocess.run(scan_command, cwd=str(repo_root), check=True)

    checked_input = dependency_check()
    batch_task = run_batch_etl(checked_input)
    validated = validate_gold_outputs()
    quality = run_data_quality_scan()

    start >> checked_input >> batch_task >> validated >> quality >> finish


dag: Any = retail_batch_etl_orchestration()

