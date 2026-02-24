"""Airflow DAG for Phase 3 cost/performance automation controls."""

from __future__ import annotations

from datetime import timedelta
import logging
import os
from pathlib import Path
import subprocess
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.context import get_current_context
from airflow.utils.trigger_rule import TriggerRule

from common.notifications import sla_miss_callback, task_failure_callback
from common.run_metadata import (
    build_metadata_path,
    derive_overall_status,
    summarize_task_states,
    write_json_record,
)

LOGGER = logging.getLogger("airflow.cost_performance_optimization")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "on_failure_callback": task_failure_callback,
    "sla": timedelta(hours=4),
}


@dag(
    dag_id="retail_cost_performance_optimization",
    schedule="30 5 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=sla_miss_callback,
    tags=["phase3", "cost", "performance", "optimization"],
)
def retail_cost_performance_optimization() -> None:
    """
    Phase 3 optimization workflow:
    - validate policy artifacts (WLM/lifecycle/budgets/scaling)
    - compact Silver/Gold datasets
    - publish run metadata for governance and auditability
    """

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    @task(task_id="validate_phase3_policies")
    def validate_phase3_policies() -> None:
        repo_root = Path(
            os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
        )
        if not repo_root.exists():
            repo_root = Path.cwd()

        python_bin = os.getenv("AIRFLOW_PYTHON_BIN", "python")
        command = [
            python_bin,
            "scripts/validate_phase3_policies.py",
            "--repo-root",
            ".",
        ]
        LOGGER.info("Running Phase 3 policy validation command: %s", " ".join(command))
        subprocess.run(command, cwd=str(repo_root), check=True)

    @task(task_id="run_lakehouse_compaction")
    def run_lakehouse_compaction() -> None:
        repo_root = Path(
            os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
        )
        if not repo_root.exists():
            repo_root = Path.cwd()

        python_bin = os.getenv("AIRFLOW_PYTHON_BIN", "python")
        base_path = os.getenv("AIRFLOW_OUTPUT_BASE_PATH", "data/lakehouse")
        table_format = os.getenv("AIRFLOW_TABLE_FORMAT", "parquet")
        datasets = os.getenv(
            "AIRFLOW_COMPACTION_DATASETS",
            ",".join(
                [
                    "silver/transactions",
                    "gold/daily_revenue_by_store",
                    "gold/top_10_products_by_day",
                    "gold/customer_lifetime_value",
                ]
            ),
        )
        target_size_mb = os.getenv("AIRFLOW_COMPACTION_TARGET_FILE_SIZE_MB", "256")

        command = [
            python_bin,
            "spark/optimization/compact_tables.py",
            "--base-path",
            base_path,
            "--table-format",
            table_format,
            "--datasets",
            datasets,
            "--target-file-size-mb",
            target_size_mb,
        ]
        LOGGER.info("Running compaction command: %s", " ".join(command))
        subprocess.run(command, cwd=str(repo_root), check=True)

    @task(task_id="publish_run_metadata", trigger_rule=TriggerRule.ALL_DONE)
    def publish_run_metadata() -> None:
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
        }
        write_json_record(record, metadata_path)
        LOGGER.info(
            "Published Phase 3 run metadata status=%s output_path=%s",
            status,
            metadata_path,
        )

    validated = validate_phase3_policies()
    compacted = run_lakehouse_compaction()
    metadata = publish_run_metadata()

    start >> validated >> compacted
    [validated, compacted] >> metadata >> finish


dag: Any = retail_cost_performance_optimization()
