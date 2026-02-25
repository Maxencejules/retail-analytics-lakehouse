"""Airflow DAG: controlled batch backfills using the daily orchestration DAG."""

from __future__ import annotations

from datetime import date, timedelta
import os
from typing import Any

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.context import get_current_context
from airflow.utils.trigger_rule import TriggerRule

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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,
    "sla": timedelta(hours=6),
}


def _publish_backfill_metadata() -> None:
    context = get_current_context()
    dag_run = context["dag_run"]
    params = context["params"]

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
        "backfill_start": params.get("backfill_start"),
        "backfill_end": params.get("backfill_end"),
        "task_states": task_states,
        "deployment": collect_deployment_metadata(),
    }
    write_json_record(record, metadata_path)


with DAG(
    dag_id="retail_batch_etl_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=12),
    default_args=DEFAULT_ARGS,
    sla_miss_callback=sla_miss_callback,
    params={
        "backfill_start": "2026-01-01",
        "backfill_end": "2026-01-07",
    },
    tags=["batch", "etl", "backfill"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    @task(task_id="build_backfill_confs")
    def build_backfill_confs() -> list[dict[str, Any]]:
        context = get_current_context()
        params = context["params"]

        try:
            start_date = date.fromisoformat(str(params["backfill_start"]))
            end_date = date.fromisoformat(str(params["backfill_end"]))
        except ValueError as exc:
            raise AirflowFailException(
                "backfill_start and backfill_end must be YYYY-MM-DD"
            ) from exc

        if start_date > end_date:
            raise AirflowFailException("backfill_start must be <= backfill_end")

        max_days = int(os.getenv("AIRFLOW_BACKFILL_MAX_DAYS", "90"))
        total_days = (end_date - start_date).days + 1
        if total_days > max_days:
            raise AirflowFailException(
                f"Backfill range too large: {total_days} days exceeds {max_days}"
            )

        confs: list[dict[str, Any]] = []
        current = start_date
        while current <= end_date:
            confs.append(
                {
                    "ingestion_date": current.isoformat(),
                    "triggered_by": "retail_batch_etl_backfill",
                }
            )
            current += timedelta(days=1)

        return confs

    backfill_confs = build_backfill_confs()

    trigger_daily_runs = TriggerDagRunOperator.partial(
        task_id="trigger_daily_runs",
        trigger_dag_id="retail_batch_etl_orchestration",
        wait_for_completion=True,
        poke_interval=60,
        reset_dag_run=True,
        allowed_states=["success"],
        failed_states=["failed"],
    ).expand(conf=backfill_confs)

    publish_run_metadata = PythonOperator(
        task_id="publish_run_metadata",
        python_callable=_publish_backfill_metadata,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> backfill_confs >> trigger_daily_runs
    [backfill_confs, trigger_daily_runs] >> publish_run_metadata >> finish
