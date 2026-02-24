"""Helpers for publishing Airflow run metadata as JSON artifacts."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any
from urllib.parse import urlparse

from airflow.exceptions import AirflowFailException
from airflow.models.dagrun import DagRun

TERMINAL_FAILURE_STATES = {"failed", "upstream_failed"}
TERMINAL_SUCCESS_STATES = {"success"}
TERMINAL_NON_SUCCESS_STATES = {"skipped", "removed"}
PATH_SAFE_PATTERN = re.compile(r"[^A-Za-z0-9._=-]")


def _sanitize_path_component(value: str) -> str:
    return PATH_SAFE_PATTERN.sub("_", value.strip())


def build_metadata_path(template: str, context: dict[str, Any]) -> str:
    """Build an output path for metadata, allowing templated runtime fields."""
    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", "unknown_dag")
    run_id = str(context.get("run_id", "unknown_run"))
    ds = str(context.get("ds", datetime.now(timezone.utc).date().isoformat()))

    return template.format(
        dag_id=dag_id,
        ds=ds,
        run_id=run_id,
        run_id_safe=_sanitize_path_component(run_id),
    )


def summarize_task_states(
    dag_run: DagRun,
    *,
    exclude_task_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Collect a compact snapshot of task states for the current DAG run."""
    excluded = exclude_task_ids or set()
    summary: list[dict[str, Any]] = []
    for task_instance in dag_run.get_task_instances():
        if task_instance.task_id in excluded:
            continue
        summary.append(
            {
                "task_id": task_instance.task_id,
                "state": str(task_instance.state),
                "try_number": int(task_instance.try_number),
            }
        )
    return sorted(summary, key=lambda value: value["task_id"])


def derive_overall_status(task_states: list[dict[str, Any]]) -> str:
    """Derive a stable run status from task states."""
    states = {str(task["state"]) for task in task_states if task.get("state")}
    if states & TERMINAL_FAILURE_STATES:
        return "failed"
    if states and states.issubset(TERMINAL_SUCCESS_STATES):
        return "success"
    if states and states.issubset(TERMINAL_SUCCESS_STATES | TERMINAL_NON_SUCCESS_STATES):
        return "partial_success"
    return "unknown"


def write_json_record(record: dict[str, Any], output_path: str) -> None:
    """Persist a JSON record to local storage or s3:// URI."""
    if output_path.startswith("s3://"):
        parsed = urlparse(output_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise AirflowFailException(f"Invalid S3 metadata output path: {output_path}")

        try:
            import boto3
        except ImportError as exc:  # pragma: no cover
            raise AirflowFailException(
                "boto3 is required for writing metadata to s3:// paths."
            ) from exc

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(record, indent=2, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        return

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(record, indent=2, sort_keys=True), encoding="utf-8")
