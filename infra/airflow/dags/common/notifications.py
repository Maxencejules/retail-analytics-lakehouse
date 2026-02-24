"""Shared alerting callbacks for Airflow DAGs."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import logging
import os
from typing import Any
from urllib import request

LOGGER = logging.getLogger("airflow.notifications")


def _send_webhook(webhook_url: str, payload: dict[str, Any]) -> None:
    encoded = json.dumps(payload).encode("utf-8")
    req = request.Request(
        webhook_url,
        data=encoded,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with request.urlopen(req, timeout=10) as response:  # noqa: S310
        if response.status >= 300:
            raise RuntimeError(f"Alert webhook returned status {response.status}")


def _build_message(text: str) -> dict[str, Any]:
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "text": text,
    }


def task_failure_callback(context: dict[str, Any]) -> None:
    """Notify on task failure when AIRFLOW_ALERT_WEBHOOK_URL is configured."""
    webhook_url = os.getenv("AIRFLOW_ALERT_WEBHOOK_URL", "").strip()
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    run_id = context.get("run_id", "unknown")

    message = (
        f"[AIRFLOW][FAILURE] dag={dag_id} task={task_id} run_id={run_id}. "
        "Check Airflow logs for full traceback."
    )
    LOGGER.error(message)

    if not webhook_url:
        LOGGER.info("AIRFLOW_ALERT_WEBHOOK_URL not set; skipping external failure alert.")
        return

    _send_webhook(webhook_url, _build_message(message))


def sla_miss_callback(
    dag: Any,
    task_list: str,
    blocking_task_list: str,
    slas: list[Any],
    blocking_tis: list[Any],
) -> None:
    """Notify on SLA miss when AIRFLOW_ALERT_WEBHOOK_URL is configured."""
    webhook_url = os.getenv("AIRFLOW_ALERT_WEBHOOK_URL", "").strip()
    message = (
        f"[AIRFLOW][SLA_MISS] dag={getattr(dag, 'dag_id', 'unknown')} "
        f"tasks={task_list} blocking={blocking_task_list} "
        f"sla_records={len(slas)} blocking_tis={len(blocking_tis)}."
    )
    LOGGER.warning(message)

    if not webhook_url:
        LOGGER.info("AIRFLOW_ALERT_WEBHOOK_URL not set; skipping external SLA alert.")
        return

    _send_webhook(webhook_url, _build_message(message))

