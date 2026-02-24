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


def _send_sns(topic_arn: str, payload: dict[str, Any]) -> None:
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("boto3 is required for SNS alerting.") from exc

    client = boto3.client("sns")
    client.publish(
        TopicArn=topic_arn,
        Subject=f"Airflow Alert: {payload.get('event_type', 'unknown')}",
        Message=json.dumps(payload, separators=(",", ":")),
    )


def _build_message(
    *,
    text: str,
    event_type: str,
    severity: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "text": text,
        "event_type": event_type,
        "severity": severity,
    }
    if metadata:
        payload["metadata"] = metadata
    return payload


def _dispatch_alert(payload: dict[str, Any]) -> None:
    webhook_url = os.getenv("AIRFLOW_ALERT_WEBHOOK_URL", "").strip()
    sns_topic_arn = os.getenv("AIRFLOW_ALERT_SNS_TOPIC_ARN", "").strip()

    if not webhook_url and not sns_topic_arn:
        LOGGER.info(
            "No external alert channel configured; set AIRFLOW_ALERT_WEBHOOK_URL "
            "or AIRFLOW_ALERT_SNS_TOPIC_ARN."
        )
        return

    if webhook_url:
        try:
            _send_webhook(webhook_url, payload)
        except Exception:
            LOGGER.exception("Failed to deliver Airflow alert to webhook.")

    if sns_topic_arn:
        try:
            _send_sns(sns_topic_arn, payload)
        except Exception:
            LOGGER.exception("Failed to deliver Airflow alert to SNS.")


def task_failure_callback(context: dict[str, Any]) -> None:
    """Notify on task failure through configured external channels."""
    task_instance = context.get("task_instance")
    dag_id = getattr(task_instance, "dag_id", "unknown")
    task_id = getattr(task_instance, "task_id", "unknown")
    run_id = context.get("run_id", "unknown")

    message = (
        f"[AIRFLOW][FAILURE] dag={dag_id} task={task_id} run_id={run_id}. "
        "Check Airflow logs for full traceback."
    )
    LOGGER.error(message)

    payload = _build_message(
        text=message,
        event_type="task_failure",
        severity="critical",
        metadata={"dag_id": dag_id, "task_id": task_id, "run_id": run_id},
    )
    _dispatch_alert(payload)


def sla_miss_callback(
    dag: Any,
    task_list: str,
    blocking_task_list: str,
    slas: list[Any],
    blocking_tis: list[Any],
) -> None:
    """Notify on SLA miss through configured external channels."""
    message = (
        f"[AIRFLOW][SLA_MISS] dag={getattr(dag, 'dag_id', 'unknown')} "
        f"tasks={task_list} blocking={blocking_task_list} "
        f"sla_records={len(slas)} blocking_tis={len(blocking_tis)}."
    )
    LOGGER.warning(message)

    payload = _build_message(
        text=message,
        event_type="sla_miss",
        severity="warning",
        metadata={
            "dag_id": getattr(dag, "dag_id", "unknown"),
            "task_list": task_list,
            "blocking_task_list": blocking_task_list,
            "sla_record_count": len(slas),
            "blocking_ti_count": len(blocking_tis),
        },
    )
    _dispatch_alert(payload)
