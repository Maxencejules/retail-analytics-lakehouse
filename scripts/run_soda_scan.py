"""Run Soda checks and route failure alerts to webhook/SNS."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any
from urllib import request

LOGGER = logging.getLogger("scripts.run_soda_scan")


def _build_command(
    soda_bin: str,
    data_source: str,
    configuration: Path,
    checks: Path,
) -> list[str]:
    return [
        soda_bin,
        "scan",
        "-d",
        data_source,
        "-c",
        str(configuration),
        str(checks),
    ]


def _post_webhook(webhook_url: str, payload: dict[str, Any]) -> None:
    encoded = json.dumps(payload).encode("utf-8")
    req = request.Request(
        webhook_url,
        data=encoded,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with request.urlopen(req, timeout=10) as response:  # noqa: S310
        if response.status >= 300:
            raise RuntimeError(f"Webhook returned status={response.status}")


def _publish_sns(topic_arn: str, payload: dict[str, Any]) -> None:
    try:
        import boto3
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("boto3 is required for SNS alert routing") from exc

    client = boto3.client("sns")
    client.publish(
        TopicArn=topic_arn,
        Subject="Retail Lakehouse Soda Scan Failure",
        Message=json.dumps(payload, separators=(",", ":")),
    )


def _notify_failure(payload: dict[str, Any]) -> None:
    webhook_url = os.getenv("SODA_ALERT_WEBHOOK_URL", "").strip()
    if webhook_url:
        try:
            _post_webhook(webhook_url, payload)
        except Exception:
            LOGGER.exception("event=alert_failed channel=webhook")

    topic_arn = os.getenv("SODA_ALERT_SNS_TOPIC_ARN", "").strip()
    if topic_arn:
        try:
            _publish_sns(topic_arn, payload)
        except Exception:
            LOGGER.exception("event=alert_failed channel=sns")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Soda checks for data quality.")
    parser.add_argument(
        "--configuration",
        default="quality/soda/configuration.yml",
        help="Path to Soda datasource configuration file.",
    )
    parser.add_argument(
        "--checks",
        required=True,
        help="Path to Soda checks file.",
    )
    parser.add_argument(
        "--data-source",
        default="retail_warehouse",
        help="Soda datasource name in the configuration file.",
    )
    parser.add_argument(
        "--target",
        default="dev",
        help="Deployment target label for observability context.",
    )
    parser.add_argument(
        "--soda-bin",
        default="soda",
        help="Soda CLI executable name/path.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = parse_args(argv)

    configuration = Path(args.configuration)
    checks = Path(args.checks)
    if not configuration.exists():
        raise FileNotFoundError(f"Missing Soda configuration file: {configuration}")
    if not checks.exists():
        raise FileNotFoundError(f"Missing Soda checks file: {checks}")
    if shutil.which(args.soda_bin) is None:
        raise FileNotFoundError(f"Soda executable not found: {args.soda_bin}")

    command = _build_command(args.soda_bin, args.data_source, configuration, checks)
    env = os.environ.copy()
    env["SODA_TARGET_ENV"] = args.target
    LOGGER.info(
        "event=soda_scan_start target=%s datasource=%s checks=%s",
        args.target,
        args.data_source,
        checks,
    )

    completed = subprocess.run(  # noqa: S603
        command,
        check=False,
        text=True,
        capture_output=True,
        env=env,
    )
    if completed.stdout:
        LOGGER.info("event=soda_scan_stdout output=%s", completed.stdout.strip())
    if completed.stderr:
        LOGGER.warning("event=soda_scan_stderr output=%s", completed.stderr.strip())

    if completed.returncode == 0:
        LOGGER.info("event=soda_scan_success target=%s", args.target)
        return 0

    payload: dict[str, Any] = {
        "event": "soda_scan_failed",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "target": args.target,
        "data_source": args.data_source,
        "checks_file": str(checks),
        "return_code": completed.returncode,
        "stderr_tail": completed.stderr[-500:] if completed.stderr else "",
    }
    LOGGER.error(
        "event=soda_scan_failure target=%s return_code=%s",
        args.target,
        completed.returncode,
    )
    _notify_failure(payload)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
