"""Run chaos experiments locally or submit them to Gremlin-compatible APIs."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any
from urllib import request

LOGGER = logging.getLogger("chaos.run_experiment")

EXPERIMENT_AIRFLOW_NETWORK_PARTITION = "airflow_network_partition"
EXPERIMENT_SPARK_NODE_CRASH = "spark_node_crash"


@dataclass(frozen=True, slots=True)
class ExperimentConfig:
    experiment: str
    mode: str
    duration_seconds: int
    target_container: str | None
    docker_network: str
    compose_project: str
    gremlin_endpoint: str | None
    payload_file: str | None
    output_file: str | None
    dry_run: bool
    headers: tuple[str, ...]

    def validate(self) -> None:
        if self.duration_seconds <= 0:
            raise ValueError("--duration-seconds must be > 0")

        allowed_experiments = {
            EXPERIMENT_AIRFLOW_NETWORK_PARTITION,
            EXPERIMENT_SPARK_NODE_CRASH,
        }
        if self.experiment not in allowed_experiments:
            allowed = ", ".join(sorted(allowed_experiments))
            raise ValueError(f"--experiment must be one of: {allowed}")

        if self.mode not in {"local-docker", "gremlin-http"}:
            raise ValueError("--mode must be local-docker or gremlin-http")

        if self.mode == "gremlin-http" and not self.gremlin_endpoint:
            raise ValueError("--gremlin-endpoint is required for gremlin-http mode")


def _run_command(
    command: list[str],
    *,
    cwd: Path,
    dry_run: bool,
) -> subprocess.CompletedProcess[str]:
    LOGGER.info("running_command %s", " ".join(command))
    if dry_run:
        return subprocess.CompletedProcess(command, returncode=0, stdout="", stderr="")
    completed = subprocess.run(
        command,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.stdout:
        LOGGER.info("command_stdout=%s", completed.stdout.strip())
    if completed.stderr:
        LOGGER.warning("command_stderr=%s", completed.stderr.strip())
    if completed.returncode != 0:
        raise RuntimeError(
            f"Command failed with exit code {completed.returncode}: {' '.join(command)}"
        )
    return completed


def _resolve_spark_container(repo_root: Path, dry_run: bool) -> str:
    completed = _run_command(
        ["docker", "compose", "ps", "-q", "spark"],
        cwd=repo_root,
        dry_run=dry_run,
    )
    container_id = completed.stdout.strip()
    if dry_run:
        return container_id or "spark"
    if not container_id:
        raise RuntimeError(
            "Unable to resolve Spark container ID. "
            "Start the stack first with `docker compose up -d spark`."
        )
    return container_id


def _run_local_airflow_partition(
    config: ExperimentConfig, repo_root: Path
) -> dict[str, Any]:
    target_container = config.target_container or os.getenv(
        "CHAOS_AIRFLOW_CONTAINER",
        "airflow-webserver",
    )
    report: dict[str, Any] = {
        "experiment": config.experiment,
        "mode": config.mode,
        "target_container": target_container,
        "docker_network": config.docker_network,
        "duration_seconds": config.duration_seconds,
        "status": "started",
    }

    _run_command(
        ["docker", "network", "disconnect", config.docker_network, target_container],
        cwd=repo_root,
        dry_run=config.dry_run,
    )
    report["status"] = "partition_active"
    time.sleep(config.duration_seconds)
    _run_command(
        ["docker", "network", "connect", config.docker_network, target_container],
        cwd=repo_root,
        dry_run=config.dry_run,
    )
    report["status"] = "recovered"
    return report


def _run_local_spark_crash(config: ExperimentConfig, repo_root: Path) -> dict[str, Any]:
    target = config.target_container or _resolve_spark_container(
        repo_root, config.dry_run
    )
    report: dict[str, Any] = {
        "experiment": config.experiment,
        "mode": config.mode,
        "target_container": target,
        "duration_seconds": config.duration_seconds,
        "status": "started",
    }

    _run_command(["docker", "kill", target], cwd=repo_root, dry_run=config.dry_run)
    report["status"] = "container_killed"
    time.sleep(config.duration_seconds)
    _run_command(
        ["docker", "compose", "up", "-d", "spark"],
        cwd=repo_root,
        dry_run=config.dry_run,
    )
    report["status"] = "recovered"
    return report


def _replace_placeholders(value: Any, replacements: dict[str, str]) -> Any:
    if isinstance(value, str):
        resolved = value
        for key, replacement in replacements.items():
            resolved = resolved.replace(f"{{{{{key}}}}}", replacement)
        return resolved
    if isinstance(value, dict):
        return {k: _replace_placeholders(v, replacements) for k, v in value.items()}
    if isinstance(value, list):
        return [_replace_placeholders(item, replacements) for item in value]
    return value


def _parse_headers(header_items: tuple[str, ...]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for item in header_items:
        if "=" not in item:
            raise ValueError(f"Invalid header format '{item}'. Use KEY=VALUE.")
        key, value = item.split("=", maxsplit=1)
        headers[key.strip()] = value.strip()
    return headers


def _build_gremlin_payload(config: ExperimentConfig) -> dict[str, Any]:
    replacements = {
        "experiment": config.experiment,
        "duration_seconds": str(config.duration_seconds),
        "target_container": config.target_container or "",
        "docker_network": config.docker_network,
        "compose_project": config.compose_project,
    }

    if config.payload_file:
        payload_path = Path(config.payload_file)
        template = json.loads(payload_path.read_text(encoding="utf-8"))
        if not isinstance(template, dict):
            raise ValueError("Gremlin payload template must be a JSON object.")
        return _replace_placeholders(template, replacements)

    return {
        "experiment": config.experiment,
        "duration_seconds": config.duration_seconds,
        "target_container": config.target_container,
        "docker_network": config.docker_network,
        "compose_project": config.compose_project,
        "notes": (
            "Default payload generated by chaos/run_experiment.py. "
            "Prefer --payload-file for account-specific Gremlin API schemas."
        ),
    }


def _run_gremlin_http(config: ExperimentConfig) -> dict[str, Any]:
    endpoint = config.gremlin_endpoint or ""
    payload = _build_gremlin_payload(config)

    headers = {"Content-Type": "application/json"}
    headers.update(_parse_headers(config.headers))
    token = os.getenv("GREMLIN_API_TOKEN", "").strip()
    if token and "Authorization" not in headers:
        headers["Authorization"] = f"Bearer {token}"

    req = request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers=headers,
    )

    if config.dry_run:
        return {
            "experiment": config.experiment,
            "mode": config.mode,
            "endpoint": endpoint,
            "status": "dry_run",
            "payload": payload,
        }

    with request.urlopen(req, timeout=20) as response:  # noqa: S310
        body = response.read().decode("utf-8")
        parsed: dict[str, Any]
        try:
            parsed = json.loads(body) if body else {}
            if not isinstance(parsed, dict):
                parsed = {"raw_body": body}
        except json.JSONDecodeError:
            parsed = {"raw_body": body}

        return {
            "experiment": config.experiment,
            "mode": config.mode,
            "endpoint": endpoint,
            "status_code": response.status,
            "response": parsed,
        }


def _default_output_path(experiment: str) -> Path:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return Path("chaos") / "results" / f"{timestamp}-{experiment}.json"


def _write_report(report: dict[str, Any], output_file: str | None) -> Path:
    output_path = (
        Path(output_file)
        if output_file
        else _default_output_path(str(report.get("experiment", "unknown")))
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return output_path


def run_experiment(config: ExperimentConfig) -> Path:
    config.validate()
    repo_root = Path(__file__).resolve().parents[1]

    if config.mode == "local-docker":
        if config.experiment == EXPERIMENT_AIRFLOW_NETWORK_PARTITION:
            report = _run_local_airflow_partition(config, repo_root)
        elif config.experiment == EXPERIMENT_SPARK_NODE_CRASH:
            report = _run_local_spark_crash(config, repo_root)
        else:
            raise ValueError(
                f"Unsupported local-docker experiment: {config.experiment}"
            )
    elif config.mode == "gremlin-http":
        report = _run_gremlin_http(config)
    else:
        raise ValueError(f"Unsupported mode: {config.mode}")

    report["executed_at_utc"] = datetime.now(timezone.utc).isoformat()
    report_path = _write_report(report, config.output_file)
    LOGGER.info("chaos_report=%s", report_path)
    return report_path


def parse_args(argv: list[str] | None = None) -> ExperimentConfig:
    parser = argparse.ArgumentParser(
        description="Run chaos experiments for airflow/spark resilience drills."
    )
    parser.add_argument(
        "--experiment",
        required=True,
        choices=[EXPERIMENT_AIRFLOW_NETWORK_PARTITION, EXPERIMENT_SPARK_NODE_CRASH],
    )
    parser.add_argument(
        "--mode",
        default="local-docker",
        choices=["local-docker", "gremlin-http"],
    )
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--target-container", default=None)
    parser.add_argument(
        "--docker-network",
        default=os.getenv("CHAOS_DOCKER_NETWORK", "retail-analytics-lakehouse_default"),
    )
    parser.add_argument(
        "--compose-project",
        default=os.getenv("CHAOS_DOCKER_PROJECT", "retail-analytics-lakehouse"),
    )
    parser.add_argument("--gremlin-endpoint", default=os.getenv("GREMLIN_API_ENDPOINT"))
    parser.add_argument("--payload-file", default=None)
    parser.add_argument("--output-file", default=None)
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        help="Custom HTTP headers in KEY=VALUE format (gremlin-http mode).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log actions without executing attack calls.",
    )
    args = parser.parse_args(argv)
    return ExperimentConfig(
        experiment=args.experiment,
        mode=args.mode,
        duration_seconds=args.duration_seconds,
        target_container=args.target_container,
        docker_network=args.docker_network,
        compose_project=args.compose_project,
        gremlin_endpoint=args.gremlin_endpoint,
        payload_file=args.payload_file,
        output_file=args.output_file,
        dry_run=args.dry_run,
        headers=tuple(args.header),
    )


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    config = parse_args(argv)
    run_experiment(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
