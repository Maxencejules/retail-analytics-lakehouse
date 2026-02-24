"""Airflow DAG: environment promotion workflow with dependency and quality gates."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import os
from pathlib import Path
import subprocess
from typing import Any
from urllib.parse import urlparse

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
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

LOGGER = logging.getLogger("airflow.environment_promotion")

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": task_failure_callback,
    "sla": timedelta(hours=1),
}


def _s3_prefix_exists(path_value: str) -> bool:
    parsed = urlparse(path_value)
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if not bucket or not prefix:
        return False

    try:
        import boto3
    except ImportError as exc:  # pragma: no cover
        raise AirflowFailException(
            "boto3 is required to validate s3:// promotion artifacts."
        ) from exc

    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return response.get("KeyCount", 0) > 0


@dag(
    dag_id="retail_environment_promotion_workflow",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    sla_miss_callback=sla_miss_callback,
    params={
        "source_env": Param("dev", type="string", enum=["dev", "stage", "prod"]),
        "target_env": Param("stage", type="string", enum=["dev", "stage", "prod"]),
        "release_version": Param("manual", type="string"),
    },
    tags=["promotion", "dbt", "quality"],
)
def retail_environment_promotion_workflow() -> None:
    """
    Promotion workflow:
    - validate promotion request
    - check source dependency artifact
    - run dbt build/tests in target environment
    - run data quality observability checks
    - publish promotion record
    """

    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    @task(task_id="validate_promotion_request")
    def validate_promotion_request() -> dict[str, str]:
        context = get_current_context()
        params = context["params"]
        source_env = params["source_env"]
        target_env = params["target_env"]
        release_version = params["release_version"]

        if source_env == target_env:
            raise AirflowFailException("source_env and target_env must be different.")

        order = {"dev": 1, "stage": 2, "prod": 3}
        if order[target_env] - order[source_env] != 1:
            raise AirflowFailException(
                "Promotion must follow environment order dev->stage->prod."
            )

        return {
            "source_env": source_env,
            "target_env": target_env,
            "release_version": release_version,
        }

    @task(task_id="check_dependency_artifact")
    def check_dependency_artifact(payload: dict[str, str]) -> dict[str, str]:
        artifact_template = os.getenv(
            "AIRFLOW_PROMOTION_ARTIFACT_TEMPLATE",
            "data/promotions/{source_env}/{release_version}/ready.flag",
        )
        artifact_path = artifact_template.format(**payload)

        if artifact_path.startswith("s3://"):
            exists = _s3_prefix_exists(artifact_path)
        else:
            exists = Path(artifact_path).exists()

        if not exists:
            raise AirflowFailException(
                f"Promotion dependency artifact not found: {artifact_path}"
            )

        payload["artifact_path"] = artifact_path
        return payload

    @task(task_id="run_dbt_build")
    def run_dbt_build(payload: dict[str, str]) -> dict[str, str]:
        repo_root = Path(
            os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
        )
        if not repo_root.exists():
            repo_root = Path.cwd()

        dbt_bin = os.getenv("AIRFLOW_DBT_BIN", "dbt")
        command = [
            dbt_bin,
            "build",
            "--project-dir",
            "warehouse/dbt",
            "--profiles-dir",
            "warehouse/dbt/profiles",
            "--target",
            payload["target_env"],
        ]
        dbt_selector = os.getenv("AIRFLOW_DBT_SELECTOR", "phase2_governed_models").strip()
        if dbt_selector:
            command.extend(["--selector", dbt_selector])
        LOGGER.info("Running dbt build command: %s", " ".join(command))
        subprocess.run(command, cwd=str(repo_root), check=True)
        return payload

    @task(task_id="run_dbt_source_freshness")
    def run_dbt_source_freshness(payload: dict[str, str]) -> dict[str, str]:
        repo_root = Path(
            os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
        )
        if not repo_root.exists():
            repo_root = Path.cwd()

        dbt_bin = os.getenv("AIRFLOW_DBT_BIN", "dbt")
        command = [
            dbt_bin,
            "source",
            "freshness",
            "--project-dir",
            "warehouse/dbt",
            "--profiles-dir",
            "warehouse/dbt/profiles",
            "--target",
            payload["target_env"],
        ]
        LOGGER.info("Running dbt source freshness command: %s", " ".join(command))
        subprocess.run(command, cwd=str(repo_root), check=True)
        return payload

    @task(task_id="run_dbt_docs_generate")
    def run_dbt_docs_generate(payload: dict[str, str]) -> dict[str, str]:
        repo_root = Path(
            os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
        )
        if not repo_root.exists():
            repo_root = Path.cwd()

        dbt_bin = os.getenv("AIRFLOW_DBT_BIN", "dbt")
        command = [
            dbt_bin,
            "docs",
            "generate",
            "--project-dir",
            "warehouse/dbt",
            "--profiles-dir",
            "warehouse/dbt/profiles",
            "--target",
            payload["target_env"],
        ]
        LOGGER.info("Running dbt docs generate command: %s", " ".join(command))
        subprocess.run(command, cwd=str(repo_root), check=True)
        return payload

    @task(task_id="run_quality_observability_scan")
    def run_quality_observability_scan(payload: dict[str, str]) -> dict[str, str]:
        repo_root = Path(
            os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
        )
        if not repo_root.exists():
            repo_root = Path.cwd()

        python_bin = os.getenv("AIRFLOW_PYTHON_BIN", "python")
        command = [
            python_bin,
            "scripts/run_soda_scan.py",
            "--checks",
            "quality/soda/checks/gold_quality.yml",
            "--target",
            payload["target_env"],
        ]
        LOGGER.info("Running Soda scan command: %s", " ".join(command))
        subprocess.run(command, cwd=str(repo_root), check=True)
        return payload

    @task(task_id="publish_promotion_record")
    def publish_promotion_record(payload: dict[str, str]) -> None:
        target = payload["target_env"]
        source = payload["source_env"]
        release = payload["release_version"]

        record_template = os.getenv(
            "AIRFLOW_PROMOTION_RECORD_TEMPLATE",
            "data/promotions/history/{target_env}/{release_version}.json",
        )
        output_path = record_template.format(
            source_env=source,
            target_env=target,
            release_version=release,
        )
        record = {
            "source_env": source,
            "target_env": target,
            "release_version": release,
            "promoted_at_utc": datetime.now(timezone.utc).isoformat(),
            "artifact_path": payload["artifact_path"],
        }
        write_json_record(record, output_path)

    @task(task_id="publish_run_metadata", trigger_rule=TriggerRule.ALL_DONE)
    def publish_run_metadata() -> None:
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
        logical_date = context.get("logical_date")

        record = {
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
            "status": status,
            "event_timestamp_utc": pendulum.now("UTC").isoformat(),
            "logical_date_utc": logical_date.isoformat() if logical_date else None,
            "source_env": params.get("source_env"),
            "target_env": params.get("target_env"),
            "release_version": params.get("release_version"),
            "task_states": task_states,
        }
        write_json_record(record, metadata_path)
        LOGGER.info(
            "Published promotion run metadata status=%s output_path=%s",
            status,
            metadata_path,
        )

    validated = validate_promotion_request()
    dependency_checked = check_dependency_artifact(validated)
    built = run_dbt_build(dependency_checked)
    freshness = run_dbt_source_freshness(built)
    docs_generated = run_dbt_docs_generate(freshness)
    scanned = run_quality_observability_scan(docs_generated)
    published = publish_promotion_record(scanned)
    metadata = publish_run_metadata()

    start >> validated >> dependency_checked >> built >> freshness >> docs_generated >> scanned >> published
    [validated, dependency_checked, built, freshness, docs_generated, scanned, published] >> metadata >> finish


dag: Any = retail_environment_promotion_workflow()
