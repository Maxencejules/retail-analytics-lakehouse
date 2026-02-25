"""Airflow DAG: environment promotion workflow with mandatory dbt and quality gates."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from urllib.parse import urlparse

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sensors.filesystem import FileSensor
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
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": task_failure_callback,
    "sla": timedelta(hours=1),
}


def _render_param_template(value: str) -> str:
    rendered = value
    rendered = rendered.replace("{source_env}", "{{ params.source_env }}")
    rendered = rendered.replace("{target_env}", "{{ params.target_env }}")
    rendered = rendered.replace("{release_version}", "{{ params.release_version }}")
    return rendered


def _build_artifact_sensor(task_id: str):
    artifact_template = os.getenv(
        "AIRFLOW_PROMOTION_ARTIFACT_TEMPLATE",
        "data/promotions/{source_env}/{release_version}/ready.flag",
    )
    rendered = _render_param_template(artifact_template)

    if rendered.startswith("s3://"):
        parsed = urlparse(rendered)
        return S3KeySensor(
            task_id=task_id,
            bucket_name=parsed.netloc,
            bucket_key=parsed.path.lstrip("/"),
            wildcard_match=True,
            poke_interval=120,
            timeout=60 * 30,
            mode="reschedule",
        )

    return FileSensor(
        task_id=task_id,
        filepath=rendered,
        fs_conn_id="fs_default",
        poke_interval=120,
        timeout=60 * 30,
        mode="reschedule",
    )


def _validate_promotion_request() -> None:
    context = get_current_context()
    params = context["params"]
    source_env = params["source_env"]
    target_env = params["target_env"]

    if source_env == target_env:
        raise AirflowFailException("source_env and target_env must be different.")

    order = {"dev": 1, "stage": 2, "prod": 3}
    if order[target_env] - order[source_env] != 1:
        raise AirflowFailException(
            "Promotion must follow environment order dev->stage->prod."
        )


def _publish_promotion_record() -> None:
    context = get_current_context()
    params = context["params"]

    source = str(params["source_env"])
    target = str(params["target_env"])
    release = str(params["release_version"])

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
        "deployment": collect_deployment_metadata(),
    }
    write_json_record(record, output_path)


def _publish_run_metadata() -> None:
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
        "deployment": collect_deployment_metadata(),
    }
    write_json_record(record, metadata_path)


repo_root = os.getenv("AIRFLOW_REPO_ROOT", "/opt/retail-analytics-lakehouse")
dbt_bin = os.getenv("AIRFLOW_DBT_BIN", "dbt")
python_bin = os.getenv("AIRFLOW_PYTHON_BIN", "python")
dbt_selector = os.getenv("AIRFLOW_DBT_SELECTOR", "phase2_governed_models").strip()

with DAG(
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
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")

    validate_promotion_request = PythonOperator(
        task_id="validate_promotion_request",
        python_callable=_validate_promotion_request,
    )

    check_dependency_artifact = _build_artifact_sensor(
        task_id="check_dependency_artifact"
    )

    run_dbt_governance_contracts = BashOperator(
        task_id="run_dbt_governance_contracts",
        cwd=repo_root,
        bash_command=f"{python_bin} scripts/validate_dbt_governance.py --repo-root .",
    )

    run_dbt_build_source = BashOperator(
        task_id="run_dbt_build_source",
        cwd=repo_root,
        bash_command=(
            f"{dbt_bin} build --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles "
            "--target {{ params.source_env }} --selector "
            f"{dbt_selector}"
        ),
    )

    run_dbt_freshness_source = BashOperator(
        task_id="run_dbt_freshness_source",
        cwd=repo_root,
        bash_command=(
            f"{dbt_bin} source freshness --project-dir warehouse/dbt "
            "--profiles-dir warehouse/dbt/profiles --target {{ params.source_env }}"
        ),
    )

    run_dbt_snapshot_source = BashOperator(
        task_id="run_dbt_snapshot_source",
        cwd=repo_root,
        bash_command=(
            f"{dbt_bin} snapshot --project-dir warehouse/dbt "
            "--profiles-dir warehouse/dbt/profiles --target {{ params.source_env }}"
        ),
    )

    run_dbt_build_target = BashOperator(
        task_id="run_dbt_build_target",
        cwd=repo_root,
        bash_command=(
            f"{dbt_bin} build --project-dir warehouse/dbt --profiles-dir warehouse/dbt/profiles "
            "--target {{ params.target_env }} --selector "
            f"{dbt_selector}"
        ),
    )

    run_dbt_freshness_target = BashOperator(
        task_id="run_dbt_freshness_target",
        cwd=repo_root,
        bash_command=(
            f"{dbt_bin} source freshness --project-dir warehouse/dbt "
            "--profiles-dir warehouse/dbt/profiles --target {{ params.target_env }}"
        ),
    )

    run_dbt_snapshot_target = BashOperator(
        task_id="run_dbt_snapshot_target",
        cwd=repo_root,
        bash_command=(
            f"{dbt_bin} snapshot --project-dir warehouse/dbt "
            "--profiles-dir warehouse/dbt/profiles --target {{ params.target_env }}"
        ),
    )

    run_dbt_docs_generate_target = BashOperator(
        task_id="run_dbt_docs_generate_target",
        cwd=repo_root,
        bash_command=(
            f"{dbt_bin} docs generate --project-dir warehouse/dbt "
            "--profiles-dir warehouse/dbt/profiles --target {{ params.target_env }}"
        ),
    )

    run_quality_scan_source = BashOperator(
        task_id="run_quality_scan_source",
        cwd=repo_root,
        bash_command=(
            f"{python_bin} scripts/run_soda_scan.py --checks quality/soda/checks/gold_quality.yml "
            "--target {{ params.source_env }}"
        ),
    )

    run_quality_scan_target = BashOperator(
        task_id="run_quality_scan_target",
        cwd=repo_root,
        bash_command=(
            f"{python_bin} scripts/run_soda_scan.py --checks quality/soda/checks/gold_quality.yml "
            "--target {{ params.target_env }}"
        ),
    )

    publish_promotion_record = PythonOperator(
        task_id="publish_promotion_record",
        python_callable=_publish_promotion_record,
    )

    publish_run_metadata = PythonOperator(
        task_id="publish_run_metadata",
        python_callable=_publish_run_metadata,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        start
        >> validate_promotion_request
        >> check_dependency_artifact
        >> run_dbt_governance_contracts
    )
    (
        run_dbt_governance_contracts
        >> run_dbt_build_source
        >> run_dbt_freshness_source
        >> run_dbt_snapshot_source
    )
    (
        run_dbt_snapshot_source
        >> run_dbt_build_target
        >> run_dbt_freshness_target
        >> run_dbt_snapshot_target
    )
    (
        run_dbt_snapshot_target
        >> run_dbt_docs_generate_target
        >> run_quality_scan_source
        >> run_quality_scan_target
    )
    run_quality_scan_target >> publish_promotion_record
    (
        [
            validate_promotion_request,
            check_dependency_artifact,
            run_dbt_governance_contracts,
            run_dbt_build_source,
            run_dbt_freshness_source,
            run_dbt_snapshot_source,
            run_dbt_build_target,
            run_dbt_freshness_target,
            run_dbt_snapshot_target,
            run_dbt_docs_generate_target,
            run_quality_scan_source,
            run_quality_scan_target,
            publish_promotion_record,
        ]
        >> publish_run_metadata
        >> finish
    )
