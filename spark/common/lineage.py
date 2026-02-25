"""OpenLineage runtime helpers for Spark jobs."""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean value")


@dataclass(frozen=True, slots=True)
class OpenLineageConfig:
    enabled: bool
    transport_url: str
    namespace: str
    parent_job_namespace: str
    parent_job_name: str | None
    parent_run_id: str | None

    @classmethod
    def from_env(cls, *, default_parent_job_name: str) -> "OpenLineageConfig":
        return cls(
            enabled=_env_bool("OPENLINEAGE_ENABLED", False),
            transport_url=os.getenv(
                "OPENLINEAGE_URL", "http://marquez:5000/api/v1/lineage"
            ).strip(),
            namespace=os.getenv(
                "OPENLINEAGE_NAMESPACE", "retail-analytics-lakehouse"
            ).strip(),
            parent_job_namespace=os.getenv(
                "OPENLINEAGE_PARENT_JOB_NAMESPACE", "airflow"
            ).strip(),
            parent_job_name=os.getenv(
                "OPENLINEAGE_PARENT_JOB_NAME", default_parent_job_name
            ).strip()
            or None,
            parent_run_id=os.getenv("OPENLINEAGE_PARENT_RUN_ID", "").strip() or None,
        )

    def spark_conf(self) -> dict[str, str]:
        if not self.enabled:
            return {}

        conf = {
            "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
            "spark.openlineage.transport.type": "http",
            "spark.openlineage.transport.url": self.transport_url,
            "spark.openlineage.namespace": self.namespace,
            "spark.openlineage.parentJobNamespace": self.parent_job_namespace,
        }
        if self.parent_job_name:
            conf["spark.openlineage.parentJobName"] = self.parent_job_name
        if self.parent_run_id:
            conf["spark.openlineage.parentRunId"] = self.parent_run_id
        return conf


def apply_openlineage(builder: Any, config: OpenLineageConfig) -> Any:
    for key, value in config.spark_conf().items():
        builder = builder.config(key, value)
    return builder
