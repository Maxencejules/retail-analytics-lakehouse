"""Environment-driven Spark performance profile controls."""

from __future__ import annotations

from dataclasses import dataclass, replace
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


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


@dataclass(frozen=True, slots=True)
class SparkPerformanceProfile:
    """Spark profile tuned for cost/performance workload controls."""

    profile_name: str
    adaptive_enabled: bool
    dynamic_allocation_enabled: bool
    min_executors: int
    initial_executors: int
    max_executors: int
    shuffle_partitions: int
    max_partition_bytes_mb: int
    auto_broadcast_join_threshold_mb: int
    speculation_enabled: bool
    scheduler_mode: str

    def validate(self) -> None:
        if self.min_executors <= 0:
            raise ValueError("min_executors must be > 0")
        if self.initial_executors < self.min_executors:
            raise ValueError("initial_executors must be >= min_executors")
        if self.max_executors < self.initial_executors:
            raise ValueError("max_executors must be >= initial_executors")
        if self.shuffle_partitions <= 0:
            raise ValueError("shuffle_partitions must be > 0")
        if self.max_partition_bytes_mb <= 0:
            raise ValueError("max_partition_bytes_mb must be > 0")
        if self.auto_broadcast_join_threshold_mb < 0:
            raise ValueError("auto_broadcast_join_threshold_mb must be >= 0")
        if self.scheduler_mode not in {"FAIR", "FIFO"}:
            raise ValueError("scheduler_mode must be FAIR or FIFO")

    @classmethod
    def defaults(cls, profile_name: str) -> "SparkPerformanceProfile":
        profile = profile_name.strip().lower()
        if profile == "cost_saver":
            return cls(
                profile_name="cost_saver",
                adaptive_enabled=True,
                dynamic_allocation_enabled=True,
                min_executors=1,
                initial_executors=2,
                max_executors=8,
                shuffle_partitions=96,
                max_partition_bytes_mb=128,
                auto_broadcast_join_threshold_mb=10,
                speculation_enabled=False,
                scheduler_mode="FAIR",
            )

        if profile == "high_throughput":
            return cls(
                profile_name="high_throughput",
                adaptive_enabled=True,
                dynamic_allocation_enabled=True,
                min_executors=4,
                initial_executors=8,
                max_executors=40,
                shuffle_partitions=400,
                max_partition_bytes_mb=256,
                auto_broadcast_join_threshold_mb=20,
                speculation_enabled=True,
                scheduler_mode="FAIR",
            )

        return cls(
            profile_name="balanced",
            adaptive_enabled=True,
            dynamic_allocation_enabled=True,
            min_executors=2,
            initial_executors=4,
            max_executors=20,
            shuffle_partitions=200,
            max_partition_bytes_mb=128,
            auto_broadcast_join_threshold_mb=10,
            speculation_enabled=True,
            scheduler_mode="FAIR",
        )

    @classmethod
    def from_env(cls) -> "SparkPerformanceProfile":
        profile = cls.defaults(os.getenv("SPARK_WORKLOAD_PROFILE", "balanced"))
        overridden = replace(
            profile,
            adaptive_enabled=_env_bool("SPARK_ADAPTIVE_ENABLED", profile.adaptive_enabled),
            dynamic_allocation_enabled=_env_bool(
                "SPARK_DYNAMIC_ALLOCATION_ENABLED",
                profile.dynamic_allocation_enabled,
            ),
            min_executors=_env_int("SPARK_MIN_EXECUTORS", profile.min_executors),
            initial_executors=_env_int("SPARK_INITIAL_EXECUTORS", profile.initial_executors),
            max_executors=_env_int("SPARK_MAX_EXECUTORS", profile.max_executors),
            shuffle_partitions=_env_int("SPARK_SHUFFLE_PARTITIONS", profile.shuffle_partitions),
            max_partition_bytes_mb=_env_int(
                "SPARK_MAX_PARTITION_BYTES_MB",
                profile.max_partition_bytes_mb,
            ),
            auto_broadcast_join_threshold_mb=_env_int(
                "SPARK_AUTO_BROADCAST_JOIN_THRESHOLD_MB",
                profile.auto_broadcast_join_threshold_mb,
            ),
            speculation_enabled=_env_bool(
                "SPARK_SPECULATION_ENABLED",
                profile.speculation_enabled,
            ),
            scheduler_mode=os.getenv("SPARK_SCHEDULER_MODE", profile.scheduler_mode)
            .strip()
            .upper(),
        )
        overridden.validate()
        return overridden

    def as_dict(self) -> dict[str, Any]:
        return {
            "profile_name": self.profile_name,
            "adaptive_enabled": self.adaptive_enabled,
            "dynamic_allocation_enabled": self.dynamic_allocation_enabled,
            "min_executors": self.min_executors,
            "initial_executors": self.initial_executors,
            "max_executors": self.max_executors,
            "shuffle_partitions": self.shuffle_partitions,
            "max_partition_bytes_mb": self.max_partition_bytes_mb,
            "auto_broadcast_join_threshold_mb": self.auto_broadcast_join_threshold_mb,
            "speculation_enabled": self.speculation_enabled,
            "scheduler_mode": self.scheduler_mode,
        }


def apply_performance_profile(builder: Any, profile: SparkPerformanceProfile) -> Any:
    """Apply profile controls to a SparkSession builder."""
    profile.validate()
    conf = {
        "spark.sql.adaptive.enabled": str(profile.adaptive_enabled).lower(),
        "spark.dynamicAllocation.enabled": str(profile.dynamic_allocation_enabled).lower(),
        "spark.dynamicAllocation.minExecutors": str(profile.min_executors),
        "spark.dynamicAllocation.initialExecutors": str(profile.initial_executors),
        "spark.dynamicAllocation.maxExecutors": str(profile.max_executors),
        "spark.dynamicAllocation.shuffleTracking.enabled": "true",
        "spark.sql.shuffle.partitions": str(profile.shuffle_partitions),
        "spark.sql.files.maxPartitionBytes": str(profile.max_partition_bytes_mb * 1024 * 1024),
        "spark.sql.autoBroadcastJoinThreshold": str(
            profile.auto_broadcast_join_threshold_mb * 1024 * 1024
        ),
        "spark.speculation": str(profile.speculation_enabled).lower(),
        "spark.scheduler.mode": profile.scheduler_mode,
    }

    for key, value in conf.items():
        builder = builder.config(key, value)
    return builder
