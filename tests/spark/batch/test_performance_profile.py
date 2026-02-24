"""Tests for Spark performance profile controls."""

from __future__ import annotations

from spark.common.performance import SparkPerformanceProfile, apply_performance_profile


class DummyBuilder:
    def __init__(self) -> None:
        self.conf: dict[str, str] = {}

    def config(self, key: str, value: str) -> "DummyBuilder":
        self.conf[key] = value
        return self


def test_profile_defaults_balanced() -> None:
    profile = SparkPerformanceProfile.defaults("balanced")
    assert profile.profile_name == "balanced"
    assert profile.dynamic_allocation_enabled is True
    assert profile.min_executors == 2
    assert profile.initial_executors == 4
    assert profile.max_executors == 20


def test_profile_from_env_override(monkeypatch) -> None:
    monkeypatch.setenv("SPARK_WORKLOAD_PROFILE", "cost_saver")
    monkeypatch.setenv("SPARK_MAX_EXECUTORS", "6")
    monkeypatch.setenv("SPARK_SHUFFLE_PARTITIONS", "80")

    profile = SparkPerformanceProfile.from_env()
    assert profile.profile_name == "cost_saver"
    assert profile.max_executors == 6
    assert profile.shuffle_partitions == 80


def test_apply_performance_profile_sets_expected_conf() -> None:
    profile = SparkPerformanceProfile.defaults("high_throughput")
    builder = DummyBuilder()
    apply_performance_profile(builder, profile)

    assert builder.conf["spark.sql.adaptive.enabled"] == "true"
    assert builder.conf["spark.dynamicAllocation.maxExecutors"] == str(
        profile.max_executors
    )
    assert builder.conf["spark.sql.shuffle.partitions"] == str(
        profile.shuffle_partitions
    )
