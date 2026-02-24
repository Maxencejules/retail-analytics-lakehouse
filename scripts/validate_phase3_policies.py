"""Validate Phase 3 cost/performance policy artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any


def _load_json(path: Path) -> dict[str, Any]:
    content = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(content, dict):
        raise ValueError(f"Expected JSON object in {path}")
    return content


def _validate_spark_policy(policy: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    spark_conf = policy.get("spark_conf")
    if not isinstance(spark_conf, dict):
        return ["spark/adaptive-scaling policy must contain spark_conf object."]

    required_bool_fields = (
        "spark.sql.adaptive.enabled",
        "spark.dynamicAllocation.enabled",
        "spark.dynamicAllocation.shuffleTracking.enabled",
        "spark.speculation",
    )
    for field in required_bool_fields:
        if field not in spark_conf:
            errors.append(f"spark policy missing field: {field}")

    min_exec = spark_conf.get("spark.dynamicAllocation.minExecutors")
    init_exec = spark_conf.get("spark.dynamicAllocation.initialExecutors")
    max_exec = spark_conf.get("spark.dynamicAllocation.maxExecutors")
    if not all(isinstance(value, int) for value in (min_exec, init_exec, max_exec)):
        errors.append("spark dynamic allocation executor values must be integers.")
    elif not (0 < min_exec <= init_exec <= max_exec):
        errors.append("spark dynamic allocation must satisfy min <= initial <= max.")

    shuffle_partitions = spark_conf.get("spark.sql.shuffle.partitions")
    if not isinstance(shuffle_partitions, int) or shuffle_partitions <= 0:
        errors.append("spark.sql.shuffle.partitions must be a positive integer.")

    workload_classes = policy.get("workload_classes")
    if not isinstance(workload_classes, list) or not workload_classes:
        errors.append("spark policy requires at least one workload class.")
    else:
        for workload in workload_classes:
            if not isinstance(workload, dict):
                errors.append("spark workload class entries must be objects.")
                continue
            if not str(workload.get("name", "")).strip():
                errors.append("spark workload class missing name.")
            if not str(workload.get("profile_override", "")).strip():
                errors.append("spark workload class missing profile_override.")
            sla = workload.get("sla_minutes")
            if not isinstance(sla, int) or sla <= 0:
                errors.append("spark workload class sla_minutes must be a positive integer.")

    return errors


def _validate_redshift_wlm(policy: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    queues = policy.get("wlm_json_configuration")
    if not isinstance(queues, list) or not queues:
        return ["redshift WLM policy must contain wlm_json_configuration list."]

    memory_total = 0
    for queue in queues:
        if not isinstance(queue, dict):
            errors.append("redshift queue entries must be objects.")
            continue
        if not str(queue.get("name", "")).strip():
            errors.append("redshift queue missing name.")

        concurrency = queue.get("query_concurrency")
        if not isinstance(concurrency, int) or concurrency <= 0:
            errors.append("redshift queue query_concurrency must be a positive integer.")

        memory = queue.get("memory_percent_to_use")
        if not isinstance(memory, int) or memory <= 0:
            errors.append("redshift queue memory_percent_to_use must be a positive integer.")
        else:
            memory_total += memory

    if memory_total > 100:
        errors.append("sum of redshift queue memory_percent_to_use must be <= 100.")

    return errors


def _validate_lifecycle_policy(policy: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    rules = policy.get("Rules")
    if not isinstance(rules, list) or not rules:
        return ["S3 lifecycle policy must contain Rules list."]

    required_prefixes = {"bronze/", "silver/", "gold/", "_checkpoints/", "_quarantine/"}
    found_prefixes: set[str] = set()

    for rule in rules:
        if not isinstance(rule, dict):
            errors.append("lifecycle rule entries must be objects.")
            continue
        if rule.get("Status") != "Enabled":
            errors.append(f"lifecycle rule {rule.get('ID', 'unknown')} must be Enabled.")

        filter_object = rule.get("Filter", {})
        prefix = filter_object.get("Prefix") if isinstance(filter_object, dict) else None
        if isinstance(prefix, str):
            found_prefixes.add(prefix)

        transitions = rule.get("Transitions", [])
        if transitions:
            if not isinstance(transitions, list):
                errors.append("lifecycle Transitions must be a list when present.")
            else:
                transition_days = []
                for transition in transitions:
                    if not isinstance(transition, dict):
                        errors.append("lifecycle transition entries must be objects.")
                        continue
                    days = transition.get("Days")
                    if not isinstance(days, int) or days <= 0:
                        errors.append("lifecycle transition Days must be a positive integer.")
                        continue
                    transition_days.append(days)
                if transition_days != sorted(transition_days):
                    errors.append(
                        f"lifecycle rule {rule.get('ID', 'unknown')} transition days must be sorted."
                    )

        expiration = rule.get("Expiration")
        if isinstance(expiration, dict):
            days = expiration.get("Days")
            if not isinstance(days, int) or days <= 0:
                errors.append("lifecycle expiration days must be a positive integer.")

    missing = required_prefixes - found_prefixes
    if missing:
        errors.append(f"lifecycle policy missing prefixes: {', '.join(sorted(missing))}.")

    return errors


def _validate_budget_policy(policy: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    limit_amount = policy.get("limit_amount_usd")
    if not isinstance(limit_amount, (int, float)) or limit_amount <= 0:
        errors.append("budget limit_amount_usd must be > 0.")

    notifications = policy.get("notifications")
    if not isinstance(notifications, list) or not notifications:
        return errors + ["budget policy requires at least one notification."]

    thresholds: list[int] = []
    has_forecasted = False
    for notification in notifications:
        if not isinstance(notification, dict):
            errors.append("budget notification entries must be objects.")
            continue

        threshold = notification.get("threshold_percent")
        if not isinstance(threshold, int) or threshold <= 0 or threshold > 100:
            errors.append("budget notification threshold_percent must be between 1 and 100.")
        else:
            thresholds.append(threshold)

        if notification.get("notification_type") == "FORECASTED":
            has_forecasted = True

        subscribers = notification.get("subscriber_sns_arns")
        if not isinstance(subscribers, list) or not subscribers:
            errors.append("budget notification must include subscriber_sns_arns.")

    if thresholds != sorted(thresholds):
        errors.append("budget notification thresholds must be sorted ascending.")
    if not has_forecasted:
        errors.append("budget policy should include at least one FORECASTED notification.")
    return errors


def _validate_log_retention(policy: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    groups = policy.get("log_groups")
    if not isinstance(groups, list) or not groups:
        return ["log retention policy requires log_groups list."]

    audit_retention: int | None = None
    for group in groups:
        if not isinstance(group, dict):
            errors.append("log retention entries must be objects.")
            continue
        name = str(group.get("name", "")).strip()
        days = group.get("retention_days")
        if not name:
            errors.append("log retention entry missing name.")
        if not isinstance(days, int) or days < 7 or days > 3653:
            errors.append("log retention_days must be between 7 and 3653.")
        if name.endswith("security-audit"):
            audit_retention = days if isinstance(days, int) else None

    if audit_retention is None or audit_retention < 365:
        errors.append("security-audit log retention must be at least 365 days.")
    return errors


def run_validation(repo_root: Path) -> list[str]:
    spark_policy = _load_json(
        repo_root / "infra" / "aws" / "spark" / "adaptive-scaling-policy.example.json"
    )
    redshift_policy = _load_json(
        repo_root / "infra" / "aws" / "redshift" / "workload-management.example.json"
    )
    lifecycle_policy = _load_json(
        repo_root
        / "infra"
        / "aws"
        / "s3"
        / "lifecycle"
        / "lakehouse-lifecycle-policy.example.json"
    )
    budget_policy = _load_json(
        repo_root / "infra" / "aws" / "cost" / "budget-alerts.example.json"
    )
    log_retention_policy = _load_json(
        repo_root / "infra" / "aws" / "cost" / "cloudwatch-log-retention.example.json"
    )

    errors: list[str] = []
    errors.extend(_validate_spark_policy(spark_policy))
    errors.extend(_validate_redshift_wlm(redshift_policy))
    errors.extend(_validate_lifecycle_policy(lifecycle_policy))
    errors.extend(_validate_budget_policy(budget_policy))
    errors.extend(_validate_log_retention(log_retention_policy))
    return errors


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate Phase 3 policy artifacts.")
    parser.add_argument("--repo-root", default=".", help="Repository root path.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    errors = run_validation(Path(args.repo_root).resolve())
    if errors:
        print("phase3 policy validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print("phase3 policy validation passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
