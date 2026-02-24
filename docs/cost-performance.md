# Cost and Performance Automation (Phase 3)

## Scope

Phase 3 operationalizes cost and performance controls across:
- Lakehouse file compaction
- Adaptive Spark scaling profiles
- Redshift workload isolation policies
- S3 lifecycle and budget guardrails
- Log retention governance

## 1. Compaction Automation

Compaction job:
- [compact_tables.py](C:/Users/USER/retail-analytics-lakehouse/spark/optimization/compact_tables.py)
- Airflow automation: [cost_performance_optimization.py](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/dags/cost_performance_optimization.py)

Run locally:

```bash
make compact-lakehouse
```

Behavior:
- Targets Silver and Gold datasets.
- Compacts latest partition by default (cost-efficient).
- Supports full-dataset compaction for maintenance windows.
- Uses target file-size planning to reduce small-file overhead.

## 2. Adaptive Spark Scaling

Shared runtime profile module:
- [performance.py](C:/Users/USER/retail-analytics-lakehouse/spark/common/performance.py)

Policy reference:
- [adaptive-scaling-policy.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/spark/adaptive-scaling-policy.example.json)

Supported profiles:
- `cost_saver`
- `balanced`
- `high_throughput`

Key environment overrides:
- `SPARK_WORKLOAD_PROFILE`
- `SPARK_MIN_EXECUTORS`
- `SPARK_INITIAL_EXECUTORS`
- `SPARK_MAX_EXECUTORS`
- `SPARK_SHUFFLE_PARTITIONS`

## 3. Workload Management Policies

Redshift WLM policy:
- [workload-management.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/redshift/workload-management.example.json)

Design goals:
- Isolate critical BI from ETL and ad-hoc workloads.
- Enforce queue-level concurrency controls.
- Use query monitoring rules for runaway query protection.

## 4. Cost Guardrails

S3 lifecycle policy:
- [lakehouse-lifecycle-policy.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/s3/lifecycle/lakehouse-lifecycle-policy.example.json)

Budget policy:
- [budget-alerts.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/cost/budget-alerts.example.json)

Log retention policy:
- [cloudwatch-log-retention.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/cost/cloudwatch-log-retention.example.json)

## 5. Policy Validation

Validator:
- [validate_phase3_policies.py](C:/Users/USER/retail-analytics-lakehouse/scripts/validate_phase3_policies.py)

Run:

```bash
make phase3-policy-validate
```

CI gate:
- [.github/workflows/ci.yml](C:/Users/USER/retail-analytics-lakehouse/.github/workflows/ci.yml)

This prevents invalid policy artifacts from reaching promotion workflows.
