# AWS Infrastructure Support

## Scope

This folder contains AWS-specific bootstrap artifacts for:

- S3 lake folder layout
- Least-privilege IAM policy examples
- Environment-based runtime configuration (no hardcoded credentials)
- Phase 3 cost/performance policy templates

## Contents

- S3 layout: [s3/README.md](C:/Users/USER/retail-analytics-lakehouse/infra/aws/s3/README.md)
- IAM policy example: [iam/lakehouse-s3-access-policy.dev.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/iam/lakehouse-s3-access-policy.dev.json)
- Env config examples:
  - [env/dev.env.example](C:/Users/USER/retail-analytics-lakehouse/infra/aws/env/dev.env.example)
  - [env/prod.env.example](C:/Users/USER/retail-analytics-lakehouse/infra/aws/env/prod.env.example)
- Runtime config helper: [runtime_config.py](C:/Users/USER/retail-analytics-lakehouse/infra/aws/runtime_config.py)
- Adaptive scaling policy: [spark/adaptive-scaling-policy.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/spark/adaptive-scaling-policy.example.json)
- Redshift WLM policy: [redshift/workload-management.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/redshift/workload-management.example.json)
- S3 lifecycle policy: [s3/lifecycle/lakehouse-lifecycle-policy.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/s3/lifecycle/lakehouse-lifecycle-policy.example.json)
- Budget policy: [cost/budget-alerts.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/cost/budget-alerts.example.json)
- Log retention policy: [cost/cloudwatch-log-retention.example.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/cost/cloudwatch-log-retention.example.json)

Phase 3 env templates include Spark workload profile settings and compaction sizing defaults.
