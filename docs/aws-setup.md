# AWS Setup Guide

## 1. S3 Lake Structure

Use one bucket per environment (recommended), with explicit layer prefixes:

```text
s3://retail-loyalty-lakehouse-<env>/
  bronze/
  silver/
  gold/
  _checkpoints/
  _quarantine/
```

Layer intent:
- `bronze/`: raw append-only outputs from ingestion.
- `silver/`: validated and standardized datasets.
- `gold/`: business-ready aggregates and marts.

## 2. IAM Least-Privilege Policy Example

Reference policy:
- [lakehouse-s3-access-policy.dev.json](C:/Users/USER/retail-analytics-lakehouse/infra/aws/iam/lakehouse-s3-access-policy.dev.json)

Policy characteristics:
- Explicit S3 actions only (`s3:ListBucket`, `s3:GetObject`, `s3:PutObject`, etc.).
- Explicit bucket and prefix resources only.
- No `Action: "*"`.
- No `Resource: "*"`.

## 3. Environment-Based Configuration

Use environment files as templates:
- [dev.env.example](C:/Users/USER/retail-analytics-lakehouse/infra/aws/env/dev.env.example)
- [prod.env.example](C:/Users/USER/retail-analytics-lakehouse/infra/aws/env/prod.env.example)

Guidelines:
- Keep only non-secret settings in env files (region, bucket, prefixes, profile).
- Never commit access keys or secret values.
- Use separate AWS profiles and buckets per environment.

Runtime helper:
- [runtime_config.py](C:/Users/USER/retail-analytics-lakehouse/infra/aws/runtime_config.py)

This module loads config from environment and creates boto3 sessions using the **AWS default credential chain**.

## 4. AWS Credentials (Default Chain)

Use one of the following (in order of precedence):
1. Environment variables injected securely by your runtime.
2. Shared credentials/config (`~/.aws/credentials`, `~/.aws/config`).
3. IAM role attached to EC2/ECS/EKS workload.

Do not pass static credentials in code or commit them to files.

## 5. Docker for Spark Jobs

Artifacts:
- Spark Docker image: [Dockerfile.spark](C:/Users/USER/retail-analytics-lakehouse/Dockerfile.spark)
- Local development stack: [docker-compose.yml](C:/Users/USER/retail-analytics-lakehouse/docker-compose.yml)
- Spark dependencies: [requirements-spark.txt](C:/Users/USER/retail-analytics-lakehouse/requirements-spark.txt)

Start local stack:

```bash
docker compose up -d --build
```

Run a command inside the Spark container:

```bash
docker compose exec spark python spark/batch/run_pipeline.py --help
```

Notes:
- Compose mounts `~/.aws` into the container at `/root/.aws` (read-only).
- On Windows, if needed, set `HOME` before running compose or adjust the bind path.

## 6. Cost Considerations

- Use S3 lifecycle policies:
  - Transition older Bronze data to lower-cost storage classes.
  - Keep Gold in standard storage for active analytics windows.
- Compact small files in Silver/Gold to reduce query scan cost.
- Set retention windows for checkpoints and quarantine data.
- Right-size Spark clusters and prefer autoscaling for batch workloads.
- Enforce partition pruning by date in all major queries.

## 7. Security Best Practices

- Enforce least privilege IAM for each workload role.
- Enable bucket encryption at rest (SSE-KMS preferred).
- Enforce TLS in transit for all clients.
- Enable S3 bucket versioning and access logging where required.
- Use centralized secret management (AWS Secrets Manager / SSM Parameter Store).
- Rotate credentials and audit IAM policies regularly.
- Mask/tokenize PII before Gold serving layers.

## 8. Secret Handling Rule

Never hardcode secrets in:
- source code
- committed env files
- Dockerfiles
- CI pipeline definitions

Always inject secrets at runtime from approved secret-management services.

