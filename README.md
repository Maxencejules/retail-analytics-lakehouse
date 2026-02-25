# Retail Analytics Lakehouse

Production-grade monorepo for a modular retail analytics lakehouse spanning ingestion, processing, warehousing, orchestration, quality, observability, and BI consumption.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Local End-to-End Workflow](#local-end-to-end-workflow)
- [Operational Runbook](#operational-runbook)
- [Configuration](#configuration)
- [Performance Benchmarking](#performance-benchmarking)
- [ML Integration (PyTorch)](#ml-integration-pytorch)
- [Chaos Engineering and SLA](#chaos-engineering-and-sla)
- [Federated Queries with Trino](#federated-queries-with-trino)
- [GitHub Pages Docs Site](#github-pages-docs-site)
- [Quality Standards and CI](#quality-standards-and-ci)
- [Security](#security)
- [Documentation](#documentation)

## Overview

This repository is designed for teams that need:

- A layered data platform (`Bronze -> Silver -> Gold -> Warehouse -> BI`) with clear ownership boundaries.
- Reproducible local development that maps directly to CI/CD controls.
- Strong governance gates for orchestration, quality, lineage, and cost/performance operations.

Core capabilities:

- Synthetic batch and streaming transaction generation.
- PySpark batch ETL and structured streaming pipelines.
- Warehouse modeling via Postgres SQL + dbt semantic/governance layer.
- Airflow DAGs for daily execution, backfills, and environment promotion.
- Soda quality scans, Prometheus/Grafana monitoring, and OpenLineage metadata support.
- Executive dashboard consuming either warehouse or Gold datasets.
- Trino federation across warehouse tables, lakehouse parquet outputs, and external object-store snapshots.

## Architecture

Primary flow:

`sources -> ingestion -> bronze/silver/gold lakehouse -> warehouse/dbt marts -> dashboard`

Operational control planes:

- Orchestration: Airflow DAGs with gate-driven promotion.
- Data quality: Spark fail-fast checks + Soda scans.
- Observability: CloudWatch, Prometheus, Alertmanager, Grafana, OpenLineage.
- Cost/performance: compaction, adaptive Spark profiles, lifecycle and policy validation.

Detailed architecture references:

- [docs/architecture.md](docs/architecture.md)
- [docs/platform-evolution.md](docs/platform-evolution.md)
- [docs/cost-performance.md](docs/cost-performance.md)

## Repository Structure

| Path | Responsibility |
|---|---|
| `ingestion/` | Synthetic transaction generation and streaming utilities. |
| `spark/batch/` | Batch ETL from raw input to Bronze/Silver/Gold outputs. |
| `spark/streaming/` | Kafka-based Structured Streaming transformations to Gold outputs. |
| `spark/optimization/` | Compaction and optimization workflows for Silver/Gold datasets. |
| `warehouse/postgres/` | Star schema DDL, index strategy, upsert logic, analytical query examples. |
| `warehouse/dbt/` | dbt models, tests, snapshots, semantic models, exposures, selectors. |
| `infra/airflow/` | DAGs for orchestration, promotion workflows, run metadata, and optimization. |
| `infra/monitoring/` | Monitoring and lineage stack assets (Prometheus, Grafana, Alertmanager, OpenLineage). |
| `infra/trino/` | Trino catalog configuration for federated SQL across Postgres and Spark outputs. |
| `infra/aws/` | AWS environment templates, IAM/policy examples, runtime configuration helpers. |
| `quality/soda/` | Soda quality definitions and alert routing templates. |
| `dashboard/` | Streamlit dashboard and data-access layer for KPI consumption. |
| `models/` | PyTorch training/scoring scripts for Gold-layer ML use cases. |
| `scripts/` | CI and validation scripts (integration, governance, policy, quality). |
| `tests/` | Unit tests for ingestion and Spark transformation logic. |
| `perf/` | Benchmark scripts documentation and generated performance reports. |
| `chaos/` | Chaos experiment scripts, Gremlin payload templates, and drill artifacts. |
| `docs-site/` | Jekyll-based GitHub Pages site for interactive documentation. |
| `docs/` | Architecture, cloud setup, CI/CD, and operational guidance. |

## Getting Started

### Prerequisites

- Python 3.11
- Java 17
- `make`
- Docker + Docker Compose
- Git

### Initial Setup

```bash
make init
```

What this does:

- creates `.venv`
- installs development dependencies
- installs pre-commit hooks

### Baseline Validation

```bash
make format
make lint
make test
make airflow-dag-validate
```

## Local End-to-End Workflow

### 1. Generate Synthetic Source Data

```bash
python ingestion/generator/generate.py \
  --mode batch \
  --rows 1000 \
  --seed 42 \
  --output-dir data/generated
```

### 2. Run Batch ETL

```bash
python spark/batch/run_pipeline.py \
  --input-path data/generated/transactions.csv.gz \
  --input-format csv \
  --output-target local \
  --output-base-path data/lakehouse \
  --ingestion-date <YYYY-MM-DD> \
  --table-format parquet
```

Expected Gold datasets (`data/lakehouse/gold/`):

- `daily_revenue_by_store`
- `top_10_products_by_day`
- `customer_lifetime_value`

### 3. Execute Integration Validation

```bash
make test-integration
```

This verifies:

- successful generator-to-Gold pipeline execution
- fail-fast behavior on invalid critical records

### 4. Run the Dashboard (Optional)

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

Dashboard data-source modes:

- `DASHBOARD_DATA_SOURCE=warehouse` (default)
- `DASHBOARD_DATA_SOURCE=gold`

Details: [dashboard/README.md](dashboard/README.md)

## Operational Runbook

### Airflow

Key workflows:

- dataset-aware daily batch orchestration
- dedicated backfill workflow
- gate-controlled `dev -> stage -> prod` promotion
- Phase 3 cost/performance orchestration

Validate DAG code:

```bash
make airflow-dag-validate
```

Reference: [infra/airflow/README.md](infra/airflow/README.md)

### dbt

```bash
pip install -r requirements-dbt.txt
make dbt-build DBT_TARGET=dev
make dbt-docs DBT_TARGET=dev
make dbt-source-freshness DBT_TARGET=dev
make dbt-slim-ci DBT_TARGET=dev
make dbt-phase2-gate DBT_TARGET=dev
make dbt-governance-validate
```

Reference: [warehouse/dbt/README.md](warehouse/dbt/README.md)

### Data Quality (Soda)

```bash
pip install -r requirements-quality.txt
make soda-scan TARGET_ENV=dev
```

Reference: [quality/soda/README.md](quality/soda/README.md)

### Monitoring and Lineage

```bash
make monitoring-up
make monitoring-down
```

Reference: [infra/monitoring/README.md](infra/monitoring/README.md)

### ML Training and Retraining (PyTorch)

```bash
pip install -r requirements-ml.txt
make ml-train
make ml-score
```

Airflow retraining DAG:
- `retail_ml_sales_retraining`

Reference:
- [docs/ml-integration.md](docs/ml-integration.md)
- [models/README.md](models/README.md)

### SLA Targets

- ETL availability SLA: **99.9% monthly uptime** for scheduled batch ETL jobs.
- ETL recovery SLO: recover from orchestrator/compute failures within **30 minutes**.
- Freshness SLO: `fact_sales` lag <= **30 minutes**.

### Federated Queries with Trino

```bash
docker compose up -d warehouse hive-metastore spark trino
docker compose exec trino trino
```

Federation guide and SQL examples:
- [docs/federated-querying.md](docs/federated-querying.md)

### Cost and Performance Controls

```bash
make phase3-policy-validate
make compact-lakehouse
```

Reference: [spark/optimization/README.md](spark/optimization/README.md)

### Chaos Engineering Drills

```bash
make chaos-airflow-partition CHAOS_DURATION_SECONDS=60 CHAOS_TARGET_CONTAINER=airflow-webserver
make chaos-spark-crash CHAOS_DURATION_SECONDS=60
```

Chaos runbook and Gremlin integration:
- [docs/chaos-engineering.md](docs/chaos-engineering.md)
- [chaos/README.md](chaos/README.md)

## Configuration

This platform is intentionally environment-driven. Do not hardcode deployment-specific values.

### Core Platform Variables

- `APP_ENV` (`dev`, `stage`, `prod`)
- `AWS_REGION`
- `AWS_PROFILE`
- `LAKEHOUSE_BUCKET`
- `LAKEHOUSE_PREFIX`
- `SPARK_WORKLOAD_PROFILE` (`cost_saver`, `balanced`, `high_throughput`)

### Airflow and Alerts

- `AIRFLOW_REPO_ROOT`
- `AIRFLOW_PYTHON_BIN`
- `AIRFLOW_ALERT_WEBHOOK_URL`
- `AIRFLOW_ALERT_SNS_TOPIC_ARN` (optional)
- `AIRFLOW_RUN_METADATA_PATH_TEMPLATE`

### Soda Alerts

- `SODA_ALERT_WEBHOOK_URL`
- `SODA_ALERT_SNS_TOPIC_ARN` (optional)

### Dashboard

- `DASHBOARD_DATA_SOURCE`
- `WAREHOUSE_DSN`
- `WAREHOUSE_SCHEMA`
- `GOLD_BASE_PATH`

### Trino Federation

- `WAREHOUSE_POSTGRES_USER`
- `WAREHOUSE_POSTGRES_PASSWORD`
- `WAREHOUSE_POSTGRES_DB`
- `AWS_REGION`

### ML Training

- `AIRFLOW_ML_GOLD_DAILY_REVENUE_PATH`
- `AIRFLOW_ML_MODEL_OUTPUT_DIR`
- `AIRFLOW_ML_PREDICTION_OUTPUT_FILE`
- `AIRFLOW_ML_TRAIN_EPOCHS`
- `AIRFLOW_ML_TRAIN_BATCH_SIZE`
- `AIRFLOW_ML_TRAIN_LEARNING_RATE`
- `AIRFLOW_ML_TRAIN_VALIDATION_RATIO`
Configuration templates and setup references:

- [docs/aws-setup.md](docs/aws-setup.md)
- [infra/aws/env/dev.env.example](infra/aws/env/dev.env.example)
- [infra/aws/env/prod.env.example](infra/aws/env/prod.env.example)
- [infra/airflow/config/airflow.env.example](infra/airflow/config/airflow.env.example)

## Performance Benchmarking

Run ETL benchmarks locally:

```bash
make benchmark-etl
```

Benchmark outputs:
- JSON: `perf/results/latest/etl-benchmark.json`
- Markdown report: `perf/results/latest/etl-benchmark.md`

Benchmark details and advanced options:
- [perf/README.md](perf/README.md)

## ML Integration (PyTorch)

Install ML dependencies:

```bash
pip install -r requirements-ml.txt
```

Train and score on Gold-layer sales data:

```bash
make ml-train
make ml-score
```

Detailed guide:
- [docs/ml-integration.md](docs/ml-integration.md)
- [models/README.md](models/README.md)

## Chaos Engineering and SLA

Chaos experiment commands:

```bash
make chaos-run CHAOS_EXPERIMENT=airflow_network_partition CHAOS_DRY_RUN=true
make chaos-run CHAOS_EXPERIMENT=spark_node_crash CHAOS_DURATION_SECONDS=45
```

Gremlin-mode example:

```bash
make chaos-run \
  CHAOS_EXPERIMENT=airflow_network_partition \
  CHAOS_MODE=gremlin-http \
  CHAOS_GREMLIN_ENDPOINT=$GREMLIN_API_ENDPOINT \
  CHAOS_PAYLOAD_FILE=chaos/payloads/airflow_network_partition.gremlin.example.json
```

Reference:
- [docs/chaos-engineering.md](docs/chaos-engineering.md)
- [infra/monitoring/SLOS.md](infra/monitoring/SLOS.md)

## GitHub Pages Docs Site

Docs site source:
- [docs-site/README.md](docs-site/README.md)

Deployment workflow:
- [.github/workflows/pages.yml](.github/workflows/pages.yml)

Hosted content includes:
- architecture diagrams backed by Draw.io source files,
- script/API references for custom operational tooling,
- quickstart guide with visual walkthrough screenshots,
- dynamic live-demo link support for AWS Free Tier sample deployments.

After pushing this workflow, enable GitHub Pages in repository settings and choose **GitHub Actions** as the source.

## Quality Standards and CI

Local development standards:

- Runtime: Python 3.11
- Formatting: `black`
- Linting: `ruff`
- Required pre-PR checks: `make format`, `make lint`, `make test`

CI workflow: [.github/workflows/ci.yml](.github/workflows/ci.yml)

CI quality gates:

1. Pre-commit checks
2. Lint checks
3. dbt slim selection validation
4. dbt governance contract validation
5. Phase 3 policy artifact validation
6. Unit tests
7. Integration tests
8. ETL benchmark artifact generation (runtime/memory/throughput on sample data)

Detailed CI/CD guide: [docs/ci-cd.md](docs/ci-cd.md)

## Security

Mandatory controls:

- Never commit secrets (API keys, tokens, passwords, private certificates, `.env` secrets).
- Use environment variables and secret-management tooling for sensitive values.
- Keep IAM and data-access policies least privilege and environment scoped.

Additional recommended practices:

- Encrypt data at rest (SSE-KMS preferred in cloud environments).
- Enforce TLS for all in-transit platform communication.
- Apply masking/tokenization for PII in downstream serving layers.

## Documentation

- Platform architecture: [docs/architecture.md](docs/architecture.md)
- Platform evolution roadmap: [docs/platform-evolution.md](docs/platform-evolution.md)
- Cost/performance automation: [docs/cost-performance.md](docs/cost-performance.md)
- Performance benchmarking: [perf/README.md](perf/README.md)
- PyTorch ML integration: [docs/ml-integration.md](docs/ml-integration.md)
- Chaos engineering runbook: [docs/chaos-engineering.md](docs/chaos-engineering.md)
- Interactive GitHub Pages docs: [docs-site/README.md](docs-site/README.md)
- AWS setup: [docs/aws-setup.md](docs/aws-setup.md)
- Federated querying with Trino: [docs/federated-querying.md](docs/federated-querying.md)
- CI/CD quality gates: [docs/ci-cd.md](docs/ci-cd.md)
- Airflow orchestration: [infra/airflow/README.md](infra/airflow/README.md)
- dbt warehouse: [warehouse/dbt/README.md](warehouse/dbt/README.md)
- Monitoring stack: [infra/monitoring/README.md](infra/monitoring/README.md)
- Soda quality checks: [quality/soda/README.md](quality/soda/README.md)
- Spark optimization: [spark/optimization/README.md](spark/optimization/README.md)
- Batch ETL: [spark/batch/README.md](spark/batch/README.md)
- Streaming ETL: [spark/streaming/README.md](spark/streaming/README.md)
- Dashboard: [dashboard/README.md](dashboard/README.md)
