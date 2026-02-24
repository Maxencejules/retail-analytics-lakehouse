# Retail Analytics Lakehouse

Production-oriented monorepo for a retail analytics lakehouse platform.

## Architecture Overview

- `infra/`: AWS, Airflow orchestration, monitoring, and operational infrastructure.
- `ingestion/`: Synthetic transaction generator and ingestion utilities.
- `spark/`: Batch and streaming PySpark pipelines for Bronze/Silver/Gold processing.
- `warehouse/`: Postgres star schema SQL and dbt transformation project.
- `dashboard/`: Executive KPI dashboard and data access abstractions.
- `quality/`: Soda data-quality observability checks and alerting config.
- `docs/`: Architecture, setup guides, and CI/CD runbooks.
- `scripts/`: Automation helpers (integration tests, quality scan runners).
- `tests/`: Unit and pipeline transformation tests.

End-to-end flow:
`sources -> ingestion -> bronze/silver/gold lakehouse -> warehouse/dbt marts -> dashboard`

Detailed architecture reference:
- [architecture.md](C:/Users/USER/retail-analytics-lakehouse/docs/architecture.md)
- [platform-evolution.md](C:/Users/USER/retail-analytics-lakehouse/docs/platform-evolution.md)
- [cost-performance.md](C:/Users/USER/retail-analytics-lakehouse/docs/cost-performance.md)

## Core Platform Additions

- Airflow orchestration:
  - [infra/airflow](C:/Users/USER/retail-analytics-lakehouse/infra/airflow/README.md)
  - Batch DAG includes retries, backfills, dependency checks, and SLA miss notifications.
  - Promotion DAG enforces `dev -> stage -> prod` gates with dbt + Soda checks.
  - Run metadata artifacts are emitted for orchestration audit trails.
- dbt warehouse discipline:
  - [warehouse/dbt](C:/Users/USER/retail-analytics-lakehouse/warehouse/dbt/README.md)
  - Staging, marts, semantic/metric models, exposures, and governance selectors.
- Monitoring stack:
  - [infra/monitoring](C:/Users/USER/retail-analytics-lakehouse/infra/monitoring/README.md)
  - CloudWatch + Prometheus/Grafana + OpenLineage/Marquez baseline.
- Data-quality observability:
  - [quality/soda](C:/Users/USER/retail-analytics-lakehouse/quality/soda/README.md)
  - Alert routing via webhook and optional SNS.
- Cost/performance automation:
  - [spark/optimization](C:/Users/USER/retail-analytics-lakehouse/spark/optimization/README.md)
  - Automated compaction + adaptive scaling + workload policy validation gates.

## Local Development

### Prerequisites

- Python 3.11
- `make`
- Docker (for Kafka/Spark and monitoring stack)
- Git

### Setup

```bash
make init
```

### Daily Commands

```bash
make format
make lint
make test
make airflow-dag-validate
```

### Platform Operations

```bash
pip install -r requirements-dbt.txt
pip install -r requirements-quality.txt
make dbt-build DBT_TARGET=dev
make dbt-docs DBT_TARGET=dev
make dbt-governance-validate
make phase3-policy-validate
make compact-lakehouse
make soda-scan TARGET_ENV=dev
make monitoring-up
```

## AWS and CI/CD

- AWS setup: [docs/aws-setup.md](C:/Users/USER/retail-analytics-lakehouse/docs/aws-setup.md)
- Spark container: [Dockerfile.spark](C:/Users/USER/retail-analytics-lakehouse/Dockerfile.spark)
- Local data stack: [docker-compose.yml](C:/Users/USER/retail-analytics-lakehouse/docker-compose.yml)
- CI/CD guide: [docs/ci-cd.md](C:/Users/USER/retail-analytics-lakehouse/docs/ci-cd.md)
