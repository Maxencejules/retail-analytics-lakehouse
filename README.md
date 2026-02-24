# Retail Analytics Lakehouse

A lightweight monorepo scaffold for a retail data engineering lakehouse.

## Architecture Overview

- `infra/`: Infrastructure as code, environment provisioning, and platform setup.
- `ingestion/`: Source connectors and raw data ingestion pipelines.
- `spark/`: Spark-based transformation and enrichment jobs.
- `warehouse/`: Curated warehouse models, SQL transformations, and marts.
- `dashboard/`: BI layer assets and dashboard definitions.
- `docs/`: Architecture notes, runbooks, and operational docs.
- `scripts/`: Developer and CI helper scripts.
- `tests/`: Unit, integration, and data quality tests.

Data flow:
`Sources -> ingestion -> bronze/silver (spark) -> warehouse (gold) -> dashboard`

## Local Development

### Prerequisites

- Python 3.11
- `make`
- Git

### Setup

```bash
make init
```

### Common Commands

```bash
make format
make lint
make test
```

### Pre-commit

```bash
pre-commit run --all-files
```

## AWS Support

- AWS setup guide: [docs/aws-setup.md](C:/Users/USER/retail-analytics-lakehouse/docs/aws-setup.md)
- Spark Docker image: [Dockerfile.spark](C:/Users/USER/retail-analytics-lakehouse/Dockerfile.spark)
- Local Docker stack: [docker-compose.yml](C:/Users/USER/retail-analytics-lakehouse/docker-compose.yml)

## CI/CD

- CI/CD and local quality checks: [docs/ci-cd.md](C:/Users/USER/retail-analytics-lakehouse/docs/ci-cd.md)
