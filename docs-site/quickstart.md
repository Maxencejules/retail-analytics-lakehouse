---
layout: default
title: Quickstart
---

# Quickstart Guide

## 1. Clone and Bootstrap

```bash
git clone <repo-url>
cd retail-analytics-lakehouse
make init
```

## 2. Run Core Validation

```bash
make format
make lint
make test
```

<figure class="screenshot">
  <img src="{{ '/assets/screenshots/quickstart-terminal.svg' | relative_url }}" alt="Terminal quickstart output">
  <figcaption>Example terminal flow for local setup and validation checks.</figcaption>
</figure>

## 3. Run Spark ETL + Benchmarks

```bash
python spark/batch/run_pipeline.py \
  --input-path data/generated/transactions.csv.gz \
  --input-format csv \
  --output-target local \
  --output-base-path data/lakehouse \
  --ingestion-date 2026-02-25 \
  --table-format parquet

make benchmark-etl
```

## 4. Optional UI Preview

```bash
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

<figure class="screenshot">
  <img src="{{ '/assets/screenshots/quickstart-dashboard.svg' | relative_url }}" alt="Dashboard quickstart screenshot">
  <figcaption>Reference analytics UI view after loading Gold/warehouse datasets.</figcaption>
</figure>

## 5. Trino Federation Stack (Optional)

```bash
docker compose up -d warehouse hive-metastore spark trino
docker compose exec trino trino
```

For federated SQL examples, see:
[`docs/federated-querying.md`]({{ site.repository_url }}/blob/{{ site.docs_default_branch }}/docs/federated-querying.md)
