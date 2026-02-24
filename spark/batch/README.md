# Batch ETL Pipeline (Bronze/Silver/Gold)

## What This Pipeline Does

- Reads raw transactions into Bronze partitioned by `ingestion_date` with no business transformations.
- Builds Silver with explicit casts, currency normalization, null promo handling, deduplication, and data quality enforcement.
- Builds Gold data products:
  - Daily revenue per store
  - Top 10 products per day
  - Customer lifetime value

## CLI Example (Local Output)

```bash
python spark/batch/run_pipeline.py \
  --input-path data/generated/transactions.csv.gz \
  --input-format csv \
  --output-target local \
  --output-base-path data/lakehouse \
  --ingestion-date 2026-02-24 \
  --table-format parquet
```

## CLI Example (S3 Output)

```bash
python spark/batch/run_pipeline.py \
  --input-path s3://raw-zone/transactions/2026-02-24/ \
  --input-format parquet \
  --output-target s3 \
  --output-base-path s3://retail-loyalty-lake/dev \
  --ingestion-date 2026-02-24 \
  --table-format parquet
```

## Idempotency

- All layer writes are executed in `overwrite` mode.
- Partition overwrite mode is dynamic, so reruns replace affected partitions deterministically.
- Deterministic deduplication logic ensures stable record selection for duplicate transaction IDs.

## Fail-Fast Data Quality

- Pipeline raises an exception when critical quality rules fail (default behavior).
- Disable fail-fast only for controlled backfill investigations:
  - `--no-fail-fast-quality`

## Adaptive Scaling Profile

Batch Spark sessions use shared workload profiles:
- `SPARK_WORKLOAD_PROFILE=cost_saver|balanced|high_throughput`
- `SPARK_MIN_EXECUTORS`
- `SPARK_INITIAL_EXECUTORS`
- `SPARK_MAX_EXECUTORS`
- `SPARK_SHUFFLE_PARTITIONS`

This supports Phase 3 cost/performance control without code changes per environment.
