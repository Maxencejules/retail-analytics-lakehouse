# Batch ETL Pipeline (Bronze/Silver/Gold)

## What This Pipeline Does

- Reads raw transactions into Bronze partitioned by `ingestion_date` with no business transformations.
- Builds Silver with explicit casts, currency normalization, null promo handling, deduplication, and data quality enforcement.
- Builds Gold data products:
  - Daily revenue per store and currency
  - Top 10 products per day and currency
  - Customer observed lifetime value per currency

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

- One ingestion date is one replaceable batch. Replaying that date replaces its Bronze partition; different dates remain available.
- All retained Bronze batches must have compatible raw field types. CSV is used by the proof. Unsupported raw schema evolution (for example CSV string quantity followed by JSON numeric quantity) is rejected before writing Bronze; normalize the new source first. Automatic schema migration is outside this implementation.
- Silver and all three Gold datasets are rebuilt from all retained Bronze partitions. Full static replacement clears obsolete event dates/currencies after correction or backfill while retaining unrelated history. This small reference implementation trades full-history work for explicit correctness; it is not an incremental merge engine.
- A transaction ID denotes one logical sale. Greatest event timestamp wins, then greatest ingestion date. Exact normalized duplicate versions collapse. Different business values at the same latest version raise `DataQualityError`; price is not a version selector.
- LTV is cumulative observed sales, excludes returns, and retains one current snapshot labeled by the greatest retained ingestion date. Backfill does not move the label backward. This is receipt metadata, not an event-time cutoff.
- Gold includes `currency`; no revenue sum or product ranking crosses currencies and no FX conversion occurs. Numeric fields remain doubles, rounded to two places for Gold; this is analytics rather than a financial ledger.
- Use one writer. Failed validation retains Bronze for diagnosis while prior Silver/Gold remain unchanged. A later write failure can leave partially published tables; rerun the corrected batch to reconstruct them. Parquet has no atomic multi-table commit. Delta needs a separately configured runtime and uses `replaceWhere` for Bronze; cloud/Delta operation is not covered by the local Parquet proof.

## Reproducible correctness proof

```bash
.venv/bin/python scripts/verify_batch_history.py --output .tmp/batch-history-proof.json
```

Use Linux/Python3.11/Java17 and `requirements-dev.txt`. The script writes only its own temporary lakehouse, executes real Spark, and checks every Silver and Gold row against an independent Python/Decimal oracle. It tests overlapping dates, exact retries, cross-date correction, replay, earlier-batch replacement, invalid input preserving prior outputs, and repaired replay. CI uploads proof JSON. Tiny synthetic runtime is measured without scale/SLA claims. Windows Spark may require compatible Hadoop binaries and working Python workers; Windows helper tests do not prove Parquet execution.

Consumers must honor the currency key: dashboard uses `DASHBOARD_CURRENCY`; ML train/score use `--currency` and a matching checkpoint. Legacy Gold without currency must be rebuilt before ML loading. Gold product rankings are per-day top10, so a period sum is a truncated ranking rather than all-products revenue; this aggregate also lacks store-level product attribution.

## Fail-Fast Data Quality

- Pipeline raises an exception when critical quality rules fail (default behavior).
- Null/unsupported required categories, fractional/out-of-range quantities, invalid dates, and NaN/infinite prices or derived revenue are rejected. CSV structural errors use FAILFAST parsing. Duplicate-version conflicts fail even in invalid-record filtering mode.
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
