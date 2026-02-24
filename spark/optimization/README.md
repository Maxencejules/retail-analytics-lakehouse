# Spark Optimization Jobs

## Compaction Automation

Compaction job:
- [compact_tables.py](C:/Users/USER/retail-analytics-lakehouse/spark/optimization/compact_tables.py)

Purpose:
- Reduce small-file fragmentation in Silver/Gold.
- Rebalance output file count toward a target size.
- Improve query scan efficiency and reduce compute/storage overhead.

Supported datasets:
- `silver/transactions`
- `gold/daily_revenue_by_store`
- `gold/top_10_products_by_day`
- `gold/customer_lifetime_value`

## Example Commands

Compact latest partition only (default behavior):

```bash
python spark/optimization/compact_tables.py \
  --base-path data/lakehouse \
  --table-format parquet \
  --target-file-size-mb 256
```

Compact full dataset scope:

```bash
python spark/optimization/compact_tables.py \
  --base-path s3://retail-loyalty-lakehouse-dev \
  --table-format parquet \
  --full-dataset \
  --target-file-size-mb 256 \
  --max-file-count 256
```

## Adaptive Scaling Notes

The compaction job uses the same Spark performance profile controls as batch/streaming:
- `SPARK_WORKLOAD_PROFILE` (`cost_saver`, `balanced`, `high_throughput`)
- `SPARK_MIN_EXECUTORS`, `SPARK_INITIAL_EXECUTORS`, `SPARK_MAX_EXECUTORS`
- `SPARK_SHUFFLE_PARTITIONS`

These controls are documented in Phase 3 operational docs.
