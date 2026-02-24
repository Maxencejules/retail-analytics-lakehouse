# Spark Structured Streaming: Transactions to Gold

## Overview

This application consumes retail transactions from Kafka topic `transactions`, validates and parses JSON with an explicit schema, aggregates revenue windows, and writes Gold outputs using Delta Lake when available (or Parquet fallback).

Implemented outputs:
- 5-minute tumbling window revenue by `store_id`
- 1-hour sliding window revenue by `channel`
- Quarantine stream for malformed/invalid records

## Run Instructions

### 1. Prerequisites

- Apache Spark 3.x with Kafka source support:
  - `org.apache.spark:spark-sql-kafka-0-10_2.12:<spark_version>`
- Kafka cluster reachable from Spark
- Optional Delta support (preferred):
  - `io.delta:delta-spark_2.12:<delta_version>`
  - Spark SQL configs:
    - `spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension`
    - `spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog`

### 2. Example command (Delta preferred)

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  spark/streaming/app.py \
  --kafka-bootstrap-servers localhost:9092 \
  --kafka-topic transactions \
  --output-format auto \
  --checkpoint-root s3://retail-loyalty-lake/dev/checkpoints/transactions_streaming \
  --gold-root s3://retail-loyalty-lake/dev/gold
```

### 3. Example command (Parquet-only environments)

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  spark/streaming/app.py \
  --kafka-bootstrap-servers localhost:9092 \
  --kafka-topic transactions \
  --output-format parquet \
  --checkpoint-root data/checkpoints/transactions_streaming \
  --gold-root data/gold
```

### 4. Key output paths

- `.../gold/store_revenue_5m`
- `.../gold/channel_revenue_1h_sliding`
- `.../gold/quarantine/transactions_malformed`

## Fault Tolerance Design

- Spark Structured Streaming checkpoints each query independently under `--checkpoint-root`.
- On restart, each query resumes from its checkpointed offsets/state.
- Kafka source offsets are tracked in Spark checkpoint metadata (not externalized manually).
- Sink writes are atomic at micro-batch boundaries.
- In Delta mode, transactional commits provide strong exactly-once sink behavior.
- In Parquet file sink mode, Spark checkpoint + commit protocol prevents duplicate file commits per completed micro-batch.
- Malformed JSON does not crash the stream; records are isolated to quarantine output.

## Watermarking Strategy

- A watermark (`--watermark-delay`, default `30 minutes`) is applied on parsed `event_time` (`ts_utc`).
- Watermark purpose:
  - bound state size for window aggregations
  - allow late-arriving data up to configured threshold
  - finalize window output in append mode once watermark passes window end
- 5-minute store tumbling windows and 1-hour channel sliding windows both use the same event-time watermark.
- Records arriving later than the watermark threshold are excluded from window updates, trading perfect completeness for predictable state and latency.

## Operational Notes

- Graceful shutdown is handled via `SIGINT`/`SIGTERM`; active queries are stopped cleanly before Spark session shutdown.
- Use dedicated checkpoint paths per environment and application instance.
- For production scale, tune:
  - `--max-offsets-per-trigger`
  - Spark shuffle partitions
  - Kafka topic partitions
  - cluster autoscaling policies

Phase 3 adaptive scaling controls are supported through shared profile environment variables:
- `SPARK_WORKLOAD_PROFILE=cost_saver|balanced|high_throughput`
- `SPARK_MIN_EXECUTORS`
- `SPARK_INITIAL_EXECUTORS`
- `SPARK_MAX_EXECUTORS`
- `SPARK_SHUFFLE_PARTITIONS`
