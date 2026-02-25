"""Structured Streaming job for retail transaction aggregates."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import logging
import signal
import sys
import time
from typing import Iterable, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from spark.common.lineage import OpenLineageConfig, apply_openlineage
from spark.common.performance import SparkPerformanceProfile, apply_performance_profile

LOGGER = logging.getLogger("spark.streaming.transactions")

TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=True),
        StructField("ts_utc", StringType(), nullable=True),
        StructField("store_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("channel", StringType(), nullable=True),
        StructField("promo_id", StringType(), nullable=True),
        StructField("_corrupt_record", StringType(), nullable=True),
    ]
)

UUID_PATTERN = (
    "^[0-9a-fA-F]{8}-"
    "[0-9a-fA-F]{4}-"
    "[1-5][0-9a-fA-F]{3}-"
    "[89abAB][0-9a-fA-F]{3}-"
    "[0-9a-fA-F]{12}$"
)

ALLOWED_CHANNELS: tuple[str, ...] = ("online", "store")
ALLOWED_PAYMENT_METHODS: tuple[str, ...] = (
    "credit_card",
    "debit_card",
    "cash",
    "mobile_wallet",
)


@dataclass(frozen=True, slots=True)
class StreamingConfig:
    """Runtime configuration for the streaming application."""

    kafka_bootstrap_servers: str
    kafka_topic: str = "transactions"
    starting_offsets: str = "latest"
    max_offsets_per_trigger: int | None = None
    trigger_interval: str = "1 minute"
    watermark_delay: str = "30 minutes"
    channel_slide_duration: str = "5 minutes"
    checkpoint_root: str = "data/checkpoints/transactions_streaming"
    gold_root: str = "data/gold"
    output_format: str = "auto"
    app_name: str = "retail-transactions-streaming"
    log_level: str = "INFO"

    def validate(self) -> None:
        if not self.kafka_bootstrap_servers.strip():
            raise ValueError("kafka_bootstrap_servers cannot be empty")

        if not self.kafka_topic.strip():
            raise ValueError("kafka_topic cannot be empty")

        if self.starting_offsets not in {"earliest", "latest"}:
            raise ValueError("starting_offsets must be 'earliest' or 'latest'")

        if self.max_offsets_per_trigger is not None and self.max_offsets_per_trigger <= 0:
            raise ValueError("max_offsets_per_trigger must be > 0 when provided")

        if self.output_format not in {"auto", "delta", "parquet"}:
            raise ValueError("output_format must be one of: auto, delta, parquet")


class GracefulShutdown:
    """Signal-driven shutdown control."""

    def __init__(self) -> None:
        self.stop_requested = False

    def install(self) -> None:
        signal.signal(signal.SIGINT, self._request_shutdown)
        signal.signal(signal.SIGTERM, self._request_shutdown)

    def _request_shutdown(self, signum: int, _: object) -> None:
        self.stop_requested = True
        LOGGER.info("shutdown_signal_received signal=%s", signum)


def configure_logging(level: str) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


def build_spark_session(app_name: str) -> SparkSession:
    profile = SparkPerformanceProfile.from_env()
    lineage = OpenLineageConfig.from_env(default_parent_job_name=app_name)
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.streaming.metricsEnabled", "true")
    )
    builder = apply_performance_profile(builder, profile)
    builder = apply_openlineage(builder, lineage)
    spark = builder.getOrCreate()
    LOGGER.info("spark_performance_profile_applied %s", profile.as_dict())
    return spark


def is_delta_runtime_ready(spark: SparkSession) -> bool:
    try:
        spark._jvm.java.lang.Class.forName("io.delta.tables.DeltaTable")
    except Exception:
        return False

    extensions = spark.conf.get("spark.sql.extensions", "")
    catalog = spark.conf.get("spark.sql.catalog.spark_catalog", "")
    return (
        "io.delta.sql.DeltaSparkSessionExtension" in extensions
        and "org.apache.spark.sql.delta.catalog.DeltaCatalog" in catalog
    )


def resolve_sink_format(config: StreamingConfig, spark: SparkSession) -> str:
    if config.output_format == "parquet":
        LOGGER.info("sink_format_selected format=parquet")
        return "parquet"

    if config.output_format == "delta":
        if not is_delta_runtime_ready(spark):
            raise RuntimeError(
                "Delta format requested but runtime is not configured. "
                "Provide Delta dependencies and Spark SQL extensions."
            )
        LOGGER.info("sink_format_selected format=delta")
        return "delta"

    if is_delta_runtime_ready(spark):
        LOGGER.info("sink_format_selected format=delta")
        return "delta"

    LOGGER.warning(
        "sink_format_fallback requested=auto selected=parquet reason=delta_unavailable"
    )
    return "parquet"


def read_kafka_stream(spark: SparkSession, config: StreamingConfig) -> DataFrame:
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap_servers)
        .option("subscribe", config.kafka_topic)
        .option("startingOffsets", config.starting_offsets)
        .option("failOnDataLoss", "false")
    )

    if config.max_offsets_per_trigger is not None:
        reader = reader.option(
            "maxOffsetsPerTrigger",
            str(config.max_offsets_per_trigger),
        )

    return reader.load().selectExpr(
        "CAST(value AS STRING) AS raw_json",
        "CAST(key AS STRING) AS kafka_key",
        "timestamp AS kafka_ingest_ts",
        "topic",
        "partition",
        "offset",
    )


def parse_and_validate_transactions(raw_stream: DataFrame) -> tuple[DataFrame, DataFrame]:
    parsed = raw_stream.withColumn(
        "payload",
        F.from_json(
            F.col("raw_json"),
            TRANSACTION_SCHEMA,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )

    flattened = parsed.select(
        "raw_json",
        "kafka_key",
        "kafka_ingest_ts",
        "topic",
        "partition",
        "offset",
        "payload.*",
    ).withColumn("event_time", F.to_timestamp(F.col("ts_utc")))

    is_valid = (
        F.col("_corrupt_record").isNull()
        & F.col("transaction_id").rlike(UUID_PATTERN)
        & F.col("event_time").isNotNull()
        & F.col("store_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("product_id").isNotNull()
        & F.col("quantity").isNotNull()
        & F.col("unit_price").isNotNull()
        & (F.col("quantity") > F.lit(0))
        & (F.col("unit_price") > F.lit(0.0))
        & F.col("currency").isNotNull()
        & F.col("payment_method").isin(*ALLOWED_PAYMENT_METHODS)
        & F.col("channel").isin(*ALLOWED_CHANNELS)
    )

    with_flags = (
        flattened.withColumn("is_valid", is_valid)
        .withColumn(
            "error_reason",
            F.when(F.col("_corrupt_record").isNotNull(), F.lit("malformed_json"))
            .when(F.col("transaction_id").isNull(), F.lit("missing_transaction_id"))
            .when(~F.col("transaction_id").rlike(UUID_PATTERN), F.lit("invalid_uuid"))
            .when(F.col("event_time").isNull(), F.lit("invalid_ts_utc"))
            .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), F.lit("invalid_quantity"))
            .when(
                F.col("unit_price").isNull() | (F.col("unit_price") <= 0.0),
                F.lit("invalid_unit_price"),
            )
            .when(~F.col("channel").isin(*ALLOWED_CHANNELS), F.lit("invalid_channel"))
            .when(
                ~F.col("payment_method").isin(*ALLOWED_PAYMENT_METHODS),
                F.lit("invalid_payment_method"),
            )
            .otherwise(F.lit("unknown_validation_error")),
        )
    )

    valid = (
        with_flags.filter(F.col("is_valid"))
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .drop("_corrupt_record", "is_valid", "error_reason", "raw_json")
    )

    malformed = with_flags.filter(~F.col("is_valid")).select(
        "kafka_ingest_ts",
        "topic",
        "partition",
        "offset",
        "raw_json",
        "error_reason",
    )

    return valid, malformed


def aggregate_store_revenue_5m(valid_transactions: DataFrame, watermark_delay: str) -> DataFrame:
    return (
        valid_transactions.withWatermark("event_time", watermark_delay)
        .groupBy(F.window(F.col("event_time"), "5 minutes"), F.col("store_id"))
        .agg(
            F.round(F.sum("revenue"), 2).alias("revenue_total"),
            F.sum("quantity").alias("units_sold"),
            F.count("*").alias("transaction_count"),
        )
        .select(
            F.col("window.start").alias("window_start_utc"),
            F.col("window.end").alias("window_end_utc"),
            F.col("store_id"),
            F.col("revenue_total"),
            F.col("units_sold"),
            F.col("transaction_count"),
            F.current_timestamp().alias("processed_at_utc"),
        )
        .withColumn("window_date", F.to_date(F.col("window_start_utc")))
    )


def aggregate_channel_revenue_1h_sliding(
    valid_transactions: DataFrame,
    watermark_delay: str,
    slide_duration: str,
) -> DataFrame:
    return (
        valid_transactions.withWatermark("event_time", watermark_delay)
        .groupBy(F.window(F.col("event_time"), "1 hour", slide_duration), F.col("channel"))
        .agg(
            F.round(F.sum("revenue"), 2).alias("revenue_total"),
            F.sum("quantity").alias("units_sold"),
            F.count("*").alias("transaction_count"),
        )
        .select(
            F.col("window.start").alias("window_start_utc"),
            F.col("window.end").alias("window_end_utc"),
            F.col("channel"),
            F.col("revenue_total"),
            F.col("units_sold"),
            F.col("transaction_count"),
            F.current_timestamp().alias("processed_at_utc"),
        )
        .withColumn("window_date", F.to_date(F.col("window_start_utc")))
    )


def start_sink_query(
    dataframe: DataFrame,
    *,
    query_name: str,
    sink_format: str,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str,
    partition_cols: Sequence[str],
) -> StreamingQuery:
    writer = (
        dataframe.writeStream.queryName(query_name)
        .outputMode("append")
        .format(sink_format)
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .partitionBy(*partition_cols)
    )

    if sink_format == "delta":
        writer = writer.option("mergeSchema", "true")

    return writer.start()


def stop_queries(queries: Iterable[StreamingQuery], timeout_seconds: int = 60) -> None:
    for query in queries:
        if query.isActive:
            LOGGER.info("stopping_query name=%s id=%s", query.name, query.id)
            query.stop()

    for query in queries:
        try:
            query.awaitTermination(timeout_seconds)
        except Exception as exc:  # pragma: no cover
            LOGGER.warning("query_termination_warning name=%s error=%s", query.name, exc)


def run(config: StreamingConfig) -> None:
    config.validate()
    configure_logging(config.log_level)

    spark = build_spark_session(config.app_name)
    sink_format = resolve_sink_format(config, spark)

    LOGGER.info(
        "stream_start app=%s topic=%s sink_format=%s",
        config.app_name,
        config.kafka_topic,
        sink_format,
    )

    raw_stream = read_kafka_stream(spark, config)
    valid_transactions, malformed_transactions = parse_and_validate_transactions(raw_stream)

    store_agg = aggregate_store_revenue_5m(valid_transactions, config.watermark_delay)
    channel_agg = aggregate_channel_revenue_1h_sliding(
        valid_transactions,
        config.watermark_delay,
        config.channel_slide_duration,
    )

    queries = [
        start_sink_query(
            store_agg,
            query_name="gold_store_revenue_5m",
            sink_format=sink_format,
            output_path=f"{config.gold_root}/store_revenue_5m",
            checkpoint_path=f"{config.checkpoint_root}/gold_store_revenue_5m",
            trigger_interval=config.trigger_interval,
            partition_cols=("window_date", "store_id"),
        ),
        start_sink_query(
            channel_agg,
            query_name="gold_channel_revenue_1h_sliding",
            sink_format=sink_format,
            output_path=f"{config.gold_root}/channel_revenue_1h_sliding",
            checkpoint_path=f"{config.checkpoint_root}/gold_channel_revenue_1h_sliding",
            trigger_interval=config.trigger_interval,
            partition_cols=("window_date", "channel"),
        ),
        start_sink_query(
            malformed_transactions,
            query_name="gold_transactions_malformed",
            sink_format=sink_format,
            output_path=f"{config.gold_root}/quarantine/transactions_malformed",
            checkpoint_path=f"{config.checkpoint_root}/gold_transactions_malformed",
            trigger_interval=config.trigger_interval,
            partition_cols=("topic",),
        ),
    ]

    shutdown = GracefulShutdown()
    shutdown.install()

    try:
        while not shutdown.stop_requested:
            for query in queries:
                if not query.isActive:
                    raise RuntimeError(f"query '{query.name}' stopped unexpectedly")

                query_exception = query.exception()
                if query_exception is not None:
                    raise RuntimeError(f"query '{query.name}' failed: {query_exception}")

            time.sleep(5)
    except KeyboardInterrupt:
        LOGGER.info("keyboard_interrupt_received")
    finally:
        stop_queries(queries)
        spark.stop()
        LOGGER.info("stream_stopped app=%s", config.app_name)


def parse_args(argv: Sequence[str] | None = None) -> StreamingConfig:
    parser = argparse.ArgumentParser(
        description="Spark Structured Streaming application for retail transaction analytics."
    )
    parser.add_argument("--kafka-bootstrap-servers", required=True)
    parser.add_argument("--kafka-topic", default="transactions")
    parser.add_argument("--starting-offsets", default="latest", choices=("earliest", "latest"))
    parser.add_argument("--max-offsets-per-trigger", type=int, default=None)
    parser.add_argument("--trigger-interval", default="1 minute")
    parser.add_argument("--watermark-delay", default="30 minutes")
    parser.add_argument("--channel-slide-duration", default="5 minutes")
    parser.add_argument("--checkpoint-root", default="data/checkpoints/transactions_streaming")
    parser.add_argument("--gold-root", default="data/gold")
    parser.add_argument("--output-format", default="auto", choices=("auto", "delta", "parquet"))
    parser.add_argument("--app-name", default="retail-transactions-streaming")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))

    args = parser.parse_args(argv)
    return StreamingConfig(
        kafka_bootstrap_servers=args.kafka_bootstrap_servers,
        kafka_topic=args.kafka_topic,
        starting_offsets=args.starting_offsets,
        max_offsets_per_trigger=args.max_offsets_per_trigger,
        trigger_interval=args.trigger_interval,
        watermark_delay=args.watermark_delay,
        channel_slide_duration=args.channel_slide_duration,
        checkpoint_root=args.checkpoint_root,
        gold_root=args.gold_root,
        output_format=args.output_format,
        app_name=args.app_name,
        log_level=args.log_level,
    )


def main(argv: Sequence[str] | None = None) -> int:
    config = parse_args(argv)
    run(config)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
