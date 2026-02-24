"""Orchestration for the retail transactions batch ETL pipeline."""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark.batch.config import PipelineConfig
from spark.batch.io import ensure_delta_available, read_dataset, read_raw_transactions, write_dataset
from spark.batch.transforms import (
    build_gold_customer_lifetime_value,
    build_gold_daily_revenue_by_store,
    build_gold_top_10_products_by_day,
    prepare_bronze,
    transform_bronze_to_silver,
)
from spark.common.performance import SparkPerformanceProfile, apply_performance_profile

LOGGER = logging.getLogger("spark.batch.pipeline")


def build_spark_session(config: PipelineConfig) -> SparkSession:
    profile = SparkPerformanceProfile.from_env()
    builder = (
        SparkSession.builder.appName(config.app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    builder = apply_performance_profile(builder, profile)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    LOGGER.info("spark_performance_profile_applied %s", profile.as_dict())
    return spark


def _log_count(df_name: str, count: int) -> None:
    LOGGER.info("record_count", extra={"context": {"dataset": df_name, "rows": count}})


def run_pipeline(config: PipelineConfig) -> None:
    """Execute Bronze, Silver, and Gold ETL stages."""
    config.validate()
    spark = build_spark_session(config)

    if config.table_format == "delta":
        ensure_delta_available(spark)

    try:
        LOGGER.info(
            "pipeline_start",
            extra={
                "context": {
                    "input_path": config.input_path,
                    "input_format": config.input_format,
                    "output_base_path": config.base_path,
                    "output_target": config.output_target,
                    "table_format": config.table_format,
                    "ingestion_date": config.ingestion_date,
                }
            },
        )

        raw_df = read_raw_transactions(
            spark,
            input_path=config.input_path,
            input_format=config.input_format,
        )
        _log_count("raw_transactions", raw_df.count())

        bronze_df = prepare_bronze(raw_df, ingestion_date=config.ingestion_date)
        # Bronze is intentionally raw: only ingestion_date metadata is appended for partition management.
        write_dataset(
            bronze_df,
            path=config.bronze_transactions_path,
            table_format=config.table_format,
            partition_by=["ingestion_date"],
            mode="overwrite",
        )

        bronze_current_df = (
            read_dataset(
                spark,
                path=config.bronze_transactions_path,
                table_format=config.table_format,
            )
            .where(F.col("ingestion_date") == F.to_date(F.lit(config.ingestion_date)))
        )
        _log_count("bronze_transactions_ingestion_partition", bronze_current_df.count())

        silver_df = transform_bronze_to_silver(
            bronze_current_df,
            fail_fast_quality=config.fail_fast_quality,
        )
        _log_count("silver_transactions", silver_df.count())
        write_dataset(
            silver_df,
            path=config.silver_transactions_path,
            table_format=config.table_format,
            partition_by=["event_date"],
            mode="overwrite",
        )

        gold_daily_store = build_gold_daily_revenue_by_store(silver_df)
        _log_count("gold_daily_revenue_by_store", gold_daily_store.count())
        write_dataset(
            gold_daily_store,
            path=config.gold_store_daily_path,
            table_format=config.table_format,
            partition_by=["event_date"],
            mode="overwrite",
        )

        gold_top_products = build_gold_top_10_products_by_day(silver_df)
        _log_count("gold_top_10_products_by_day", gold_top_products.count())
        write_dataset(
            gold_top_products,
            path=config.gold_top_products_path,
            table_format=config.table_format,
            partition_by=["event_date"],
            mode="overwrite",
        )

        gold_customer_ltv = build_gold_customer_lifetime_value(
            silver_df,
            snapshot_date=config.ingestion_date,
        )
        _log_count("gold_customer_lifetime_value", gold_customer_ltv.count())
        write_dataset(
            gold_customer_ltv,
            path=config.gold_customer_ltv_path,
            table_format=config.table_format,
            partition_by=["snapshot_date"],
            mode="overwrite",
        )

        LOGGER.info(
            "pipeline_complete",
            extra={
                "context": {
                    "bronze_path": config.bronze_transactions_path,
                    "silver_path": config.silver_transactions_path,
                    "gold_paths": {
                        "daily_revenue_by_store": config.gold_store_daily_path,
                        "top_10_products_by_day": config.gold_top_products_path,
                        "customer_lifetime_value": config.gold_customer_ltv_path,
                    },
                }
            },
        )
    finally:
        spark.stop()
