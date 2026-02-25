"""Bronze->Silver->Gold transformations for retail transaction data."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column
from pyspark.sql.window import Window

from spark.batch.exceptions import DataQualityError
from spark.batch.schemas import (
    SILVER_SCHEMA,
    TRANSACTION_REQUIRED_COLUMNS,
    validate_required_columns,
    validate_schema_exact,
)

ALLOWED_CURRENCIES: tuple[str, ...] = ("CAD", "USD")
ALLOWED_CHANNELS: tuple[str, ...] = ("online", "store")
ALLOWED_PAYMENT_METHODS: tuple[str, ...] = (
    "credit_card",
    "debit_card",
    "cash",
    "mobile_wallet",
)


def prepare_bronze(raw_df: DataFrame, *, ingestion_date: str) -> DataFrame:
    """
    Prepare Bronze records.

    We only append ingestion metadata for partitioning and do not alter
    business values, preserving raw payload fidelity in Bronze.
    """
    validate_required_columns(raw_df, TRANSACTION_REQUIRED_COLUMNS, stage="bronze")
    return raw_df.withColumn("ingestion_date", F.to_date(F.lit(ingestion_date)))


def _normalize_currency_expr() -> Column:
    cleaned = F.upper(F.trim(F.col("currency")))
    return (
        F.when(cleaned.isin("CAD", "C$", "CAD$"), F.lit("CAD"))
        .when(cleaned.isin("USD", "US$", "USD$"), F.lit("USD"))
        .otherwise(cleaned)
    )


def _cast_bronze_to_silver(bronze_df: DataFrame) -> DataFrame:
    # Explicit casting isolates type harmonization in one place and keeps downstream contracts stable.
    return (
        bronze_df.select(
            F.trim(F.col("transaction_id")).alias("transaction_id"),
            F.to_timestamp(F.col("ts_utc")).alias("ts_utc"),
            F.trim(F.col("store_id")).alias("store_id"),
            F.trim(F.col("customer_id")).alias("customer_id"),
            F.trim(F.col("product_id")).alias("product_id"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("unit_price").cast("double").alias("unit_price"),
            _normalize_currency_expr().alias("currency"),
            F.lower(F.trim(F.col("payment_method"))).alias("payment_method"),
            F.lower(F.trim(F.col("channel"))).alias("channel"),
            F.when(
                F.col("promo_id").isNull() | (F.trim(F.col("promo_id")) == ""),
                F.lit(None),
            )
            .otherwise(F.trim(F.col("promo_id")))
            .alias("promo_id"),
            F.to_date(F.col("ingestion_date")).alias("ingestion_date"),
        )
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .withColumn("event_date", F.to_date(F.col("ts_utc")))
    )


def _annotate_quality_errors(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "dq_error",
        F.when(
            F.col("transaction_id").isNull() | (F.col("transaction_id") == ""),
            "missing_transaction_id",
        )
        .when(F.col("ts_utc").isNull(), "invalid_ts_utc")
        .when(
            F.col("store_id").isNull() | (F.col("store_id") == ""), "missing_store_id"
        )
        .when(
            F.col("customer_id").isNull() | (F.col("customer_id") == ""),
            "missing_customer_id",
        )
        .when(
            F.col("product_id").isNull() | (F.col("product_id") == ""),
            "missing_product_id",
        )
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
        .when(
            F.col("unit_price").isNull() | (F.col("unit_price") <= 0),
            "invalid_unit_price",
        )
        .when(F.col("revenue").isNull() | (F.col("revenue") < 0), "invalid_revenue")
        .when(~F.col("currency").isin(*ALLOWED_CURRENCIES), "invalid_currency")
        .when(
            ~F.col("payment_method").isin(*ALLOWED_PAYMENT_METHODS),
            "invalid_payment_method",
        )
        .when(~F.col("channel").isin(*ALLOWED_CHANNELS), "invalid_channel")
        .otherwise(F.lit(None)),
    )


def _fail_fast_if_invalid(df: DataFrame, *, stage: str) -> None:
    invalid = df.filter(F.col("dq_error").isNotNull())
    if invalid.limit(1).count() == 0:
        return

    summary_rows = (
        invalid.groupBy("dq_error").count().orderBy(F.desc("count")).collect()
    )
    summary = ", ".join(f"{row['dq_error']}={row['count']}" for row in summary_rows)
    raise DataQualityError(
        f"{stage}: critical data quality violations found: {summary}"
    )


def _deduplicate_transactions(df: DataFrame) -> DataFrame:
    # Dedupe on transaction_id and keep the latest timestamp to preserve the most current transaction state.
    tie_breaker = Window.partitionBy("transaction_id").orderBy(
        F.col("ts_utc").desc_nulls_last(),
        F.col("ingestion_date").desc_nulls_last(),
        F.col("unit_price").desc_nulls_last(),
    )
    return (
        df.withColumn("_row_num", F.row_number().over(tie_breaker))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def transform_bronze_to_silver(
    bronze_df: DataFrame, *, fail_fast_quality: bool = True
) -> DataFrame:
    """Transform Bronze raw data into validated, typed Silver records."""
    validate_required_columns(
        bronze_df,
        tuple(TRANSACTION_REQUIRED_COLUMNS) + ("ingestion_date",),
        stage="silver",
    )

    casted = _cast_bronze_to_silver(bronze_df)
    checked = _annotate_quality_errors(casted)

    if fail_fast_quality:
        _fail_fast_if_invalid(checked, stage="silver")
        clean = checked.filter(F.col("dq_error").isNull()).drop("dq_error")
    else:
        clean = checked.filter(F.col("dq_error").isNull()).drop("dq_error")

    deduped = _deduplicate_transactions(clean)
    final_df = deduped.select(
        "transaction_id",
        "ts_utc",
        "store_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "currency",
        "payment_method",
        "channel",
        "promo_id",
        "revenue",
        "event_date",
        "ingestion_date",
    )
    validate_schema_exact(final_df, SILVER_SCHEMA, stage="silver")
    return final_df


def build_gold_daily_revenue_by_store(silver_df: DataFrame) -> DataFrame:
    """Aggregate daily revenue and volume by store."""
    return silver_df.groupBy("event_date", "store_id").agg(
        F.round(F.sum("revenue"), 2).alias("daily_revenue"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("transaction_id").alias("transaction_count"),
    )


def build_gold_top_10_products_by_day(silver_df: DataFrame) -> DataFrame:
    """
    Rank top products by daily revenue.

    We apply a deterministic row_number tie-breaker so reruns produce identical
    top-10 membership when revenues are equal.
    """
    product_daily = silver_df.groupBy("event_date", "product_id").agg(
        F.round(F.sum("revenue"), 2).alias("daily_revenue"),
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("transaction_id").alias("transaction_count"),
    )
    ranking = Window.partitionBy("event_date").orderBy(
        F.col("daily_revenue").desc(),
        F.col("units_sold").desc(),
        F.col("product_id").asc(),
    )
    return (
        product_daily.withColumn("rank", F.row_number().over(ranking))
        .filter(F.col("rank") <= 10)
        .select(
            "event_date",
            "rank",
            "product_id",
            "daily_revenue",
            "units_sold",
            "transaction_count",
        )
    )


def build_gold_customer_lifetime_value(
    silver_df: DataFrame, *, snapshot_date: str
) -> DataFrame:
    """Compute customer lifetime value as cumulative revenue across all transactions."""
    return (
        silver_df.groupBy("customer_id")
        .agg(
            F.round(F.sum("revenue"), 2).alias("lifetime_value"),
            F.countDistinct("transaction_id").alias("transaction_count"),
            F.min("ts_utc").alias("first_purchase_ts_utc"),
            F.max("ts_utc").alias("last_purchase_ts_utc"),
        )
        .withColumn("snapshot_date", F.to_date(F.lit(snapshot_date)))
    )
