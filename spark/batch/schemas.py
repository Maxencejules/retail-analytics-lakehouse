"""Schema definitions and schema validation for transaction ETL."""

from __future__ import annotations

from typing import Iterable

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spark.batch.exceptions import DataQualityError

TRANSACTION_REQUIRED_COLUMNS: tuple[str, ...] = (
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
)

SILVER_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("ts_utc", TimestampType(), nullable=False),
        StructField("store_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("product_id", StringType(), nullable=False),
        StructField("quantity", IntegerType(), nullable=False),
        StructField("unit_price", DoubleType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("payment_method", StringType(), nullable=False),
        StructField("channel", StringType(), nullable=False),
        StructField("promo_id", StringType(), nullable=True),
        StructField("revenue", DoubleType(), nullable=False),
        StructField("event_date", DateType(), nullable=False),
        StructField("ingestion_date", DateType(), nullable=False),
    ]
)


def validate_required_columns(df: DataFrame, required_columns: Iterable[str], stage: str) -> None:
    """Ensure required columns exist before transformations run."""
    missing = [column for column in required_columns if column not in df.columns]
    if missing:
        raise DataQualityError(
            f"{stage}: missing required columns: {', '.join(sorted(missing))}"
        )


def validate_schema_exact(df: DataFrame, expected_schema: StructType, stage: str) -> None:
    """Validate column order and data types exactly for deterministic contracts."""
    actual_fields = list(df.schema.fields)
    expected_fields = list(expected_schema.fields)

    if len(actual_fields) != len(expected_fields):
        raise DataQualityError(
            f"{stage}: schema field count mismatch actual={len(actual_fields)} "
            f"expected={len(expected_fields)}"
        )

    for index, (actual, expected) in enumerate(zip(actual_fields, expected_fields), start=1):
        if actual.name != expected.name:
            raise DataQualityError(
                f"{stage}: schema column mismatch at position {index}: "
                f"actual={actual.name} expected={expected.name}"
            )

        if actual.dataType.simpleString() != expected.dataType.simpleString():
            raise DataQualityError(
                f"{stage}: data type mismatch for column {expected.name}: "
                f"actual={actual.dataType.simpleString()} expected={expected.dataType.simpleString()}"
            )

