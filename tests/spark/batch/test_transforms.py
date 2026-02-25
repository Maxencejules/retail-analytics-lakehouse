"""Unit tests for batch ETL transformation logic."""

from __future__ import annotations

from datetime import date, datetime, timezone
import os
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
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
from spark.batch.transforms import (
    build_gold_customer_lifetime_value,
    build_gold_daily_revenue_by_store,
    build_gold_top_10_products_by_day,
    transform_bronze_to_silver,
)

BRONZE_TEST_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=True),
        StructField("ts_utc", StringType(), nullable=True),
        StructField("store_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", StringType(), nullable=True),
        StructField("unit_price", StringType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("payment_method", StringType(), nullable=True),
        StructField("channel", StringType(), nullable=True),
        StructField("promo_id", StringType(), nullable=True),
        StructField("ingestion_date", StringType(), nullable=True),
    ]
)

SILVER_TEST_SCHEMA = StructType(
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


def _create_bronze_df(spark: SparkSession, rows: list[dict[str, object]]):
    return spark.createDataFrame(rows, schema=BRONZE_TEST_SCHEMA)


def _create_silver_df(spark: SparkSession, rows: list[dict[str, object]]):
    return spark.createDataFrame(rows, schema=SILVER_TEST_SCHEMA)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    tmp_root = Path(".tmp/pytest-spark")
    tmp_root.mkdir(parents=True, exist_ok=True)
    resolved_tmp = str(tmp_root.resolve())

    os.environ["TEMP"] = resolved_tmp
    os.environ["TMP"] = resolved_tmp

    session = (
        SparkSession.builder.master("local[2]")
        .appName("batch-transforms-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", resolved_tmp)
        .getOrCreate()
    )
    yield session
    session.stop()


def test_silver_casts_normalizes_and_handles_null_promo(spark: SparkSession) -> None:
    bronze_df = _create_bronze_df(
        spark,
        [
            {
                "transaction_id": "tx-1",
                "ts_utc": "2026-02-24T10:15:00Z",
                "store_id": "STORE-0001",
                "customer_id": "CUST-1",
                "product_id": "PROD-1",
                "quantity": "2",
                "unit_price": "10.50",
                "currency": "cad$",
                "payment_method": "Credit_Card",
                "channel": "STORE",
                "promo_id": "",
                "ingestion_date": "2026-02-24",
            }
        ],
    )

    silver_df = transform_bronze_to_silver(bronze_df, fail_fast_quality=True)
    row = silver_df.collect()[0]

    assert row.quantity == 2
    assert row.unit_price == pytest.approx(10.5)
    assert row.currency == "CAD"
    assert row.payment_method == "credit_card"
    assert row.channel == "store"
    assert row.promo_id is None
    assert row.revenue == pytest.approx(21.0)
    assert row.event_date == date(2026, 2, 24)


def test_silver_deduplicates_by_latest_timestamp(spark: SparkSession) -> None:
    bronze_df = _create_bronze_df(
        spark,
        [
            {
                "transaction_id": "tx-dup",
                "ts_utc": "2026-02-24T09:00:00Z",
                "store_id": "STORE-0001",
                "customer_id": "CUST-1",
                "product_id": "PROD-1",
                "quantity": "1",
                "unit_price": "10.00",
                "currency": "CAD",
                "payment_method": "debit_card",
                "channel": "store",
                "promo_id": None,
                "ingestion_date": "2026-02-24",
            },
            {
                "transaction_id": "tx-dup",
                "ts_utc": "2026-02-24T10:00:00Z",
                "store_id": "STORE-0001",
                "customer_id": "CUST-1",
                "product_id": "PROD-1",
                "quantity": "2",
                "unit_price": "11.00",
                "currency": "CAD",
                "payment_method": "debit_card",
                "channel": "store",
                "promo_id": None,
                "ingestion_date": "2026-02-24",
            },
        ],
    )

    silver_df = transform_bronze_to_silver(bronze_df, fail_fast_quality=True)
    rows = silver_df.collect()

    assert len(rows) == 1
    assert rows[0].quantity == 2
    assert rows[0].unit_price == pytest.approx(11.0)


def test_silver_fail_fast_on_critical_quality_violations(spark: SparkSession) -> None:
    bronze_df = _create_bronze_df(
        spark,
        [
            {
                "transaction_id": "tx-bad",
                "ts_utc": "2026-02-24T09:00:00Z",
                "store_id": "STORE-0001",
                "customer_id": "CUST-1",
                "product_id": "PROD-1",
                "quantity": "-1",
                "unit_price": "10.00",
                "currency": "CAD",
                "payment_method": "debit_card",
                "channel": "store",
                "promo_id": None,
                "ingestion_date": "2026-02-24",
            }
        ],
    )

    with pytest.raises(DataQualityError, match="critical data quality violations"):
        transform_bronze_to_silver(bronze_df, fail_fast_quality=True)


def test_gold_aggregations_produce_expected_metrics(spark: SparkSession) -> None:
    silver_df = _create_silver_df(
        spark,
        [
            {
                "transaction_id": "t1",
                "ts_utc": datetime(2026, 2, 24, 8, 0, tzinfo=timezone.utc),
                "store_id": "STORE-1",
                "customer_id": "CUST-1",
                "product_id": "PROD-1",
                "quantity": 2,
                "unit_price": 5.0,
                "currency": "CAD",
                "payment_method": "credit_card",
                "channel": "online",
                "promo_id": None,
                "revenue": 10.0,
                "event_date": date(2026, 2, 24),
                "ingestion_date": date(2026, 2, 24),
            },
            {
                "transaction_id": "t2",
                "ts_utc": datetime(2026, 2, 24, 9, 0, tzinfo=timezone.utc),
                "store_id": "STORE-1",
                "customer_id": "CUST-1",
                "product_id": "PROD-2",
                "quantity": 1,
                "unit_price": 20.0,
                "currency": "CAD",
                "payment_method": "credit_card",
                "channel": "store",
                "promo_id": None,
                "revenue": 20.0,
                "event_date": date(2026, 2, 24),
                "ingestion_date": date(2026, 2, 24),
            },
        ],
    )

    daily_store = build_gold_daily_revenue_by_store(silver_df).collect()
    assert len(daily_store) == 1
    assert daily_store[0].daily_revenue == pytest.approx(30.0)
    assert daily_store[0].units_sold == 3
    assert daily_store[0].transaction_count == 2

    top_products = build_gold_top_10_products_by_day(silver_df).collect()
    assert len(top_products) == 2
    assert top_products[0].rank == 1
    assert top_products[0].product_id == "PROD-2"

    customer_ltv = build_gold_customer_lifetime_value(
        silver_df,
        snapshot_date="2026-02-24",
    ).collect()
    assert len(customer_ltv) == 1
    assert customer_ltv[0].lifetime_value == pytest.approx(30.0)
    assert customer_ltv[0].transaction_count == 2
    assert customer_ltv[0].snapshot_date == date(2026, 2, 24)
