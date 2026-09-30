"""Adversarial records and order independence, executed by the real Spark JVM."""

from dataclasses import replace

import pytest
from pyspark.sql.types import StringType, StructField, StructType

from spark.batch.config import PipelineConfig
from spark.batch.exceptions import DataQualityError
from spark.batch.schemas import TRANSACTION_REQUIRED_COLUMNS
from spark.batch.transforms import (
    build_gold_daily_revenue_by_store,
    build_gold_top_10_products_by_day,
    transform_bronze_to_silver,
)


def row(**overrides):
    result = dict(
        zip(
            TRANSACTION_REQUIRED_COLUMNS,
            (
                "t1",
                "2025-01-01T12:00:00Z",
                "s1",
                "c1",
                "p1",
                "2",
                "10.00",
                "CAD",
                "credit_card",
                "store",
                None,
            ),
        )
    )
    result["ingestion_date"] = "2025-02-01"
    return {**result, **overrides}


def bronze(spark, rows):
    schema = StructType([StructField(k, StringType(), True) for k in row()])
    return spark.createDataFrame(rows, schema)


def test_null_categories_nonfinite_numbers_and_lossy_quantities(spark):
    invalid = [
        row(transaction_id=f"bad-{i}", **change)
        for i, change in enumerate(
            (
                {"currency": None},
                {"payment_method": None},
                {"channel": None},
                {"unit_price": "NaN"},
                {"unit_price": "Infinity"},
                {"unit_price": "-Infinity"},
                {"unit_price": "1e308", "quantity": "8"},
                {"quantity": "1.9"},
                {"quantity": "2147483648"},
                {"ingestion_date": "not-a-date"},
            )
        )
    ]
    data = bronze(spark, [row(), *invalid])
    with pytest.raises(DataQualityError, match="critical data quality"):
        transform_bronze_to_silver(data)
    assert [
        r.transaction_id
        for r in transform_bronze_to_silver(data, fail_fast_quality=False).collect()
    ] == ["t1"]


def test_conflicting_latest_version_is_rejected_even_in_filter_mode(spark):
    for rows in ([row(), row(quantity="3")], [row(quantity="3"), row()]):
        with pytest.raises(DataQualityError, match="conflicting latest versions"):
            transform_bronze_to_silver(
                bronze(spark, rows).repartition(2), fail_fast_quality=False
            )


def test_exact_retry_and_later_ingestion_version_are_order_independent(spark):
    records = [row(), row(), row(quantity="3", ingestion_date="2025-02-02")]
    expected = None
    for records_in_order, partitions in ((records, 1), (list(reversed(records)), 3)):
        actual = transform_bronze_to_silver(
            bronze(spark, records_in_order).repartition(partitions)
        ).collect()
        assert len(actual) == 1 and actual[0].quantity == 3
        current = actual[0].asDict()
        assert expected is None or current == expected
        expected = current


def test_gold_never_adds_currencies_and_ranks_top_ten_per_currency(spark):
    rows = [
        row(transaction_id=f"{currency}-{i}", product_id=f"p{i:02d}", currency=currency)
        for currency in ("CAD", "USD")
        for i in range(12)
    ]
    silver = transform_bronze_to_silver(bronze(spark, rows))
    daily = {r.currency: r for r in build_gold_daily_revenue_by_store(silver).collect()}
    assert set(daily) == {"CAD", "USD"}
    assert all(
        r.daily_revenue == 240 and r.transaction_count == 12 for r in daily.values()
    )
    ranked = build_gold_top_10_products_by_day(silver).collect()
    assert len(ranked) == 20
    for currency in ("CAD", "USD"):
        products = sorted(
            (r.rank, r.product_id) for r in ranked if r.currency == currency
        )
        assert products == [(i + 1, f"p{i:02d}") for i in range(10)]


@pytest.mark.parametrize("value", ["20250201", "2025-W05-6", "2025-02-30"])
def test_ingestion_date_requires_actual_canonical_calendar_date(value):
    config = PipelineConfig(
        "in.csv", "csv", "out", "local", "2025-02-01", "parquet", "INFO", "test", True
    )
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        replace(config, ingestion_date=value).validate()
