from datetime import date
from unittest.mock import Mock

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dashboard.config import DashboardConfig
from dashboard.data_access import (
    DashboardFilters,
    GoldLayerRepository,
    PostgresWarehouseRepository,
)
from dashboard.insights import build_highlight_insights
from models.sales_features import load_gold_daily_revenue


def gold(tmp_path):
    root = tmp_path / "gold"
    rows = [
        dict(
            store_id="s1",
            currency=c,
            event_date=date(2025, 1, 1),
            daily_revenue=value,
            units_sold=1,
            transaction_count=1,
        )
        for c, value in (("CAD", 20.0), ("USD", 500.0), ("GBP", 7.0))
    ]
    for name in ("daily_revenue_by_store", "top_10_products_by_day"):
        path = root / name / "event_date=2025-01-01"
        path.mkdir(parents=True)
        records = [{k: v for k, v in r.items() if k != "event_date"} for r in rows]
        if name == "top_10_products_by_day":
            records = [{**r, "product_id": "p1", "rank": 1} for r in records]
        pq.write_table(pa.Table.from_pylist(records), path / "part.parquet")
    return root


@pytest.mark.parametrize(
    "currency,expected", [("CAD", 20.0), ("USD", 500.0), ("GBP", 7.0)]
)
def test_duckdb_gold_queries_keep_currency_separate(tmp_path, currency, expected):
    repository = GoldLayerRepository(str(gold(tmp_path)), currency=currency)
    filters = DashboardFilters(date(2025, 1, 1), date(2025, 1, 1))
    try:
        assert repository.kpi_summary(filters).total_revenue == expected
        assert repository.top_stores(filters).iloc[0].revenue == expected
        assert repository.top_products(filters).iloc[0].revenue == expected
        assert repository.revenue_trend(filters).iloc[0].revenue == expected
        assert repository.list_stores(filters) == ["s1"]
        assert str(repository.available_date_range()[0])[:10] == "2025-01-01"
    finally:
        repository._connection.close()


@pytest.mark.parametrize(
    "currency,expected", [("CAD", 20.0), ("USD", 500.0), ("GBP", 7.0)]
)
def test_ml_reads_hive_partition_dates_and_selected_currency(
    tmp_path, currency, expected
):
    rows = load_gold_daily_revenue(
        str(gold(tmp_path) / "daily_revenue_by_store"), currency=currency
    )
    assert len(rows) == 1 and rows[0].daily_revenue == expected
    assert rows[0].event_date == date(2025, 1, 1)


def test_unknown_currency_is_rejected_before_sql(tmp_path):
    with pytest.raises(ValueError, match="currency"):
        GoldLayerRepository(str(tmp_path), currency="CAD' OR 1=1 --")
    with pytest.raises(ValueError, match="DASHBOARD_CURRENCY"):
        DashboardConfig("gold", "", "warehouse", str(tmp_path), 300, "EUR").validate()


def test_legacy_gold_without_currency_cannot_be_mislabeled(tmp_path):
    pq.write_table(
        pa.Table.from_pylist([{"daily_revenue": 12.0}]), tmp_path / "part.parquet"
    )
    with pytest.raises(ValueError, match="currency column is missing"):
        load_gold_daily_revenue(str(tmp_path))


def test_gold_store_filter_never_queries_global_products_or_reports_bogus_share(
    tmp_path,
):
    root = tmp_path / "gold"
    for dataset, rows in (
        (
            "daily_revenue_by_store",
            [
                dict(store_id=store, daily_revenue=revenue, transaction_count=1)
                for store, revenue in (("s1", 10.0), ("s2", 100.0))
            ],
        ),
        (
            "top_10_products_by_day",
            [
                dict(
                    product_id="global-product",
                    daily_revenue=100.0,
                    transaction_count=1,
                )
            ],
        ),
    ):
        path = root / dataset / "event_date=2025-01-01"
        path.mkdir(parents=True)
        pq.write_table(
            pa.Table.from_pylist([{**row, "currency": "CAD"} for row in rows]),
            path / "part.parquet",
        )
    repository = GoldLayerRepository(str(root))
    connection = repository._connection
    all_stores = DashboardFilters(date(2025, 1, 1), date(2025, 1, 1))
    selected = DashboardFilters(all_stores.start_date, all_stores.end_date, ("s1",))
    try:
        total = repository.kpi_summary(selected).total_revenue
        global_products = repository.top_products(all_stores)
        assert total == 10.0 and global_products.iloc[0].revenue == 100.0
        # A selected-store query must never touch the store-less product file.
        repository._connection = Mock()
        assert repository.top_products(selected).empty
        repository._connection.execute.assert_not_called()
        # Even a stale/global candidate frame cannot produce the old 1000% insight.
        insights = build_highlight_insights(
            total_revenue=total,
            total_orders=1,
            top_stores=pd.DataFrame(),
            top_products=global_products,
            revenue_trend=pd.DataFrame(),
            product_ranking_complete=False,
        )
        assert insights and all("global-product" not in text for text in insights)
        assert all("1000.0%" not in text for text in insights)
    finally:
        connection.close()


def test_warehouse_product_query_retains_store_filter_and_complete_insight(monkeypatch):
    # Check the SQL/parameter boundary without claiming a local PostgreSQL run.
    repository = object.__new__(PostgresWarehouseRepository)
    repository.schema, repository.currency, repository._engine = (
        "warehouse",
        "CAD",
        None,
    )
    rows = pd.DataFrame(
        [dict(product_id="store-product", product_name="Store Product", revenue=10.0)]
    )
    query = Mock(return_value=rows)
    monkeypatch.setattr(pd, "read_sql_query", query)
    filters = DashboardFilters(date(2025, 1, 1), date(2025, 1, 1), ("s1",))
    products = repository.top_products(filters)
    assert "ds.store_id = ANY(%(store_ids)s)" in query.call_args.args[0]
    assert query.call_args.kwargs["params"]["store_ids"] == ["s1"]
    insights = build_highlight_insights(
        total_revenue=10.0,
        total_orders=1,
        top_stores=pd.DataFrame(),
        top_products=products,
        revenue_trend=pd.DataFrame(),
    )
    assert any("Store Product" in text and "100.0%" in text for text in insights)
