"""Data access abstraction for dashboard backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from dashboard.config import DashboardConfig


@dataclass(frozen=True, slots=True)
class DashboardFilters:
    """Filters applied to dashboard queries."""

    start_date: date
    end_date: date
    store_ids: tuple[str, ...] = ()

    def validate(self) -> None:
        if self.start_date > self.end_date:
            raise ValueError("start_date must be <= end_date")


@dataclass(frozen=True, slots=True)
class KpiSummary:
    """Summary KPI values for executive cards."""

    total_revenue: float
    average_order_value: float
    total_orders: int


class DashboardRepository(ABC):
    """Abstract repository with backend-specific query implementations."""

    @abstractmethod
    def available_date_range(self) -> tuple[date, date]:
        """Return min and max dates available in the backend."""

    @abstractmethod
    def list_stores(self, filters: DashboardFilters | None = None) -> list[str]:
        """Return available store IDs for filter selection."""

    @abstractmethod
    def kpi_summary(self, filters: DashboardFilters) -> KpiSummary:
        """Return KPI summary values."""

    @abstractmethod
    def top_stores(self, filters: DashboardFilters, *, limit: int = 5) -> pd.DataFrame:
        """Return top stores by revenue."""

    @abstractmethod
    def top_products(self, filters: DashboardFilters, *, limit: int = 5) -> pd.DataFrame:
        """Return top products by revenue."""

    @abstractmethod
    def revenue_trend(self, filters: DashboardFilters) -> pd.DataFrame:
        """Return revenue trend aggregated by date."""


class PostgresWarehouseRepository(DashboardRepository):
    """Warehouse-backed implementation using SQL aggregation queries."""

    def __init__(self, dsn: str, schema: str = "warehouse") -> None:
        try:
            from sqlalchemy import create_engine
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "sqlalchemy is required for warehouse mode. Install with: pip install sqlalchemy psycopg[binary]"
            ) from exc

        self.schema = schema
        self._create_engine = create_engine
        self._engine = self._create_engine(dsn, pool_pre_ping=True, future=True)

    def available_date_range(self) -> tuple[date, date]:
        query = f"""
            SELECT
                MIN(dt.full_date) AS min_date,
                MAX(dt.full_date) AS max_date
            FROM {self.schema}.fact_sales fs
            INNER JOIN {self.schema}.dim_time dt ON dt.time_sk = fs.time_sk
        """
        frame = pd.read_sql_query(query, self._engine)
        if frame.empty or frame.loc[0, "min_date"] is None or frame.loc[0, "max_date"] is None:
            raise ValueError("No data found in warehouse fact_sales")

        return frame.loc[0, "min_date"], frame.loc[0, "max_date"]

    def list_stores(self, filters: DashboardFilters | None = None) -> list[str]:
        where_clause, params = self._time_filter_sql(filters)
        query = f"""
            SELECT DISTINCT ds.store_id
            FROM {self.schema}.fact_sales fs
            INNER JOIN {self.schema}.dim_store ds ON ds.store_sk = fs.store_sk
            INNER JOIN {self.schema}.dim_time dt ON dt.time_sk = fs.time_sk
            {where_clause}
            ORDER BY ds.store_id
        """
        frame = pd.read_sql_query(query, self._engine, params=params)
        return [str(value) for value in frame["store_id"].tolist()]

    def kpi_summary(self, filters: DashboardFilters) -> KpiSummary:
        where_clause, params = self._fact_filter_sql(filters)
        query = f"""
            SELECT
                COALESCE(SUM(fs.revenue), 0)::float AS total_revenue,
                COALESCE(SUM(fs.revenue) / NULLIF(COUNT(*), 0), 0)::float AS average_order_value,
                COUNT(*)::bigint AS total_orders
            FROM {self.schema}.fact_sales fs
            INNER JOIN {self.schema}.dim_time dt ON dt.time_sk = fs.time_sk
            INNER JOIN {self.schema}.dim_store ds ON ds.store_sk = fs.store_sk
            {where_clause}
        """
        frame = pd.read_sql_query(query, self._engine, params=params)
        row = frame.iloc[0]
        return KpiSummary(
            total_revenue=float(row["total_revenue"]),
            average_order_value=float(row["average_order_value"]),
            total_orders=int(row["total_orders"]),
        )

    def top_stores(self, filters: DashboardFilters, *, limit: int = 5) -> pd.DataFrame:
        where_clause, params = self._fact_filter_sql(filters)
        params["limit"] = limit
        query = f"""
            SELECT
                ds.store_id,
                COALESCE(ds.store_name, ds.store_id) AS store_name,
                SUM(fs.revenue)::float AS revenue,
                COUNT(*)::bigint AS orders
            FROM {self.schema}.fact_sales fs
            INNER JOIN {self.schema}.dim_store ds ON ds.store_sk = fs.store_sk
            INNER JOIN {self.schema}.dim_time dt ON dt.time_sk = fs.time_sk
            {where_clause}
            GROUP BY ds.store_id, ds.store_name
            ORDER BY revenue DESC
            LIMIT %(limit)s
        """
        return pd.read_sql_query(query, self._engine, params=params)

    def top_products(self, filters: DashboardFilters, *, limit: int = 5) -> pd.DataFrame:
        where_clause, params = self._fact_filter_sql(filters)
        params["limit"] = limit
        query = f"""
            SELECT
                dp.product_id,
                COALESCE(dp.product_name, dp.product_id) AS product_name,
                SUM(fs.revenue)::float AS revenue,
                COUNT(*)::bigint AS orders
            FROM {self.schema}.fact_sales fs
            INNER JOIN {self.schema}.dim_product dp ON dp.product_sk = fs.product_sk
            INNER JOIN {self.schema}.dim_time dt ON dt.time_sk = fs.time_sk
            INNER JOIN {self.schema}.dim_store ds ON ds.store_sk = fs.store_sk
            {where_clause}
            GROUP BY dp.product_id, dp.product_name
            ORDER BY revenue DESC
            LIMIT %(limit)s
        """
        return pd.read_sql_query(query, self._engine, params=params)

    def revenue_trend(self, filters: DashboardFilters) -> pd.DataFrame:
        where_clause, params = self._fact_filter_sql(filters)
        query = f"""
            SELECT
                dt.full_date AS report_date,
                SUM(fs.revenue)::float AS revenue
            FROM {self.schema}.fact_sales fs
            INNER JOIN {self.schema}.dim_time dt ON dt.time_sk = fs.time_sk
            INNER JOIN {self.schema}.dim_store ds ON ds.store_sk = fs.store_sk
            {where_clause}
            GROUP BY dt.full_date
            ORDER BY dt.full_date
        """
        frame = pd.read_sql_query(query, self._engine, params=params)
        frame["report_date"] = pd.to_datetime(frame["report_date"])
        return frame

    @staticmethod
    def _time_filter_sql(filters: DashboardFilters | None) -> tuple[str, dict[str, Any]]:
        if filters is None:
            return "", {}
        filters.validate()
        return (
            "WHERE dt.full_date BETWEEN %(start_date)s AND %(end_date)s",
            {"start_date": filters.start_date, "end_date": filters.end_date},
        )

    @staticmethod
    def _fact_filter_sql(filters: DashboardFilters) -> tuple[str, dict[str, Any]]:
        filters.validate()
        where_clauses = [
            "dt.full_date BETWEEN %(start_date)s AND %(end_date)s",
        ]
        params: dict[str, Any] = {
            "start_date": filters.start_date,
            "end_date": filters.end_date,
        }

        if filters.store_ids:
            where_clauses.append("ds.store_id = ANY(%(store_ids)s)")
            params["store_ids"] = list(filters.store_ids)

        where_sql = "WHERE " + " AND ".join(where_clauses)
        return where_sql, params


class GoldLayerRepository(DashboardRepository):
    """
    Gold-backed implementation using DuckDB over Parquet datasets.

    This mode uses aggregated Gold outputs and is optimized for local analytics.
    """

    def __init__(self, gold_base_path: str) -> None:
        try:
            import duckdb
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("duckdb is required for gold mode. Install with: pip install duckdb") from exc

        self._duckdb = duckdb
        self.gold_base_path = gold_base_path.rstrip("/")
        self._connection = self._duckdb.connect(database=":memory:")

        self.daily_store_path = self._glob_path("daily_revenue_by_store")
        self.top_products_path = self._glob_path("top_10_products_by_day")

    def _glob_path(self, dataset: str) -> str:
        base = Path(self.gold_base_path) / dataset
        return str((base / "**/*.parquet").as_posix())

    def available_date_range(self) -> tuple[date, date]:
        query = f"""
            SELECT
                MIN(event_date) AS min_date,
                MAX(event_date) AS max_date
            FROM read_parquet('{self.daily_store_path}')
        """
        frame = self._connection.execute(query).fetchdf()
        if frame.empty or pd.isna(frame.loc[0, "min_date"]) or pd.isna(frame.loc[0, "max_date"]):
            raise ValueError("No data found in Gold layer daily_revenue_by_store")
        return frame.loc[0, "min_date"], frame.loc[0, "max_date"]

    def list_stores(self, filters: DashboardFilters | None = None) -> list[str]:
        conditions = []
        if filters is not None:
            filters.validate()
            conditions.append(
                f"event_date BETWEEN DATE '{filters.start_date}' AND DATE '{filters.end_date}'"
            )
        where_sql = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        query = f"""
            SELECT DISTINCT store_id
            FROM read_parquet('{self.daily_store_path}')
            {where_sql}
            ORDER BY store_id
        """
        frame = self._connection.execute(query).fetchdf()
        return [str(value) for value in frame["store_id"].tolist()]

    def kpi_summary(self, filters: DashboardFilters) -> KpiSummary:
        filters.validate()
        store_filter = self._store_filter_sql(filters.store_ids)
        query = f"""
            SELECT
                COALESCE(SUM(daily_revenue), 0)::DOUBLE AS total_revenue,
                COALESCE(SUM(daily_revenue) / NULLIF(SUM(transaction_count), 0), 0)::DOUBLE AS average_order_value,
                COALESCE(SUM(transaction_count), 0)::BIGINT AS total_orders
            FROM read_parquet('{self.daily_store_path}')
            WHERE event_date BETWEEN DATE '{filters.start_date}' AND DATE '{filters.end_date}'
            {store_filter}
        """
        row = self._connection.execute(query).fetchdf().iloc[0]
        return KpiSummary(
            total_revenue=float(row["total_revenue"]),
            average_order_value=float(row["average_order_value"]),
            total_orders=int(row["total_orders"]),
        )

    def top_stores(self, filters: DashboardFilters, *, limit: int = 5) -> pd.DataFrame:
        filters.validate()
        store_filter = self._store_filter_sql(filters.store_ids)
        query = f"""
            SELECT
                store_id,
                store_id AS store_name,
                SUM(daily_revenue)::DOUBLE AS revenue,
                SUM(transaction_count)::BIGINT AS orders
            FROM read_parquet('{self.daily_store_path}')
            WHERE event_date BETWEEN DATE '{filters.start_date}' AND DATE '{filters.end_date}'
            {store_filter}
            GROUP BY store_id
            ORDER BY revenue DESC
            LIMIT {int(limit)}
        """
        return self._connection.execute(query).fetchdf()

    def top_products(self, filters: DashboardFilters, *, limit: int = 5) -> pd.DataFrame:
        filters.validate()
        query = f"""
            SELECT
                product_id,
                product_id AS product_name,
                SUM(daily_revenue)::DOUBLE AS revenue,
                SUM(transaction_count)::BIGINT AS orders
            FROM read_parquet('{self.top_products_path}')
            WHERE event_date BETWEEN DATE '{filters.start_date}' AND DATE '{filters.end_date}'
            GROUP BY product_id
            ORDER BY revenue DESC
            LIMIT {int(limit)}
        """
        return self._connection.execute(query).fetchdf()

    def revenue_trend(self, filters: DashboardFilters) -> pd.DataFrame:
        filters.validate()
        store_filter = self._store_filter_sql(filters.store_ids)
        query = f"""
            SELECT
                event_date AS report_date,
                SUM(daily_revenue)::DOUBLE AS revenue
            FROM read_parquet('{self.daily_store_path}')
            WHERE event_date BETWEEN DATE '{filters.start_date}' AND DATE '{filters.end_date}'
            {store_filter}
            GROUP BY event_date
            ORDER BY event_date
        """
        frame = self._connection.execute(query).fetchdf()
        frame["report_date"] = pd.to_datetime(frame["report_date"])
        return frame

    @staticmethod
    def _store_filter_sql(store_ids: tuple[str, ...]) -> str:
        if not store_ids:
            return ""
        safe_values = [store_id.replace("'", "''") for store_id in store_ids]
        escaped = ", ".join(f"'{value}'" for value in safe_values)
        return f"AND store_id IN ({escaped})"


def create_repository(config: DashboardConfig) -> DashboardRepository:
    """Factory for backend repository selection."""
    config.validate()
    if config.data_source == "warehouse":
        return PostgresWarehouseRepository(
            dsn=config.warehouse_dsn,
            schema=config.warehouse_schema,
        )
    return GoldLayerRepository(gold_base_path=config.gold_base_path)
