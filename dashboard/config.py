"""Configuration for the executive dashboard."""

from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True, slots=True)
class DashboardConfig:
    """Environment-driven runtime settings."""

    data_source: str
    warehouse_dsn: str
    warehouse_schema: str
    gold_base_path: str
    cache_ttl_seconds: int

    @classmethod
    def from_env(cls) -> "DashboardConfig":
        return cls(
            data_source=os.getenv("DASHBOARD_DATA_SOURCE", "warehouse").strip().lower(),
            warehouse_dsn=os.getenv(
                "WAREHOUSE_DSN",
                "postgresql+psycopg://postgres:postgres@localhost:5432/retail_analytics",
            ).strip(),
            warehouse_schema=os.getenv("WAREHOUSE_SCHEMA", "warehouse").strip(),
            gold_base_path=os.getenv("GOLD_BASE_PATH", "data/lakehouse/gold").strip(),
            cache_ttl_seconds=int(os.getenv("DASHBOARD_CACHE_TTL_SECONDS", "300")),
        )

    def validate(self) -> None:
        if self.data_source not in {"warehouse", "gold"}:
            raise ValueError("DASHBOARD_DATA_SOURCE must be 'warehouse' or 'gold'")
        if self.cache_ttl_seconds <= 0:
            raise ValueError("DASHBOARD_CACHE_TTL_SECONDS must be > 0")
        if self.data_source == "warehouse" and not self.warehouse_dsn:
            raise ValueError("WAREHOUSE_DSN is required when DASHBOARD_DATA_SOURCE=warehouse")
        if self.data_source == "gold" and not self.gold_base_path:
            raise ValueError("GOLD_BASE_PATH is required when DASHBOARD_DATA_SOURCE=gold")

