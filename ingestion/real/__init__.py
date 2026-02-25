"""Ingestion adapters for real-world source datasets."""

from ingestion.real.uci_online_retail import (
    ConversionStats,
    convert_uci_csv,
    map_uci_row,
    resolve_column_mapping,
)

__all__ = [
    "ConversionStats",
    "convert_uci_csv",
    "map_uci_row",
    "resolve_column_mapping",
]
