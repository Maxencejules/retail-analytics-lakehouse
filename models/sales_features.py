"""Feature engineering helpers for Gold-layer sales prediction models."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import numpy as np

FEATURE_NAMES: tuple[str, ...] = (
    "prev_daily_revenue",
    "prev_units_sold",
    "prev_transaction_count",
    "day_of_week",
    "month",
    "days_since_prev",
)


@dataclass(frozen=True, slots=True)
class SalesDailyRecord:
    """One record from Gold daily_revenue_by_store."""

    store_id: str
    event_date: date
    daily_revenue: float
    units_sold: float
    transaction_count: float


def _coerce_date(value: Any) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError(f"Unsupported date value: {value!r}")


def load_gold_daily_revenue(path: str) -> list[SalesDailyRecord]:
    """
    Load Gold daily revenue records from a Parquet dataset path.

    The path can be local or cloud-backed as long as `pyarrow.dataset` can access it.
    """
    try:
        import pyarrow.dataset as ds
    except ImportError as exc:
        raise RuntimeError(
            "pyarrow is required to read Gold-layer parquet data."
        ) from exc

    dataset = ds.dataset(path, format="parquet")
    table = dataset.to_table(
        columns=[
            "store_id",
            "event_date",
            "daily_revenue",
            "units_sold",
            "transaction_count",
        ]
    )
    data = table.to_pydict()
    rows: list[SalesDailyRecord] = []

    row_count = len(data["store_id"])
    for idx in range(row_count):
        store_id = data["store_id"][idx]
        event_value = data["event_date"][idx]
        revenue = data["daily_revenue"][idx]
        units_sold = data["units_sold"][idx]
        transactions = data["transaction_count"][idx]

        if store_id is None or event_value is None:
            continue
        if revenue is None or units_sold is None or transactions is None:
            continue

        rows.append(
            SalesDailyRecord(
                store_id=str(store_id),
                event_date=_coerce_date(event_value),
                daily_revenue=float(revenue),
                units_sold=float(units_sold),
                transaction_count=float(transactions),
            )
        )

    if not rows:
        raise ValueError(f"No usable rows found in Gold dataset at path: {path}")
    return rows


def build_lagged_sales_examples(
    records: list[SalesDailyRecord],
) -> tuple[np.ndarray, np.ndarray, list[str], list[date]]:
    """
    Build supervised examples from sequential store revenue history.

    Features are derived from the previous daily record per store, while the
    target is the current day's revenue.
    """
    grouped: dict[str, list[SalesDailyRecord]] = {}
    for record in records:
        grouped.setdefault(record.store_id, []).append(record)

    features: list[list[float]] = []
    targets: list[float] = []
    store_ids: list[str] = []
    event_dates: list[date] = []

    for store_id, store_records in grouped.items():
        ordered = sorted(store_records, key=lambda value: value.event_date)
        for idx in range(1, len(ordered)):
            previous = ordered[idx - 1]
            current = ordered[idx]
            days_since_prev = (current.event_date - previous.event_date).days
            if days_since_prev <= 0:
                continue

            features.append(
                [
                    previous.daily_revenue,
                    previous.units_sold,
                    previous.transaction_count,
                    float(current.event_date.weekday()),
                    float(current.event_date.month),
                    float(days_since_prev),
                ]
            )
            targets.append(current.daily_revenue)
            store_ids.append(store_id)
            event_dates.append(current.event_date)

    if not features:
        raise ValueError("Insufficient sequential history to build training samples.")

    feature_matrix = np.asarray(features, dtype=np.float32)
    target_vector = np.asarray(targets, dtype=np.float32)
    return feature_matrix, target_vector, store_ids, event_dates
