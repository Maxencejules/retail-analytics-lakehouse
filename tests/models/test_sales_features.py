from __future__ import annotations

from datetime import date

import numpy as np

from models.sales_features import (
    FEATURE_NAMES,
    SalesDailyRecord,
    build_lagged_sales_examples,
)


def test_build_lagged_sales_examples_creates_expected_samples() -> None:
    records = [
        SalesDailyRecord(
            store_id="STORE-001",
            event_date=date(2026, 2, 20),
            daily_revenue=1200.0,
            units_sold=100.0,
            transaction_count=80.0,
        ),
        SalesDailyRecord(
            store_id="STORE-001",
            event_date=date(2026, 2, 21),
            daily_revenue=1300.0,
            units_sold=110.0,
            transaction_count=85.0,
        ),
        SalesDailyRecord(
            store_id="STORE-002",
            event_date=date(2026, 2, 20),
            daily_revenue=900.0,
            units_sold=70.0,
            transaction_count=55.0,
        ),
        SalesDailyRecord(
            store_id="STORE-002",
            event_date=date(2026, 2, 22),
            daily_revenue=980.0,
            units_sold=75.0,
            transaction_count=58.0,
        ),
    ]

    features, targets, store_ids, event_dates = build_lagged_sales_examples(records)

    assert features.shape == (2, len(FEATURE_NAMES))
    assert targets.shape == (2,)
    np.testing.assert_allclose(targets, np.array([1300.0, 980.0], dtype=np.float32))

    assert store_ids == ["STORE-001", "STORE-002"]
    assert event_dates == [date(2026, 2, 21), date(2026, 2, 22)]

    # First sample should use STORE-001 previous-day values.
    np.testing.assert_allclose(
        features[0],
        np.array(
            [
                1200.0,  # prev_daily_revenue
                100.0,  # prev_units_sold
                80.0,  # prev_transaction_count
                5.0,  # day_of_week for 2026-02-21 (Saturday)
                2.0,  # month
                1.0,  # days_since_prev
            ],
            dtype=np.float32,
        ),
    )
