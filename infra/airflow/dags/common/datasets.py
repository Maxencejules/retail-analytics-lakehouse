"""Shared Airflow Dataset definitions for dataset-aware scheduling."""

from __future__ import annotations

import os

from airflow import Dataset


RAW_TRANSACTIONS_DATASET = Dataset(
    os.getenv(
        "AIRFLOW_DATASET_RAW_TRANSACTIONS",
        "s3://retail-loyalty-lakehouse/dev/bronze/transactions",
    )
)

GOLD_DAILY_REVENUE_DATASET = Dataset(
    os.getenv(
        "AIRFLOW_DATASET_GOLD_DAILY_REVENUE",
        "s3://retail-loyalty-lakehouse/dev/gold/daily_revenue_by_store",
    )
)

GOLD_TOP_PRODUCTS_DATASET = Dataset(
    os.getenv(
        "AIRFLOW_DATASET_GOLD_TOP_PRODUCTS",
        "s3://retail-loyalty-lakehouse/dev/gold/top_10_products_by_day",
    )
)

GOLD_CUSTOMER_LTV_DATASET = Dataset(
    os.getenv(
        "AIRFLOW_DATASET_GOLD_CUSTOMER_LTV",
        "s3://retail-loyalty-lakehouse/dev/gold/customer_lifetime_value",
    )
)

PHASE3_COMPACTION_DATASET = Dataset(
    os.getenv(
        "AIRFLOW_DATASET_PHASE3_COMPACTION",
        "s3://retail-loyalty-lakehouse/dev/ops/compaction",
    )
)

ML_SALES_MODEL_DATASET = Dataset(
    os.getenv(
        "AIRFLOW_DATASET_ML_SALES_MODEL",
        "s3://retail-loyalty-lakehouse/dev/ml/sales-revenue-model",
    )
)
