# Warehouse Star Schema (Postgres)

## Overview

This warehouse model implements a classic star schema for retail analytics with:

- One fact table: `warehouse.fact_sales`
- Four dimensions:
  - `warehouse.dim_customer`
  - `warehouse.dim_product`
  - `warehouse.dim_store`
  - `warehouse.dim_time`

## Fact Table Grain

`warehouse.fact_sales` is defined at **one row per transaction** (`transaction_id`).

Design implication:
- `quantity` and `revenue` are stored at transaction grain.
- If source orders contain multiple products, upstream ETL must emit one transaction-level row per product line before loading this fact.

## Modeling Choices

- Surrogate keys are used in all dimensions (`*_sk`) to isolate analytics joins from source-system key volatility.
- `transaction_id` is kept in the fact as the immutable business key and constrained unique for idempotent loads.
- `dim_time` uses deterministic key `YYYYMMDD` for predictable joins and easy partition/date filtering.
- Type-1 dimension behavior is applied for serving dimensions (`dim_customer`, `dim_product`, `dim_store`).
- SCD2 customer history is available through dbt snapshot-driven `dim_customer_scd2`.
- `fact_sales` enforces non-negative measures with constraints:
  - `quantity > 0`
  - `revenue >= 0`
  - `unit_price >= 0`

## Index Strategy Summary

- `fact_sales(time_sk)` for time-filtered scans.
- `fact_sales(store_sk, time_sk) INCLUDE (revenue, quantity)` for revenue-by-region aggregation paths.
- `fact_sales(product_sk, time_sk) INCLUDE (revenue, quantity)` for top-product lookups over recent windows.
- `fact_sales(customer_sk) INCLUDE (revenue, quantity, time_sk)` for customer segmentation/CLV workloads.
- `BRIN(ingestion_date)` on fact table for low-cost range scans at scale.
- Supporting slice indexes on `dim_store(region, province)`, `dim_product(category, subcategory)`, and `dim_customer(customer_segment)`.

## Incremental + Idempotent Upsert Strategy

- Source records land in `staging.*` tables.
- A fail-fast quality gate rejects critical violations before any warehouse write.
- Incremental fact source is deduplicated by `transaction_id` (latest record wins).
- Dimensions are upserted first on natural keys (type-1 updates).
- Fact upsert uses `ON CONFLICT (transaction_id) DO UPDATE`, making re-runs safe and deterministic.
- `IS DISTINCT FROM` predicates avoid unnecessary updates when payload values did not change.

## Deliverables

- DDL: [01_ddl_star_schema.sql](C:/Users/USER/retail-analytics-lakehouse/warehouse/postgres/01_ddl_star_schema.sql)
- Indexes: [02_index_strategy.sql](C:/Users/USER/retail-analytics-lakehouse/warehouse/postgres/02_index_strategy.sql)
- Incremental/idempotent load SQL: [03_incremental_load_upserts.sql](C:/Users/USER/retail-analytics-lakehouse/warehouse/postgres/03_incremental_load_upserts.sql)
- Example analytical queries: [04_analytical_queries.sql](C:/Users/USER/retail-analytics-lakehouse/warehouse/postgres/04_analytical_queries.sql)

## Deployment Order

1. Run DDL file.
2. Run index strategy file.
3. Load data into `staging.*` tables.
4. Run incremental upsert SQL.
5. Run analytical queries for validation and reporting.
