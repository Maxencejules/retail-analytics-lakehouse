-- Index strategy for warehouse star schema (PostgreSQL)

-- ==================================================
-- Fact indexes
-- ==================================================

-- Common filter path for time-bounded analytics and partition pruning analog.
CREATE INDEX IF NOT EXISTS idx_fact_sales_time_sk
    ON warehouse.fact_sales (time_sk);

-- Supports revenue-by-region workloads (store + date rollups).
CREATE INDEX IF NOT EXISTS idx_fact_sales_store_time_sk
    ON warehouse.fact_sales (store_sk, time_sk)
    INCLUDE (revenue, quantity);

-- Supports top-products queries over recent periods.
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_time_sk
    ON warehouse.fact_sales (product_sk, time_sk)
    INCLUDE (revenue, quantity);

-- Supports customer-level aggregation and segmentation.
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_sk
    ON warehouse.fact_sales (customer_sk)
    INCLUDE (revenue, quantity, time_sk);

-- Efficient append-heavy date scans for large tables.
CREATE INDEX IF NOT EXISTS idx_fact_sales_ingestion_date_brin
    ON warehouse.fact_sales
    USING BRIN (ingestion_date);

-- ==================================================
-- Dimension indexes
-- ==================================================

-- Natural-key indexes are provided by UNIQUE constraints in DDL.
-- Additional access-path indexes for common slicing fields:

CREATE INDEX IF NOT EXISTS idx_dim_store_region
    ON warehouse.dim_store (region, province);

CREATE INDEX IF NOT EXISTS idx_dim_product_category
    ON warehouse.dim_product (category, subcategory);

CREATE INDEX IF NOT EXISTS idx_dim_customer_segment
    ON warehouse.dim_customer (customer_segment);

CREATE INDEX IF NOT EXISTS idx_dim_time_full_date
    ON warehouse.dim_time (full_date);

