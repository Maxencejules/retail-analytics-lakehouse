-- Star schema DDL for PostgreSQL
-- Model: FactSales + DimCustomer + DimProduct + DimStore + DimTime

BEGIN;

CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS staging;

-- ==========================================
-- Dimensions
-- ==========================================

CREATE TABLE IF NOT EXISTS warehouse.dim_customer (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id TEXT NOT NULL,
    customer_name TEXT,
    customer_segment TEXT,
    city TEXT,
    province TEXT,
    country_code CHAR(2) NOT NULL DEFAULT 'CA',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_customer_customer_id UNIQUE (customer_id)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_product (
    product_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id TEXT NOT NULL,
    product_name TEXT,
    category TEXT,
    subcategory TEXT,
    brand TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_product_product_id UNIQUE (product_id)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_store (
    store_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    store_id TEXT NOT NULL,
    store_name TEXT,
    region TEXT,
    province TEXT,
    city TEXT,
    store_format TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_store_store_id UNIQUE (store_id)
);

CREATE TABLE IF NOT EXISTS warehouse.dim_time (
    time_sk INTEGER PRIMARY KEY, -- deterministic key: YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_of_week SMALLINT NOT NULL, -- ISO day number (1=Mon, 7=Sun)
    day_name TEXT NOT NULL,
    day_of_month SMALLINT NOT NULL,
    week_of_year SMALLINT NOT NULL,
    month_num SMALLINT NOT NULL,
    month_name TEXT NOT NULL,
    quarter_num SMALLINT NOT NULL,
    year_num INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

-- ==========================================
-- Fact
-- ==========================================

CREATE TABLE IF NOT EXISTS warehouse.fact_sales (
    sales_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- surrogate PK
    transaction_id UUID NOT NULL, -- business key at fact grain
    time_sk INTEGER NOT NULL REFERENCES warehouse.dim_time (time_sk),
    customer_sk BIGINT NOT NULL REFERENCES warehouse.dim_customer (customer_sk),
    product_sk BIGINT NOT NULL REFERENCES warehouse.dim_product (product_sk),
    store_sk BIGINT NOT NULL REFERENCES warehouse.dim_store (store_sk),
    quantity INTEGER NOT NULL,
    revenue NUMERIC(14, 2) NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'CAD',
    unit_price NUMERIC(14, 2) NOT NULL,
    promo_id TEXT NULL,
    channel TEXT NOT NULL,
    ingestion_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_fact_sales_transaction_id UNIQUE (transaction_id),
    CONSTRAINT chk_fact_sales_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_fact_sales_revenue_non_negative CHECK (revenue >= 0),
    CONSTRAINT chk_fact_sales_unit_price_non_negative CHECK (unit_price >= 0),
    CONSTRAINT chk_fact_sales_channel CHECK (channel IN ('online', 'store'))
);

COMMENT ON TABLE warehouse.fact_sales IS
    'Grain: one row per transaction_id. Measures: quantity and revenue.';

-- Optional compatibility views matching requested business names.
CREATE OR REPLACE VIEW warehouse."FactSales" AS
SELECT * FROM warehouse.fact_sales;

CREATE OR REPLACE VIEW warehouse."DimCustomer" AS
SELECT * FROM warehouse.dim_customer;

CREATE OR REPLACE VIEW warehouse."DimProduct" AS
SELECT * FROM warehouse.dim_product;

CREATE OR REPLACE VIEW warehouse."DimStore" AS
SELECT * FROM warehouse.dim_store;

CREATE OR REPLACE VIEW warehouse."DimTime" AS
SELECT * FROM warehouse.dim_time;

-- ==========================================
-- Staging tables for incremental loads
-- ==========================================

CREATE TABLE IF NOT EXISTS staging.sales_incremental (
    transaction_id TEXT NOT NULL,
    ts_utc TIMESTAMPTZ NOT NULL,
    store_id TEXT NOT NULL,
    customer_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(14, 2) NOT NULL,
    currency TEXT NOT NULL,
    payment_method TEXT,
    channel TEXT NOT NULL,
    promo_id TEXT NULL,
    ingestion_date DATE NOT NULL,
    source_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.customer_incremental (
    customer_id TEXT NOT NULL,
    customer_name TEXT,
    customer_segment TEXT,
    city TEXT,
    province TEXT,
    country_code CHAR(2),
    source_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.product_incremental (
    product_id TEXT NOT NULL,
    product_name TEXT,
    category TEXT,
    subcategory TEXT,
    brand TEXT,
    source_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.store_incremental (
    store_id TEXT NOT NULL,
    store_name TEXT,
    region TEXT,
    province TEXT,
    city TEXT,
    store_format TEXT,
    source_updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMIT;
