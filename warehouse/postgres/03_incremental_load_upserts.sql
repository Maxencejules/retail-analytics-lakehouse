-- Incremental load SQL for warehouse star schema (PostgreSQL)
-- Idempotent strategy:
-- 1) Deduplicate staging by transaction_id (latest record wins)
-- 2) Upsert dimensions by natural keys
-- 3) Upsert fact by transaction_id with ON CONFLICT
-- 4) Update fact rows only when payload changed (IS DISTINCT FROM guards)

BEGIN;

-- Prevent concurrent loaders from writing the same incremental batch.
SELECT pg_advisory_xact_lock(43100217);

-- ----------------------------------------------------------
-- Critical data quality checks (fail-fast)
-- ----------------------------------------------------------
DO $$
DECLARE
    invalid_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO invalid_count
    FROM staging.sales_incremental s
    WHERE
        s.transaction_id IS NULL
        OR s.transaction_id !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        OR s.ts_utc IS NULL
        OR COALESCE(BTRIM(s.store_id), '') = ''
        OR COALESCE(BTRIM(s.customer_id), '') = ''
        OR COALESCE(BTRIM(s.product_id), '') = ''
        OR s.quantity <= 0
        OR s.unit_price < 0
        OR (s.quantity * s.unit_price) < 0
        OR LOWER(BTRIM(s.channel)) NOT IN ('online', 'store')
        OR UPPER(BTRIM(s.currency)) NOT IN ('CAD', 'USD', 'C$', 'US$', 'CAD$', 'USD$');

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'Critical data quality violation: % invalid rows in staging.sales_incremental',
            invalid_count;
    END IF;
END $$;

-- ----------------------------------------------------------
-- Stage deduplicated incremental sales in temp table
-- ----------------------------------------------------------
DROP TABLE IF EXISTS tmp_sales_incremental;

CREATE TEMP TABLE tmp_sales_incremental ON COMMIT DROP AS
SELECT
    ranked.transaction_id::UUID AS transaction_id,
    ranked.ts_utc AS ts_utc,
    BTRIM(ranked.store_id) AS store_id,
    BTRIM(ranked.customer_id) AS customer_id,
    BTRIM(ranked.product_id) AS product_id,
    ranked.quantity::INTEGER AS quantity,
    ranked.unit_price::NUMERIC(14, 2) AS unit_price,
    CASE
        WHEN UPPER(BTRIM(ranked.currency)) IN ('CAD', 'C$', 'CAD$') THEN 'CAD'
        WHEN UPPER(BTRIM(ranked.currency)) IN ('USD', 'US$', 'USD$') THEN 'USD'
        ELSE UPPER(BTRIM(ranked.currency))
    END AS currency,
    NULLIF(BTRIM(ranked.promo_id), '') AS promo_id,
    LOWER(BTRIM(ranked.channel)) AS channel,
    ranked.ingestion_date::DATE AS ingestion_date
FROM (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.transaction_id
            ORDER BY s.ts_utc DESC, s.ingestion_date DESC, s.source_updated_at DESC
        ) AS rn
    FROM staging.sales_incremental s
) ranked
WHERE ranked.rn = 1;

-- ----------------------------------------------------------
-- Upsert DimCustomer (type-1 semantics)
-- ----------------------------------------------------------
WITH latest_customer_attrs AS (
    SELECT DISTINCT ON (c.customer_id)
        c.customer_id,
        c.customer_name,
        c.customer_segment,
        c.city,
        c.province,
        c.country_code
    FROM staging.customer_incremental c
    ORDER BY c.customer_id, c.source_updated_at DESC
),
customer_keys AS (
    SELECT DISTINCT s.customer_id
    FROM tmp_sales_incremental s
)
INSERT INTO warehouse.dim_customer (
    customer_id,
    customer_name,
    customer_segment,
    city,
    province,
    country_code,
    updated_at
)
SELECT
    k.customer_id,
    COALESCE(a.customer_name, 'Unknown'),
    COALESCE(a.customer_segment, 'Unclassified'),
    a.city,
    a.province,
    COALESCE(a.country_code, 'CA'),
    NOW()
FROM customer_keys k
LEFT JOIN latest_customer_attrs a
    ON a.customer_id = k.customer_id
ON CONFLICT (customer_id) DO UPDATE
SET
    customer_name = EXCLUDED.customer_name,
    customer_segment = EXCLUDED.customer_segment,
    city = EXCLUDED.city,
    province = EXCLUDED.province,
    country_code = EXCLUDED.country_code,
    updated_at = NOW();

-- ----------------------------------------------------------
-- Upsert DimProduct (type-1 semantics)
-- ----------------------------------------------------------
WITH latest_product_attrs AS (
    SELECT DISTINCT ON (p.product_id)
        p.product_id,
        p.product_name,
        p.category,
        p.subcategory,
        p.brand
    FROM staging.product_incremental p
    ORDER BY p.product_id, p.source_updated_at DESC
),
product_keys AS (
    SELECT DISTINCT s.product_id
    FROM tmp_sales_incremental s
)
INSERT INTO warehouse.dim_product (
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    updated_at
)
SELECT
    k.product_id,
    COALESCE(a.product_name, 'Unknown'),
    COALESCE(a.category, 'Unknown'),
    COALESCE(a.subcategory, 'Unknown'),
    a.brand,
    NOW()
FROM product_keys k
LEFT JOIN latest_product_attrs a
    ON a.product_id = k.product_id
ON CONFLICT (product_id) DO UPDATE
SET
    product_name = EXCLUDED.product_name,
    category = EXCLUDED.category,
    subcategory = EXCLUDED.subcategory,
    brand = EXCLUDED.brand,
    updated_at = NOW();

-- ----------------------------------------------------------
-- Upsert DimStore (type-1 semantics)
-- ----------------------------------------------------------
WITH latest_store_attrs AS (
    SELECT DISTINCT ON (st.store_id)
        st.store_id,
        st.store_name,
        st.region,
        st.province,
        st.city,
        st.store_format
    FROM staging.store_incremental st
    ORDER BY st.store_id, st.source_updated_at DESC
),
store_keys AS (
    SELECT DISTINCT s.store_id
    FROM tmp_sales_incremental s
)
INSERT INTO warehouse.dim_store (
    store_id,
    store_name,
    region,
    province,
    city,
    store_format,
    updated_at
)
SELECT
    k.store_id,
    COALESCE(a.store_name, k.store_id),
    COALESCE(a.region, 'Unknown'),
    a.province,
    a.city,
    a.store_format,
    NOW()
FROM store_keys k
LEFT JOIN latest_store_attrs a
    ON a.store_id = k.store_id
ON CONFLICT (store_id) DO UPDATE
SET
    store_name = EXCLUDED.store_name,
    region = EXCLUDED.region,
    province = EXCLUDED.province,
    city = EXCLUDED.city,
    store_format = EXCLUDED.store_format,
    updated_at = NOW();

-- ----------------------------------------------------------
-- Upsert DimTime
-- ----------------------------------------------------------
INSERT INTO warehouse.dim_time (
    time_sk,
    full_date,
    day_of_week,
    day_name,
    day_of_month,
    week_of_year,
    month_num,
    month_name,
    quarter_num,
    year_num,
    is_weekend
)
SELECT DISTINCT
    TO_CHAR((s.ts_utc AT TIME ZONE 'UTC')::DATE, 'YYYYMMDD')::INTEGER AS time_sk,
    (s.ts_utc AT TIME ZONE 'UTC')::DATE AS full_date,
    EXTRACT(ISODOW FROM s.ts_utc AT TIME ZONE 'UTC')::SMALLINT AS day_of_week,
    BTRIM(TO_CHAR(s.ts_utc AT TIME ZONE 'UTC', 'Day')) AS day_name,
    EXTRACT(DAY FROM s.ts_utc AT TIME ZONE 'UTC')::SMALLINT AS day_of_month,
    EXTRACT(WEEK FROM s.ts_utc AT TIME ZONE 'UTC')::SMALLINT AS week_of_year,
    EXTRACT(MONTH FROM s.ts_utc AT TIME ZONE 'UTC')::SMALLINT AS month_num,
    BTRIM(TO_CHAR(s.ts_utc AT TIME ZONE 'UTC', 'Month')) AS month_name,
    EXTRACT(QUARTER FROM s.ts_utc AT TIME ZONE 'UTC')::SMALLINT AS quarter_num,
    EXTRACT(YEAR FROM s.ts_utc AT TIME ZONE 'UTC')::INTEGER AS year_num,
    (EXTRACT(ISODOW FROM s.ts_utc AT TIME ZONE 'UTC') IN (6, 7)) AS is_weekend
FROM tmp_sales_incremental s
ON CONFLICT (time_sk) DO NOTHING;

-- ----------------------------------------------------------
-- Upsert FactSales (idempotent)
-- ----------------------------------------------------------
WITH resolved_fact AS (
    SELECT
        s.transaction_id,
        dt.time_sk,
        dc.customer_sk,
        dp.product_sk,
        ds.store_sk,
        s.quantity,
        ROUND((s.quantity * s.unit_price)::NUMERIC, 2)::NUMERIC(14, 2) AS revenue,
        s.currency::CHAR(3) AS currency,
        s.unit_price::NUMERIC(14, 2) AS unit_price,
        s.promo_id,
        s.channel,
        s.ingestion_date
    FROM tmp_sales_incremental s
    INNER JOIN warehouse.dim_time dt
        ON dt.full_date = (s.ts_utc AT TIME ZONE 'UTC')::DATE
    INNER JOIN warehouse.dim_customer dc
        ON dc.customer_id = s.customer_id
    INNER JOIN warehouse.dim_product dp
        ON dp.product_id = s.product_id
    INNER JOIN warehouse.dim_store ds
        ON ds.store_id = s.store_id
)
INSERT INTO warehouse.fact_sales (
    transaction_id,
    time_sk,
    customer_sk,
    product_sk,
    store_sk,
    quantity,
    revenue,
    currency,
    unit_price,
    promo_id,
    channel,
    ingestion_date,
    updated_at
)
SELECT
    r.transaction_id,
    r.time_sk,
    r.customer_sk,
    r.product_sk,
    r.store_sk,
    r.quantity,
    r.revenue,
    r.currency,
    r.unit_price,
    r.promo_id,
    r.channel,
    r.ingestion_date,
    NOW()
FROM resolved_fact r
ON CONFLICT (transaction_id) DO UPDATE
SET
    time_sk = EXCLUDED.time_sk,
    customer_sk = EXCLUDED.customer_sk,
    product_sk = EXCLUDED.product_sk,
    store_sk = EXCLUDED.store_sk,
    quantity = EXCLUDED.quantity,
    revenue = EXCLUDED.revenue,
    currency = EXCLUDED.currency,
    unit_price = EXCLUDED.unit_price,
    promo_id = EXCLUDED.promo_id,
    channel = EXCLUDED.channel,
    ingestion_date = EXCLUDED.ingestion_date,
    updated_at = NOW()
WHERE
    warehouse.fact_sales.time_sk IS DISTINCT FROM EXCLUDED.time_sk
    OR warehouse.fact_sales.customer_sk IS DISTINCT FROM EXCLUDED.customer_sk
    OR warehouse.fact_sales.product_sk IS DISTINCT FROM EXCLUDED.product_sk
    OR warehouse.fact_sales.store_sk IS DISTINCT FROM EXCLUDED.store_sk
    OR warehouse.fact_sales.quantity IS DISTINCT FROM EXCLUDED.quantity
    OR warehouse.fact_sales.revenue IS DISTINCT FROM EXCLUDED.revenue
    OR warehouse.fact_sales.currency IS DISTINCT FROM EXCLUDED.currency
    OR warehouse.fact_sales.unit_price IS DISTINCT FROM EXCLUDED.unit_price
    OR warehouse.fact_sales.promo_id IS DISTINCT FROM EXCLUDED.promo_id
    OR warehouse.fact_sales.channel IS DISTINCT FROM EXCLUDED.channel
    OR warehouse.fact_sales.ingestion_date IS DISTINCT FROM EXCLUDED.ingestion_date;

COMMIT;
