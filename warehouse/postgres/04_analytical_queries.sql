-- Example analytical queries on warehouse star schema

-- ==================================================
-- 1) Revenue by region
-- ==================================================
SELECT
    dt.year_num,
    dt.month_num,
    ds.region,
    SUM(fs.revenue) AS revenue_amount,
    SUM(fs.quantity) AS units_sold,
    COUNT(*) AS transaction_count
FROM warehouse.fact_sales fs
INNER JOIN warehouse.dim_store ds
    ON ds.store_sk = fs.store_sk
INNER JOIN warehouse.dim_time dt
    ON dt.time_sk = fs.time_sk
WHERE dt.full_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY
    dt.year_num,
    dt.month_num,
    ds.region
ORDER BY
    dt.year_num,
    dt.month_num,
    revenue_amount DESC;

-- ==================================================
-- 2) Top products in the last 30 days
-- ==================================================
SELECT
    dp.product_id,
    dp.product_name,
    dp.category,
    SUM(fs.revenue) AS revenue_30d,
    SUM(fs.quantity) AS units_30d,
    COUNT(*) AS transactions_30d
FROM warehouse.fact_sales fs
INNER JOIN warehouse.dim_product dp
    ON dp.product_sk = fs.product_sk
INNER JOIN warehouse.dim_time dt
    ON dt.time_sk = fs.time_sk
WHERE dt.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY
    dp.product_id,
    dp.product_name,
    dp.category
ORDER BY
    revenue_30d DESC,
    units_30d DESC
LIMIT 10;

-- ==================================================
-- 3) Customer segmentation (LTV + recency + frequency)
-- ==================================================
WITH customer_metrics AS (
    SELECT
        fs.customer_sk,
        SUM(fs.revenue) AS lifetime_value,
        COUNT(*) AS transaction_count,
        MAX(dt.full_date) AS last_purchase_date
    FROM warehouse.fact_sales fs
    INNER JOIN warehouse.dim_time dt
        ON dt.time_sk = fs.time_sk
    GROUP BY fs.customer_sk
),
scored AS (
    SELECT
        dc.customer_id,
        dc.customer_segment AS source_segment,
        cm.lifetime_value,
        cm.transaction_count,
        (CURRENT_DATE - cm.last_purchase_date) AS recency_days
    FROM customer_metrics cm
    INNER JOIN warehouse.dim_customer dc
        ON dc.customer_sk = cm.customer_sk
)
SELECT
    CASE
        WHEN s.lifetime_value >= 1000
             AND s.transaction_count >= 10
             AND s.recency_days <= 30 THEN 'Champions'
        WHEN s.lifetime_value >= 500
             AND s.transaction_count >= 5
             AND s.recency_days <= 90 THEN 'Loyal'
        WHEN s.recency_days > 120 THEN 'At Risk'
        ELSE 'Emerging'
    END AS segment_bucket,
    COUNT(*) AS customers,
    ROUND(AVG(s.lifetime_value), 2) AS avg_ltv,
    ROUND(AVG(s.recency_days), 1) AS avg_recency_days,
    ROUND(AVG(s.transaction_count), 1) AS avg_transactions
FROM scored s
GROUP BY segment_bucket
ORDER BY avg_ltv DESC;
