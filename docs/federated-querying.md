# Federated Querying with Trino

## Objective

Use Trino as a federation layer across:

- Warehouse tables in local Postgres (`warehouse` catalog).
- Spark Gold outputs in local/S3 object storage (`lakehouse` catalog).
- External supplier API snapshots landed as Parquet in S3 (same `lakehouse` catalog).

## Local Stack Components

`docker-compose.yml` now includes:

- `warehouse` (`postgres:15`) for local star-schema serving.
- `hive-metastore` (`apache/hive`) for the Trino Hive catalog metadata service.
- `trino` (`trinodb/trino`) with two catalogs:
  - [infra/trino/catalog/warehouse.properties](C:/Users/USER/retail-analytics-lakehouse/infra/trino/catalog/warehouse.properties)
  - [infra/trino/catalog/lakehouse.properties](C:/Users/USER/retail-analytics-lakehouse/infra/trino/catalog/lakehouse.properties)

Start services:

```bash
docker compose up -d warehouse hive-metastore spark trino
```

Open the Trino CLI:

```bash
docker compose exec trino trino
```

Windows note:
- If `HOME` is not defined, set it before startup so the `~/.aws` bind mount resolves correctly (or replace `${HOME}/.aws` in `docker-compose.yml` with an explicit path).

## Register Spark Gold Outputs

Run these statements once from the Trino CLI after Spark has produced `data/lakehouse/gold/*` parquet outputs.

```sql
CREATE SCHEMA IF NOT EXISTS lakehouse.gold
WITH (location = 'local:///lakehouse/gold');

CREATE TABLE IF NOT EXISTS lakehouse.gold.daily_revenue_by_store (
    store_id VARCHAR,
    daily_revenue DOUBLE,
    units_sold BIGINT,
    transaction_count BIGINT,
    event_date DATE
)
WITH (
    external_location = 'local:///lakehouse/gold/daily_revenue_by_store',
    format = 'PARQUET',
    partitioned_by = ARRAY['event_date']
);

CREATE TABLE IF NOT EXISTS lakehouse.gold.top_10_products_by_day (
    product_id VARCHAR,
    "rank" INTEGER,
    daily_revenue DOUBLE,
    units_sold BIGINT,
    transaction_count BIGINT,
    event_date DATE
)
WITH (
    external_location = 'local:///lakehouse/gold/top_10_products_by_day',
    format = 'PARQUET',
    partitioned_by = ARRAY['event_date']
);

CREATE TABLE IF NOT EXISTS lakehouse.gold.customer_lifetime_value (
    customer_id VARCHAR,
    lifetime_value DOUBLE,
    transaction_count BIGINT,
    first_purchase_ts_utc TIMESTAMP,
    last_purchase_ts_utc TIMESTAMP,
    snapshot_date DATE
)
WITH (
    external_location = 'local:///lakehouse/gold/customer_lifetime_value',
    format = 'PARQUET',
    partitioned_by = ARRAY['snapshot_date']
);
```

## Register External Supplier API Snapshots

When supplier API responses are landed to S3 as Parquet snapshots (recommended pattern), register them in Trino:

```sql
CREATE SCHEMA IF NOT EXISTS lakehouse.external
WITH (location = 's3://retail-supplier-api-us-east-1/snapshots');

CREATE TABLE IF NOT EXISTS lakehouse.external.supplier_product_snapshot (
    supplier_id VARCHAR,
    supplier_name VARCHAR,
    product_id VARCHAR,
    list_price DOUBLE,
    available_units BIGINT,
    snapshot_ts TIMESTAMP,
    snapshot_date DATE
)
WITH (
    external_location = 's3://retail-supplier-api-us-east-1/snapshots/supplier_product_snapshot',
    format = 'PARQUET',
    partitioned_by = ARRAY['snapshot_date']
);
```

## Federated Query Examples

Warehouse (Postgres) + Gold (Spark Parquet):

```sql
SELECT
    d.full_date AS event_date,
    ds.region,
    SUM(fs.revenue) AS warehouse_revenue,
    SUM(g.daily_revenue) AS gold_revenue
FROM warehouse.warehouse.fact_sales fs
JOIN warehouse.warehouse.dim_time d
    ON fs.time_sk = d.time_sk
JOIN warehouse.warehouse.dim_store ds
    ON fs.store_sk = ds.store_sk
JOIN lakehouse.gold.daily_revenue_by_store g
    ON g.event_date = d.full_date
   AND g.store_id = ds.store_id
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

Gold (Spark Parquet) + Supplier API snapshots (S3 Parquet):

```sql
SELECT
    t.event_date,
    t.product_id,
    t."rank",
    t.daily_revenue,
    s.supplier_name,
    s.list_price,
    s.available_units
FROM lakehouse.gold.top_10_products_by_day t
JOIN lakehouse.external.supplier_product_snapshot s
    ON s.product_id = t.product_id
   AND s.snapshot_date = t.event_date
ORDER BY t.event_date DESC, t."rank";
```

Warehouse + Gold + Supplier API (three-source federation):

```sql
SELECT
    d.full_date AS event_date,
    dp.product_id,
    SUM(fs.quantity) AS units_sold,
    ROUND(AVG(s.list_price), 2) AS avg_supplier_list_price,
    SUM(fs.revenue) AS warehouse_revenue
FROM warehouse.warehouse.fact_sales fs
JOIN warehouse.warehouse.dim_time d
    ON fs.time_sk = d.time_sk
JOIN warehouse.warehouse.dim_product dp
    ON fs.product_sk = dp.product_sk
JOIN lakehouse.external.supplier_product_snapshot s
    ON s.product_id = dp.product_id
   AND s.snapshot_date = d.full_date
GROUP BY 1, 2
ORDER BY event_date DESC, warehouse_revenue DESC
LIMIT 50;
```

## Multi-Region and Hybrid Patterns

- For additional AWS regions, create an extra Trino catalog file (for example `lakehouse_us_west_2.properties`) with `s3.region=us-west-2`.
- For hybrid/on-prem data, mount local/NFS storage into the Trino container and register it with `local://` external locations in a separate schema.
