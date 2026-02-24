{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with sales as (
    select
        transaction_id,
        ts_utc,
        store_id,
        customer_id,
        product_id,
        quantity,
        revenue,
        currency,
        unit_price,
        promo_id,
        channel,
        ingestion_date,
        event_date,
        source_updated_at
    from {{ ref('stg_sales') }}
    {% if is_incremental() %}
    where source_updated_at >= (
        select coalesce(max(updated_at), '1900-01-01'::timestamptz)
        from {{ this }}
    )
    {% endif %}
),
resolved as (
    select
        {{ surrogate_bigint("coalesce(s.transaction_id, '')") }} as sales_sk,
        cast(s.transaction_id as uuid) as transaction_id,
        dt.time_sk,
        dc.customer_sk,
        dp.product_sk,
        ds.store_sk,
        s.quantity,
        s.revenue,
        s.currency,
        s.unit_price,
        s.promo_id,
        s.channel,
        s.ingestion_date
    from sales s
    inner join {{ ref('dim_time') }} dt
        on dt.full_date = s.event_date
    inner join {{ ref('dim_customer') }} dc
        on dc.customer_id = s.customer_id
    inner join {{ ref('dim_product') }} dp
        on dp.product_id = s.product_id
    inner join {{ ref('dim_store') }} ds
        on ds.store_id = s.store_id
)
select
    sales_sk,
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
    current_timestamp as updated_at
from resolved

