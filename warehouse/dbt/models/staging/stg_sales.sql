with ranked as (
    select
        trim(cast(transaction_id as text)) as transaction_id,
        cast(ts_utc as timestamptz) as ts_utc,
        trim(store_id) as store_id,
        trim(customer_id) as customer_id,
        trim(product_id) as product_id,
        cast(quantity as integer) as quantity,
        cast(unit_price as numeric(14, 2)) as unit_price,
        upper(trim(currency)) as currency_raw,
        lower(trim(payment_method)) as payment_method,
        lower(trim(channel)) as channel,
        nullif(trim(promo_id), '') as promo_id,
        cast(ingestion_date as date) as ingestion_date,
        cast(source_updated_at as timestamptz) as source_updated_at,
        row_number() over (
            partition by transaction_id
            order by ts_utc desc, ingestion_date desc, source_updated_at desc
        ) as row_num
    from {{ source('staging', 'sales_incremental') }}
),
deduped as (
    select *
    from ranked
    where row_num = 1
),
cleaned as (
    select
        transaction_id,
        ts_utc,
        store_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        case
            when currency_raw in ('CAD', 'C$', 'CAD$') then 'CAD'
            when currency_raw in ('USD', 'US$', 'USD$') then 'USD'
            else currency_raw
        end as currency,
        payment_method,
        channel,
        promo_id,
        ingestion_date,
        source_updated_at,
        cast(quantity * unit_price as numeric(14, 2)) as revenue,
        cast(ts_utc at time zone 'UTC' as date) as event_date
    from deduped
)
select
    transaction_id,
    ts_utc,
    store_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    currency,
    payment_method,
    channel,
    promo_id,
    ingestion_date,
    source_updated_at,
    revenue,
    event_date
from cleaned
where
    transaction_id ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
    and quantity > 0
    and unit_price >= 0
    and revenue >= 0
    and channel in ('online', 'store')
    and payment_method in ('credit_card', 'debit_card', 'cash', 'mobile_wallet')
    and currency in ('CAD', 'USD')

