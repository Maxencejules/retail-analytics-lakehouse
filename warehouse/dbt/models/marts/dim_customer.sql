{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with source_data as (
    select
        customer_id,
        customer_name,
        customer_segment,
        city,
        province,
        country_code,
        source_updated_at
    from {{ ref('stg_customers') }}
    {% if is_incremental() %}
    where source_updated_at >= (
        select coalesce(max(updated_at), '1900-01-01'::timestamptz)
        from {{ this }}
    )
    {% endif %}
)
select
    {{ surrogate_bigint("coalesce(customer_id, '')") }} as customer_sk,
    customer_id,
    customer_name,
    customer_segment,
    city,
    province,
    country_code,
    current_timestamp as updated_at
from source_data
