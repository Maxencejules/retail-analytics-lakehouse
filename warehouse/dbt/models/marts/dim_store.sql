{{ config(
    materialized='incremental',
    unique_key='store_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with source_data as (
    select
        store_id,
        store_name,
        region,
        province,
        city,
        store_format,
        source_updated_at
    from {{ ref('stg_stores') }}
    {% if is_incremental() %}
    where source_updated_at >= (
        select coalesce(max(updated_at), '1900-01-01'::timestamptz)
        from {{ this }}
    )
    {% endif %}
)
select
    {{ surrogate_bigint("coalesce(store_id, '')") }} as store_sk,
    store_id,
    store_name,
    region,
    province,
    city,
    store_format,
    current_timestamp as updated_at
from source_data

