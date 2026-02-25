{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with source_data as (
    select
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        source_updated_at
    from {{ ref('stg_products') }}
    {% if is_incremental() %}
    where source_updated_at >= (
        select coalesce(max(updated_at), '1900-01-01'::timestamptz)
        from {{ this }}
    )
    {% endif %}
)
select
    {{ surrogate_bigint("coalesce(product_id, '')") }} as product_sk,
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    current_timestamp as updated_at
from source_data
