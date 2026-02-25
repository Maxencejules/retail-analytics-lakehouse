{{ config(
    materialized='table'
) }}

select
    {{ surrogate_bigint("coalesce(customer_id, '') || ':' || coalesce(cast(dbt_valid_from as text), '')") }} as customer_version_sk,
    {{ surrogate_bigint("coalesce(customer_id, '')") }} as customer_sk,
    customer_id,
    customer_name,
    customer_segment,
    city,
    province,
    country_code,
    cast(dbt_valid_from as timestamptz) as valid_from_utc,
    cast(dbt_valid_to as timestamptz) as valid_to_utc,
    (dbt_valid_to is null) as is_current
from {{ ref('snp_dim_customer_history') }}
