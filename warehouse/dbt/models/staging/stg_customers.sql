with ranked as (
    select
        trim(customer_id) as customer_id,
        nullif(trim(customer_name), '') as customer_name,
        nullif(trim(customer_segment), '') as customer_segment,
        nullif(trim(city), '') as city,
        nullif(trim(province), '') as province,
        coalesce(nullif(trim(country_code), ''), 'CA') as country_code,
        cast(source_updated_at as timestamptz) as source_updated_at,
        row_number() over (
            partition by customer_id
            order by source_updated_at desc
        ) as row_num
    from {{ source('staging', 'customer_incremental') }}
)
select
    customer_id,
    coalesce(customer_name, 'Unknown') as customer_name,
    coalesce(customer_segment, 'Unclassified') as customer_segment,
    city,
    province,
    upper(country_code) as country_code,
    source_updated_at
from ranked
where row_num = 1 and customer_id is not null and customer_id <> ''

