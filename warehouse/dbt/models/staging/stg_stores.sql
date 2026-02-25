with ranked as (
    select
        trim(store_id) as store_id,
        nullif(trim(store_name), '') as store_name,
        nullif(trim(region), '') as region,
        nullif(trim(province), '') as province,
        nullif(trim(city), '') as city,
        nullif(trim(store_format), '') as store_format,
        cast(source_updated_at as timestamptz) as source_updated_at,
        row_number() over (
            partition by store_id
            order by source_updated_at desc
        ) as row_num
    from {{ source('staging', 'store_incremental') }}
)
select
    store_id,
    coalesce(store_name, store_id) as store_name,
    coalesce(region, 'Unknown') as region,
    province,
    city,
    store_format,
    source_updated_at
from ranked
where row_num = 1 and store_id is not null and store_id <> ''
