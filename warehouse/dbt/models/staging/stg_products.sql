with ranked as (
    select
        trim(product_id) as product_id,
        nullif(trim(product_name), '') as product_name,
        nullif(trim(category), '') as category,
        nullif(trim(subcategory), '') as subcategory,
        nullif(trim(brand), '') as brand,
        cast(source_updated_at as timestamptz) as source_updated_at,
        row_number() over (
            partition by product_id
            order by source_updated_at desc
        ) as row_num
    from {{ source('staging', 'product_incremental') }}
)
select
    product_id,
    coalesce(product_name, 'Unknown') as product_name,
    coalesce(category, 'Unknown') as category,
    coalesce(subcategory, 'Unknown') as subcategory,
    brand,
    source_updated_at
from ranked
where row_num = 1 and product_id is not null and product_id <> ''

