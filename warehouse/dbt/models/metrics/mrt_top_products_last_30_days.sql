with base as (
    select
        dp.product_id,
        dp.product_name,
        dp.category,
        sum(fs.revenue) as revenue_30d,
        sum(fs.quantity) as units_30d,
        count(*) as transactions_30d
    from {{ ref('fact_sales') }} fs
    inner join {{ ref('dim_product') }} dp
        on dp.product_sk = fs.product_sk
    inner join {{ ref('dim_time') }} dt
        on dt.time_sk = fs.time_sk
    where dt.full_date >= current_date - interval '30 days'
    group by
        dp.product_id,
        dp.product_name,
        dp.category
)
select
    product_id,
    product_name,
    category,
    revenue_30d,
    units_30d,
    transactions_30d,
    row_number() over (
        order by revenue_30d desc, units_30d desc, product_id asc
    ) as rank_30d
from base
