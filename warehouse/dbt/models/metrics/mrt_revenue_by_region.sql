select
    dt.full_date as report_date,
    ds.region,
    sum(fs.revenue) as revenue_amount,
    sum(fs.quantity) as units_sold,
    count(*) as transaction_count
from {{ ref('fact_sales') }} fs
inner join {{ ref('dim_time') }} dt
    on dt.time_sk = fs.time_sk
inner join {{ ref('dim_store') }} ds
    on ds.store_sk = fs.store_sk
group by
    dt.full_date,
    ds.region
