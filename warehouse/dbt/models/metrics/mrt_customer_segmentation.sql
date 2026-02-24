with customer_metrics as (
    select
        fs.customer_sk,
        sum(fs.revenue) as lifetime_value,
        count(*) as transaction_count,
        max(dt.full_date) as last_purchase_date
    from {{ ref('fact_sales') }} fs
    inner join {{ ref('dim_time') }} dt
        on dt.time_sk = fs.time_sk
    group by fs.customer_sk
),
scored as (
    select
        dc.customer_id,
        dc.customer_segment as source_segment,
        cm.lifetime_value,
        cm.transaction_count,
        (current_date - cm.last_purchase_date) as recency_days
    from customer_metrics cm
    inner join {{ ref('dim_customer') }} dc
        on dc.customer_sk = cm.customer_sk
)
select
    customer_id,
    source_segment,
    lifetime_value,
    transaction_count,
    recency_days,
    case
        when lifetime_value >= 1000 and transaction_count >= 10 and recency_days <= 30 then 'Champions'
        when lifetime_value >= 500 and transaction_count >= 5 and recency_days <= 90 then 'Loyal'
        when recency_days > 120 then 'At Risk'
        else 'Emerging'
    end as segment_bucket
from scored

