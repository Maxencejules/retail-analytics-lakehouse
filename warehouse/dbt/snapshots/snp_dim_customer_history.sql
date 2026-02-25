{% snapshot snp_dim_customer_history %}
{{
    config(
        unique_key="customer_id",
        strategy="timestamp",
        updated_at="source_updated_at",
        invalidate_hard_deletes=True
    )
}}

select
    customer_id,
    customer_name,
    customer_segment,
    city,
    province,
    country_code,
    source_updated_at
from {{ ref('stg_customers') }}

{% endsnapshot %}
