{{ config(
    materialized='incremental',
    unique_key='time_sk',
    incremental_strategy='delete+insert',
    on_schema_change='sync_all_columns'
) }}

with source_dates as (
    select distinct event_date
    from {{ ref('stg_sales') }}
    {% if is_incremental() %}
    where event_date >= (
        select coalesce(max(full_date), '1900-01-01'::date)
        from {{ this }}
    )
    {% endif %}
)
select
    cast(to_char(event_date, 'YYYYMMDD') as integer) as time_sk,
    event_date as full_date,
    cast(extract(isodow from event_date) as smallint) as day_of_week,
    trim(to_char(event_date, 'Day')) as day_name,
    cast(extract(day from event_date) as smallint) as day_of_month,
    cast(extract(week from event_date) as smallint) as week_of_year,
    cast(extract(month from event_date) as smallint) as month_num,
    trim(to_char(event_date, 'Month')) as month_name,
    cast(extract(quarter from event_date) as smallint) as quarter_num,
    cast(extract(year from event_date) as integer) as year_num,
    (extract(isodow from event_date) in (6, 7)) as is_weekend
from source_dates

