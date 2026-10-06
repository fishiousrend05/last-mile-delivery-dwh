-- dbt/models/marts/bi/dim_weather_for_bi.sql
{{ config(materialized='table', indexes=[{'columns': ['weather_key'], 'unique': true}]) }}

with used_in_order_lifecycle as (
    select distinct delivery_weather_key as weather_key
    from {{ ref('fact_order_lifecycle') }}
    where delivery_weather_key is not null
),

used_in_delivery_attempts as (
    select distinct weather_key
    from {{ ref('fact_delivery_attempts') }}
    where weather_key is not null
),

used_keys as (
    select weather_key from used_in_order_lifecycle
    union
    select weather_key from used_in_delivery_attempts
)

select dw.*
from {{ ref('dim_weather') }} dw
inner join used_keys uk
    on dw.weather_key = uk.weather_key