{{ config(
    materialized='table',
    indexes=[{'columns': ['zone_id'], 'type': 'btree'}]
) }}

with weather as (
    select * from {{ ref('stg_external__weather_daily') }}
),

zone_percentiles as (
    select
        zone_id,
        count(*) as n_observations,
        percentile_cont(0.90) within group (order by temp_avg_c) as temp_p90,
        percentile_cont(0.95) within group (order by temp_avg_c) as temp_p95,
        percentile_cont(0.90) within group (order by precipitation_mm) as precip_p90,
        percentile_cont(0.95) within group (order by precipitation_mm) as precip_p95
    from weather
    group by zone_id
),

bucketed as (
    select
        w.*,
        zp.n_observations,
        case
            -- zone có quá ít quan sát thì percentile không ổn định -> fallback Unknown
            when zp.n_observations < 30 then 'Unknown'
            when w.temp_avg_c >= zp.temp_p95 or w.precipitation_mm >= zp.precip_p95 then 'High'
            when w.temp_avg_c >= zp.temp_p90 or w.precipitation_mm >= zp.precip_p90 then 'Medium'
            else 'Low'
        end as weather_severity_bucket
    from weather w
    left join zone_percentiles zp
        on w.zone_id = zp.zone_id
),

zones as (
    select zone_key, zone_id from {{ ref('dim_zone') }}
),

dates as (
    select date_key, full_date from {{ ref('dim_date') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['b.zone_id', 'b.weather_date']) }} as weather_key,
    z.zone_key,
    d.date_key,
    b.zone_id,
    b.weather_date,
    b.temp_avg_c,
    b.temp_max_c,
    b.temp_min_c,
    b.precipitation_mm,
    b.n_observations,
    b.weather_severity_bucket
from bucketed b
left join zones z on b.zone_id = z.zone_id
left join dates d on b.weather_date = d.full_date