with weather as (
    select * from {{ ref('stg_external__weather_daily') }}
),

zones as (
    select zone_key, zone_id from {{ ref('dim_zone') }}
),

dates as (
    select date_key, full_date from {{ ref('dim_date') }}
),

bucketed as (
    select
        *,
        -- THRESHOLD TẠM — chưa có wind_speed, chỉ dựa temp + precipitation.
        -- Cần bạn xác nhận ngưỡng phù hợp bối cảnh Brazil trước khi coi là final.
        case
            when precipitation_mm >= 50 or temp_avg_c >= 35 then 'High'
            when precipitation_mm >= 10 or temp_avg_c >= 30 then 'Medium'
            else 'Low'
        end as weather_severity_bucket
    from weather
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
    b.weather_severity_bucket
from bucketed b
left join zones z on b.zone_id = z.zone_id
left join dates d on b.weather_date = d.full_date