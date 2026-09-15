with source as (
    select * from {{ source('landing_external', 'weather_daily') }}
),
renamed as (
    select
        zone_id,
        date::date as weather_date,
        temp_max_c::numeric as temp_max_c,
        temp_min_c::numeric as temp_min_c,
        temp_avg_c::numeric as temp_avg_c,
        precipitation_mm::numeric as precipitation_mm,
        source as weather_source
    from source
)
select * from renamed