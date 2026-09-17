with source as (
    select * from {{ source('landing_external', 'weather_daily') }}
),
deduped as (
    select
        *,
        row_number() over (
            partition by zone_id, date
            order by (select null)
        ) as rn
    from source
),
renamed as (
    select
        zone_id,
        date::date as weather_date,
        temp_max_c,
        temp_min_c,
        temp_avg_c,
        precipitation_mm,
        source as weather_source
    from deduped
    where rn = 1
)
select * from renamed