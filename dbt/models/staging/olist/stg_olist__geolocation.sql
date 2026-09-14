with source as (
    select * from {{ source('landing_olist', 'olist_geolocation') }}
),
deduped as (
    select
        lpad(geolocation_zip_code_prefix::text, 5, '0') as zip_code_prefix,
        geolocation_lat as latitude,
        geolocation_lng as longitude,
        geolocation_city as city,
        geolocation_state as state,
        -- Lấy tọa độ đầu tiên cho mỗi mã zip code để loại bỏ trùng lặp
        row_number() over (
            partition by lpad(geolocation_zip_code_prefix::text, 5, '0')
            order by geolocation_lat, geolocation_lng
        ) as rn
    from source
)
select
    zip_code_prefix,
    latitude,
    longitude,
    city,
    state
from deduped
where rn = 1