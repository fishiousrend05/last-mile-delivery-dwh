with source as (
    -- Sử dụng ref() vì đây là seed, không phải source từ database
    select * from {{ ref('zone_centroids') }}
),

renamed as (
    select
        zone_id,
        centroid_lat as zone_centroid_latitude,
        centroid_lng as zone_centroid_longitude,
        point_count as zone_point_count,
        dominant_state as zone_dominant_state
    from source
)

select * from renamed