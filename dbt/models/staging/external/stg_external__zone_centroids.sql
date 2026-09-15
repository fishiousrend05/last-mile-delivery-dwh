with source as (
    select * from {{ ref('zone_centroids') }}
),
renamed as (
    select
        zone_id,
        centroid_lat,
        centroid_lng,
        point_count,
        dominant_state
    from source
)
select * from renamed