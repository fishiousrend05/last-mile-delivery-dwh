with zones as (
    select * from {{ ref('zone_centroids') }}
),

labeled as (
    select
        *,
        row_number() over (partition by dominant_state order by zone_id) as state_seq
    from zones
)

select
    {{ dbt_utils.generate_surrogate_key(['zone_id']) }} as zone_key,
    zone_id,
    centroid_lat,
    centroid_lng,
    point_count,
    dominant_state,
    dominant_state || '-' || lpad(state_seq::text, 4, '0') as zone_label
from labeled