with source as (
    select * from {{ ref('commercial_events') }}
),
renamed as (
    select
        date::date as event_date,
        commercial_event_name,
        commercial_event_tier
    from source
)
select * from renamed