with source as (
    select * from {{ ref('holidays') }}
),
renamed as (
    select
        date as holiday_date,
        name as holiday_name,
        local_name as holiday_local_name,
        country_code,
        holiday_impact_tier
    from source
)
select * from renamed