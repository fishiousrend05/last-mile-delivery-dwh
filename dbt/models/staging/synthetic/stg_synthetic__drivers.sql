with source as (
    select * from {{ source('landing_synthetic', 'drivers') }}
),
renamed as (
    select
        driver_id,
        full_name as driver_name,
        vehicle_type,
        zone_id as home_zone_id,
        hire_date::date as hire_date,
        status as driver_status
    from source
)
select * from renamed