-- dbt/models/marts/extension/dim_driver.sql

{{ config(tags=['extension']) }}

with driver_versions as (

    select * from {{ ref('int_driver_versions') }}

)

select

    {{ dbt_utils.generate_surrogate_key(['dv.driver_version_id']) }} as driver_key,

    dv.driver_version_id,
    dv.driver_id,
    dv.driver_name,
    dv.vehicle_type,
    dv.home_zone_id,
    z.zone_key as home_zone_key,
    dv.hire_date,
    dv.driver_status,
    dv.version_number,
    dv.row_effective_date,
    dv.row_end_date,
    dv.is_current

from driver_versions dv
left join {{ ref('dim_zone') }} z
    on z.zone_id = dv.home_zone_id