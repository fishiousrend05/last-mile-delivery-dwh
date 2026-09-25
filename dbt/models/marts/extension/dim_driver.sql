-- dbt/models/marts/extension/dim_driver.sql

{{ config(tags=['extension']) }}

/*
    Grain: 1 dòng/version SCD2 của driver — chọn thẳng từ int_driver_versions,
    mart chỉ resolve surrogate key (đúng pattern đã dùng ở marts/core: logic
    nghiệp vụ nằm ở intermediate, mart không tự tính gì thêm).

    home_zone_id -> home_zone_key: resolve qua dim_zone để nhất quán với cách
    fact_order_lifecycle resolve customer_zone_key (Kimball: dimension khác
    dimension luôn nối qua surrogate key, không giữ natural key H3 thô ở đây)
    — outrigger reference, KHÔNG phải fact join.

    driver_key dùng driver_version_id làm nguồn hash (đã unique theo grain
    1 dòng/version) — không dùng lại chính driver_version_id làm PK trần vì
    driver_version_id vốn được sinh từ generate_surrogate_key(driver_id,
    dbt_scd_id) bên int_driver_versions rồi, hash thêm 1 lớp ở đây giữ đúng
    convention "mọi PK trong marts/ đều là *_key do chính model đó sinh",
    khớp cách dim_date/dim_zone/dim_weather đang làm.
*/

with driver_versions as (

    select * from {{ ref('int_driver_versions') }}

)

select

    {{ dbt_utils.generate_surrogate_key(['dv.driver_version_id']) }} as driver_key,

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