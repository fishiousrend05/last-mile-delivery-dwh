-- dbt/models/intermediate/extension/int_driver_versions.sql

{{
    config(
        tags=['extension']
    )
}}

/*
    Chuẩn hóa snap_driver (dbt snapshot, strategy='check') thành các version
    SCD2 sạch, sẵn sàng cho dim_driver và cho point-in-time join
    (driver_key resolution trong fact_delivery_attempts).

    Vấn đề cần xử lý: dbt_valid_from của VERSION ĐẦU TIÊN mỗi driver là thời
    điểm bạn CHẠY snapshot lần đầu (2026), không phải thời điểm driver có
    hiệu lực trong dữ liệu mô phỏng (2016-2018, tức hire_date). Nếu giữ
    nguyên dbt_valid_from, mọi attempt (2016-2018) sẽ join range không ra
    driver nào — toàn bộ dim_driver "biến mất" theo range. Vì vậy version
    đầu tiên của mỗi driver dùng hire_date làm row_effective_date; chỉ các
    version SAU (driver_status/vehicle_type/home_zone_id đổi giữa các lần
    snapshot) mới dùng đúng dbt_valid_from của chính nó.
*/

with snapshotted as (

    select
        driver_id,
        driver_name,
        vehicle_type,
        home_zone_id,
        hire_date,
        driver_status,
        dbt_scd_id,
        dbt_valid_from::date as dbt_valid_from_date,
        dbt_valid_to::date as dbt_valid_to_date
    from {{ ref('snap_driver') }}

),

versioned as (

    select
        *,
        row_number() over (
            partition by driver_id
            order by dbt_valid_from_date
        ) as version_number
    from snapshotted

)

select

    {{ dbt_utils.generate_surrogate_key(['driver_id', 'dbt_scd_id']) }} as driver_version_id,

    driver_id,
    driver_name,
    vehicle_type,
    home_zone_id,
    hire_date,
    driver_status,
    version_number,

    case
        when version_number = 1 then hire_date
        else dbt_valid_from_date
    end as row_effective_date,

    coalesce(dbt_valid_to_date, date('9999-12-31')) as row_end_date,

    dbt_valid_to_date is null as is_current

from versioned