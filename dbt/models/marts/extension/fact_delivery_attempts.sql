-- dbt/models/marts/extension/fact_delivery_attempts.sql

{{ config(tags=['extension']) }}

/*
    Grain: 1 dòng/lần giao mô phỏng/order (khớp int_delivery_attempts).
    Mart chỉ resolve surrogate key — mọi logic nghiệp vụ (flag, join weather
    theo attempt date, join driver theo point-in-time) đã nằm ở
    int_delivery_attempts.sql.

    driver_key: JOIN THẲNG qua driver_version_id (không recompute hash).
    dim_driver giữ lại cột driver_version_id gốc (chưa hash) bên cạnh
    driver_key đã hash — nên đây là join tự nhiên qua khóa đã có sẵn ở cả 2
    bên, an toàn hơn việc tự tính lại generate_surrogate_key(['driver_version_id'])
    ở model này rồi hy vọng khớp bit-for-bit với cách dim_driver tính. Sửa lại
    ghi chú ở bước dim_driver trước đó — join qua natural id, không qua hash.

    weather_key: dùng THẲNG int_delivery_attempts.attempt_weather_key (đã
    tính theo đúng công thức hash của dim_weather.weather_key ở tầng
    intermediate) — không join qua dim_weather ở đây, resolve 1 lần duy nhất.

    order_id: KHÔNG có surrogate order_key riêng — fact_order_lifecycle giữ
    order_id làm PK tự nhiên (giống Olist gốc), nên fact này tham chiếu thẳng
    order_id, không hash.

    Không có FR00 — attempt_status là cột filter chính cho "thành công/thất
    bại" (KPI #8-11 group theo attempt_status, không dựa vào failed_reason_key
    IS NOT NULL). failed_reason_key NULL với attempt success là có chủ đích.

    sla_risk_score: KHÔNG có field này — đã bỏ khỏi schema từ Phase 1 (chỉ
    dùng nội bộ tính xác suất khi generate, không lưu cột) — khác STM gốc.
*/

with attempts as (

    select * from {{ ref('int_delivery_attempts') }}

)

select

    a.attempt_id,
    a.order_id,
    a.attempt_number,
    a.attempt_timestamp,
    a.attempt_status as attempt_outcome,

    dd.date_key as attempt_date_key,
    z.zone_key,
    drv.driver_key,
    a.attempt_weather_key as weather_key,
    fr.failed_reason_key,

    a.is_first_attempt_flag,
    a.is_final_attempt_flag,
    a.is_home_zone_driver_flag

from attempts a
left join {{ ref('dim_date') }} dd
    on dd.full_date = a.attempt_date
left join {{ ref('dim_zone') }} z
    on z.zone_id = a.zone_id
left join {{ ref('dim_driver') }} drv
    on drv.driver_version_id = a.driver_version_id
left join {{ ref('dim_failed_reason') }} fr
    on fr.failed_reason_id = a.failed_reason_id