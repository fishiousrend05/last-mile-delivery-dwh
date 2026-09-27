-- dbt/models/marts/extension/fact_driver_daily_kpi.sql

{{ config(tags=['extension']) }}

/*
    Grain: 1 dòng/driver_version_id × ngày lịch — PERIODIC SNAPSHOT fact
    (khác fact_delivery_attempts = transaction grain, fact_order_lifecycle =
    accumulating snapshot). Đặc điểm periodic snapshot: mọi period (ngày)
    trong phạm vi active của driver ĐỀU có 1 dòng, kể cả khi total_attempts=0
    — int_driver_daily đã đảm bảo full calendar coverage này, mart chỉ
    resolve key.

    date_key/driver_key tự động đúng theo SCD2: int_driver_daily.driver_spine
    join theo date_day BETWEEN row_effective_date AND row_end_date của TỪNG
    version, nên nếu 1 driver có 2 version (SCD2 thật), mỗi ngày sẽ tự động
    map đúng driver_key của version đang hiệu lực NGÀY HÔM ĐÓ — không cần xử
    lý gì thêm ở đây.

    success_rate: giữ nguyên là tỷ lệ tại GRAIN NÀY (1 driver × 1 ngày) — hợp
    lệ ở mức native grain, nhưng KHÔNG ĐƯỢC AVG(success_rate) khi rollup lên
    nhiều ngày/nhiều driver (Simpson's paradox — ngày ít attempt bị tính
    ngang ngày nhiều attempt). KPI #13 phải dùng
    SUM(successful_attempts)/SUM(total_attempts), không phải AVG(success_rate)
    — xem ghi chú ở bước int_driver_daily.
*/

with driver_daily as (

    select * from {{ ref('int_driver_daily') }}

)

select

    dd.driver_key,
    d.date_key,

    dr.total_attempts,
    dr.successful_attempts,
    dr.failed_attempts,
    dr.success_rate,
    dr.zones_covered_count,
    dr.is_active_day

from driver_daily dr
left join {{ ref('dim_driver') }} dd
    on dd.driver_version_id = dr.driver_version_id
left join {{ ref('dim_date') }} d
    on d.full_date = dr.date_day