-- dbt/models/intermediate/extension/int_driver_daily.sql

{{ config(tags=['extension']) }}

/*
    Grain: 1 dòng = 1 driver_version × 1 ngày lịch, PHỦ TOÀN BỘ khoảng ngày
    driver version đó active — kể cả ngày không có attempt nào (metrics = 0).
    Đây là yêu cầu đã chốt trong STM: "Bao gồm cả những ngày tài xế không có
    hoạt động giao hàng, với các chỉ số bằng 0" — không dùng inner join với
    attempts (sẽ mất hết ngày rảnh).

    Ngày lịch dùng chung 1 range với dim_date (2016-01-01 -> 2018-12-31) để
    date_key resolve đúng ở mart. Model này KHÔNG ref dim_date (marts) —
    intermediate không phụ thuộc ngược lên marts — nên range được lặp lại ở
    đây qua 2 dbt var (xem ghi chú cuối file). Nếu sau này đổi range dim_date,
    PHẢI đổi luôn 2 var này.

    driver_version_id (không phải driver_id) là đơn vị join chính — vì mỗi
    version có 1 khoảng [row_effective_date, row_end_date] riêng, và
    assert_no_overlapping_driver_versions đã đảm bảo 1 driver_id không bao
    giờ có 2 version cùng active tại 1 ngày.
*/

with date_spine as (

    select cast(d as date) as date_day
    from generate_series(
        {{ var('dwh_start_date', "'2016-01-01'") }}::date,
        {{ var('dwh_end_date', "'2018-12-31'") }}::date,
        interval '1 day'
    ) as d

),

driver_spine as (

    select
        dv.driver_id,
        dv.driver_version_id,
        ds.date_day
    from {{ ref('int_driver_versions') }} dv
    inner join date_spine ds
        on ds.date_day between dv.row_effective_date and dv.row_end_date

),

attempts_daily as (

    select
        driver_version_id,
        attempt_date,
        count(*) as total_attempts,
        count(*) filter (where attempt_status = 'success') as successful_attempts,
        count(*) filter (where attempt_status = 'failed') as failed_attempts,
        count(distinct zone_id) as zones_covered_count
    from {{ ref('int_delivery_attempts') }}
    where driver_version_id is not null  -- xem not_null(severity=warn) trên cột này ở int_delivery_attempts
    group by 1, 2

)

select

    ds.driver_id,
    ds.driver_version_id,
    ds.date_day,

    coalesce(a.total_attempts, 0) as total_attempts,
    coalesce(a.successful_attempts, 0) as successful_attempts,
    coalesce(a.failed_attempts, 0) as failed_attempts,

    -- NULL (không phải 0) khi total_attempts = 0 — tỷ lệ không xác định trên
    -- mẫu số 0, khác về ngữ nghĩa với "hôm đó tỷ lệ thành công = 0%".
    case
        when coalesce(a.total_attempts, 0) > 0
        then a.successful_attempts::numeric / a.total_attempts
    end as success_rate,

    coalesce(a.zones_covered_count, 0) as zones_covered_count,
    coalesce(a.total_attempts, 0) > 0 as is_active_day

from driver_spine ds
left join attempts_daily a
    on a.driver_version_id = ds.driver_version_id
    and a.attempt_date = ds.date_day