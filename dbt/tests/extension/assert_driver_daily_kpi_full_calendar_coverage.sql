-- dbt/tests/extension/assert_driver_daily_kpi_full_calendar_coverage.sql
-- BẤT BIẾN QUAN TRỌNG NHẤT của periodic snapshot fact: mỗi driver_version
-- PHẢI có ĐÚNG số ngày = độ dài khoảng active của nó (cắt theo biên DWH
-- 2016-01-01 -> 2018-12-31), không thiếu (gap) không thừa (duplicate).
-- Reconciliation ở trên chỉ so TỔNG số dòng/measures — test này so
-- TỪNG driver_version, bắt được cả lỗi bù trừ (driver A thiếu 5 ngày,
-- driver B thừa 5 ngày -> tổng vẫn khớp nhưng đây vẫn là lỗi).

with expected as (

    select
        driver_version_id,
        (
            least(row_end_date, {{ var('dwh_end_date', "'2018-12-31'") }}::date)
            - greatest(row_effective_date, {{ var('dwh_start_date', "'2016-01-01'") }}::date)
            + 1
        ) as expected_days
    from {{ ref('int_driver_versions') }}
    where row_effective_date <= {{ var('dwh_end_date', "'2018-12-31'") }}::date
      and row_end_date >= {{ var('dwh_start_date', "'2016-01-01'") }}::date

),

actual as (

    select
        dd.driver_version_id,
        count(*) as actual_days
    from {{ ref('fact_driver_daily_kpi') }} f
    join {{ ref('dim_driver') }} dd on dd.driver_key = f.driver_key
    group by dd.driver_version_id

)

select
    e.driver_version_id,
    e.expected_days,
    coalesce(a.actual_days, 0) as actual_days
from expected e
left join actual a using (driver_version_id)
where coalesce(a.actual_days, 0) != e.expected_days