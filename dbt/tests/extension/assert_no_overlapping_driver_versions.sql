-- dbt/tests/extension/assert_no_overlapping_driver_versions.sql
-- Mỗi dòng trả về = 1 cặp version của CÙNG driver bị chồng khoảng ngày —
-- nếu có, point-in-time join sau này (fact_delivery_attempts) sẽ ra >1 driver_key
-- cho 1 attempt, gây fan-out.

select
    a.driver_id,
    a.driver_version_id as version_a,
    b.driver_version_id as version_b,
    a.row_effective_date as a_start,
    a.row_end_date as a_end,
    b.row_effective_date as b_start,
    b.row_end_date as b_end
from {{ ref('int_driver_versions') }} a
join {{ ref('int_driver_versions') }} b
    on a.driver_id = b.driver_id
    and a.driver_version_id < b.driver_version_id
    and a.row_effective_date <= b.row_end_date
    and b.row_effective_date <= a.row_end_date