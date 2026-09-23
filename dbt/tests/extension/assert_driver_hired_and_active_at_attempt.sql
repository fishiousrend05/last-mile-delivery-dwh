-- dbt/tests/extension/assert_driver_hired_and_active_at_attempt.sql
-- Rule #6 (đã được Python đảm bảo LÚC SINH DỮ LIỆU) — test này là lưới an
-- toàn ở tầng dbt cho point-in-time join của driver_at_attempt trong
-- int_delivery_attempts, ĐỘC LẬP với check của Python. Hiện tại mọi driver
-- chỉ có 1 version nên test này PASS vô điều kiện (vacuous) — nó chỉ thật
-- sự có tác dụng từ lúc bạn demo SCD2 (UPDATE landing.drivers + snapshot lần
-- 2), khi driver_status có thể khác nhau giữa các version cùng driver_id.

select
    a.attempt_id,
    a.driver_id,
    a.attempt_date,
    dv.driver_version_id,
    dv.driver_status,
    dv.hire_date
from {{ ref('int_delivery_attempts') }} a
join {{ ref('int_driver_versions') }} dv
    on dv.driver_version_id = a.driver_version_id
where dv.driver_status != 'active'
   or a.attempt_date < dv.hire_date