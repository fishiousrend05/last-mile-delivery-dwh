-- dbt/tests/extension/assert_driver_daily_reconciles_with_attempts.sql
-- So tổng total_attempts/successful_attempts giữa int_driver_daily (đã spine
-- theo ngày) và int_delivery_attempts (nguồn thô đã enrich) — phải khớp
-- TUYỆT ĐỐI vì int_driver_daily chỉ spine thêm ngày rỗng, không được làm
-- mất hay nhân đôi attempt nào đã có driver_version_id.

with from_daily as (
    select
        sum(total_attempts) as total_attempts,
        sum(successful_attempts) as successful_attempts
    from {{ ref('int_driver_daily') }}
),

from_attempts as (
    select
        count(*) as total_attempts,
        count(*) filter (where attempt_status = 'success') as successful_attempts
    from {{ ref('int_delivery_attempts') }}
    where driver_version_id is not null
)

select
    d.total_attempts as daily_total,
    a.total_attempts as attempts_total,
    d.successful_attempts as daily_success,
    a.successful_attempts as attempts_success
from from_daily d
cross join from_attempts a
where d.total_attempts != a.total_attempts
   or d.successful_attempts != a.successful_attempts