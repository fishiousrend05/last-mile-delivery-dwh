-- dbt/tests/extension/assert_final_attempt_matches_order_outcome.sql
-- Mỗi dòng trả về = 1 order mà attempt cuối "success/failed" KHÔNG khớp kết
-- quả thật của đơn (order_status/customer_at) — vi phạm quy tắc #2a đã chốt
-- trong generator, dbt phải soi lại đúng bất biến này ở tầng transform.

with final_attempts as (

    select order_id, attempt_status
    from {{ ref('int_delivery_attempts') }}
    where is_final_attempt_flag

)

select
    fa.order_id,
    fa.attempt_status as final_attempt_status,
    ol.is_successful_flag
from final_attempts fa
join {{ ref('int_order_lifecycle') }} ol using (order_id)
where (fa.attempt_status = 'success') != coalesce(ol.is_successful_flag, false)