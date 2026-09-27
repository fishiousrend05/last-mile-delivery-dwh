with violations as (
    select order_id
    from {{ ref('int_order_lifecycle') }}
    where (order_approved_at is not null and order_approved_at < order_purchase_at)
       or (order_delivered_carrier_at is not null and order_approved_at is not null
           and order_delivered_carrier_at < order_approved_at)
       or (last_mile_duration_quality = 'negative_duration')
)
select count(*) as violation_count
from violations
having count(*) <> 23
-- Baseline đã biết: đúng 23 negative_duration (xem STM mục E). Khác 23 nghĩa là
-- có bất thường MỚI ngoài known limitation -> cần điều tra, không phải sửa test.