-- Đảm bảo nhãn chất lượng khớp tuyệt đối với giá trị số học của last_mile_delivery_days
select 
    order_id,
    last_mile_delivery_days,
    last_mile_duration_quality
from {{ ref('fact_order_lifecycle') }}
where (last_mile_duration_quality = 'valid' and (last_mile_delivery_days is null or last_mile_delivery_days <= 0))
   or (last_mile_duration_quality = 'negative_duration' and (last_mile_delivery_days is null or last_mile_delivery_days >= 0))
   or (last_mile_duration_quality = 'zero_duration' and (last_mile_delivery_days is null or last_mile_delivery_days <> 0))
   or (last_mile_duration_quality = 'missing_timestamp' and last_mile_delivery_days is not null)