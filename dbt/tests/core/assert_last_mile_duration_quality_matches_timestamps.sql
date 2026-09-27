select order_id
from {{ ref('int_order_lifecycle') }}
where (last_mile_duration_quality = 'valid'
        and not (order_delivered_customer_at > order_delivered_carrier_at))
   or (last_mile_duration_quality = 'zero_duration'
        and order_delivered_customer_at is distinct from order_delivered_carrier_at)
   or (last_mile_duration_quality = 'negative_duration'
        and not (order_delivered_customer_at < order_delivered_carrier_at))
   or (last_mile_duration_quality = 'missing_timestamp'
        and order_delivered_carrier_at is not null
        and order_delivered_customer_at is not null)