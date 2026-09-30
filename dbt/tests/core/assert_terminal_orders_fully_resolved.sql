with known_anomaly_orders as (
    select order_id
    from {{ ref('fact_order_lifecycle') }}
    where order_status = 'delivered'
      and not is_successful_flag
      -- đúng 9 đơn đã audit ở STM mục E, loại trừ hợp lệ
)
select order_id
from {{ ref('fact_order_lifecycle') }}
where order_status = 'delivered'
  and order_id not in (select order_id from known_anomaly_orders)
  and (
        purchase_date_key is null
     or carrier_date_key is null
     or delivered_date_key is null
     or estimated_delivery_date_key is null
  )