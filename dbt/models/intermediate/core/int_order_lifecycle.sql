with orders as (
    select * from {{ ref('stg_olist__orders') }}
),

customers as (
    select * from {{ ref('stg_olist__customers') }}
),

zip_zone as (
    select 
        -- Ép kiểu sang text và lấp đầy số 0 ở đầu cho đủ 5 ký tự để khớp với Staging
        lpad(zip_code_prefix::text, 5, '0') as zip_code_prefix,
        zone_id 
    from {{ ref('zip_zone_mapping') }}
),

weather as (
    select * from {{ ref('stg_external__weather_daily') }}
),

order_items as (
    select * from {{ ref('int_order_items_aggregated') }}
),

-- Resolve customer's zone: zip_code_prefix -> zone_id qua seed zip_zone_
-- (mapping build_zip_zone_mapping() đảm bảo 1 zip -> 1 zone_id, không fan-out)
orders_with_zone as (
    select
        o.*,
        zz.zone_id
    from orders o
    left join customers c
        on o.customer_id = c.customer_id
    left join zip_zone zz
        on c.zip_code_prefix = zz.zip_code_prefix
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.zone_id,

        -- full-lifecycle timestamps
        o.order_purchase_at,
        o.order_approved_at,
        o.order_delivered_carrier_at,
        o.order_delivered_customer_at,
        o.order_estimated_delivery_at,
        o.order_status,

        -- full-lifecycle metrics
        extract(epoch from (o.order_approved_at - o.order_purchase_at)) / 3600.0
            as approval_lag_hours,
        extract(epoch from (o.order_delivered_carrier_at - o.order_approved_at)) / 86400.0
            as carrier_lag_days,
        extract(epoch from (o.order_delivered_customer_at - o.order_purchase_at)) / 86400.0
            as total_fulfillment_days,

        -- last-mile metric (chặng carrier -> customer)
        extract(epoch from (o.order_delivered_customer_at - o.order_delivered_carrier_at)) / 86400.0
            as last_mile_delivery_days,

        -- estimated-delivery metrics
        extract(epoch from (o.order_delivered_customer_at - o.order_estimated_delivery_at)) / 86400.0
            as estimated_delivery_delay_days,
        case
            when o.order_delivered_customer_at > o.order_estimated_delivery_at then true
            when o.order_delivered_customer_at is not null then false
            else null
        end as estimated_delivery_breach_flag,

        -- KPI #2 (Last-Mile Completion Rate)
        (o.order_delivered_carrier_at is not null) as is_eligible_flag,
        (
            o.order_delivered_carrier_at is not null
            and o.order_status = 'delivered'
            and o.order_delivered_customer_at is not null
        ) as is_successful_flag,
        (
            o.order_delivered_carrier_at is not null
            and o.order_status not in ('delivered', 'canceled', 'unavailable')
        ) as is_unresolved_outcome_flag,

        -- delivery-moment weather — join theo zone + ngày giao đến tay khách.
        -- NULL có chủ đích với đơn chưa delivered.
        -- wind_speed / weather_severity_bucket tạm chưa đưa vào — chờ xác nhận
        -- wind_speed có bị thiếu thật ở staging hay không.
        w.temp_avg_c,
        w.temp_max_c,
        w.temp_min_c,
        w.precipitation_mm,

        -- order value
        oi.total_price,
        oi.total_freight,
        oi.item_count

    from orders_with_zone o
    left join order_items oi
        on o.order_id = oi.order_id
    left join weather w
        on o.zone_id = w.zone_id
        and o.order_delivered_customer_at::date = w.weather_date
)

select * from final