with lifecycle as (

    select l.*
    from {{ ref('int_order_lifecycle') }} as l

    {% if is_incremental() %}

    where 
    -- 1. Cập nhật các đơn hàng cũ đang có sự thay đổi (chưa giao & chưa hủy)
    l.order_id in (
        select order_id
        from {{ this }}
        where delivered_date_key is null
          and order_status not in ('canceled', 'unavailable')
    )

    -- 2. Thêm các đơn hàng mới hoàn toàn (dùng NOT EXISTS thay vì NOT IN để chạy nhanh hơn)
    or not exists (
        select 1 
        from {{ this }} t 
        where t.order_id = l.order_id
    )

    {% endif %}

),

zones as (
    select zone_key, zone_id from {{ ref('dim_zone') }}
),

weather as (
    select weather_key, zone_id, weather_date from {{ ref('dim_weather') }}
),

dim_date_purchase as (select date_key, full_date from {{ ref('dim_date') }}),
dim_date_approved as (select date_key, full_date from {{ ref('dim_date') }}),
dim_date_carrier  as (select date_key, full_date from {{ ref('dim_date') }}),
dim_date_delivered as (select date_key, full_date from {{ ref('dim_date') }}),
dim_date_estimated as (select date_key, full_date from {{ ref('dim_date') }}),

final as (
    select
        l.order_id,
        l.customer_id,

        z.zone_key as customer_zone_key,

        dp.date_key as purchase_date_key,
        da.date_key as approved_date_key,
        dc.date_key as carrier_date_key,
        dd.date_key as delivered_date_key,
        de.date_key as estimated_delivery_date_key,

        w.weather_key as delivery_weather_key,

        l.order_status,

        l.approval_lag_hours,
        l.carrier_lag_days,
        l.total_fulfillment_days,
        l.last_mile_delivery_days,
        l.estimated_delivery_delay_days,
        l.estimated_delivery_breach_flag,

        l.is_eligible_flag,
        l.is_successful_flag,
        l.is_unresolved_outcome_flag,

        l.total_price,
        l.total_freight,
        l.item_count

    from lifecycle l
    left join zones z
        on l.zone_id = z.zone_id
    left join dim_date_purchase dp
        on l.order_purchase_at::date = dp.full_date
    left join dim_date_approved da
        on l.order_approved_at::date = da.full_date
    left join dim_date_carrier dc
        on l.order_delivered_carrier_at::date = dc.full_date
    left join dim_date_delivered dd
        on l.order_delivered_customer_at::date = dd.full_date
    left join dim_date_estimated de
        on l.order_estimated_delivery_at::date = de.full_date
    left join weather w
        on l.zone_id = w.zone_id
        and l.order_delivered_customer_at::date = w.weather_date
)

select * from final