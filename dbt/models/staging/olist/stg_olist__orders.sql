with source as (
    select * from {{ source('landing_olist', 'olist_orders') }}
),
renamed as (
    select
        order_id,
        customer_id,
        order_status,
        -- Ép kiểu đồng loạt tất cả các mốc thời gian sang TIMESTAMPTZ chuẩn
        order_purchase_timestamp::timestamptz as order_purchase_at,
        order_approved_at::timestamptz as order_approved_at,
        order_delivered_carrier_date::timestamptz as order_delivered_carrier_at,
        order_delivered_customer_date::timestamptz as order_delivered_customer_at,
        order_estimated_delivery_date::timestamptz as order_estimated_delivery_at
    from source
)
select * from renamed