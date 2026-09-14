with source as (
    select * from {{ source('landing_olist', 'olist_order_items') }}
),
renamed as (
    select
        order_id,
        order_item_id::int as order_item_id,
        product_id,
        seller_id,
        shipping_limit_date::timestamptz as shipping_limit_date,
        price::numeric as price,
        freight_value::numeric as freight_value
    from source
)
select * from renamed