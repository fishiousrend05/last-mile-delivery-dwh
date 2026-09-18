with order_items as (
    select * from {{ ref('stg_olist__order_items') }}
),

aggregated as (
    select
        order_id,
        sum(price) as total_price,
        sum(freight_value) as total_freight,
        count(*) as item_count
    from order_items
    group by order_id
)

select * from aggregated