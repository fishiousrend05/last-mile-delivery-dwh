with source as (
    select * from {{ source('landing_olist', 'olist_products') }}
),
renamed as (
    select
        product_id,
        product_category_name,
        product_name_lenght as product_name_length, -- Sửa lỗi chính tả từ data gốc
        product_description_lenght as product_description_length, -- Sửa lỗi chính tả
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    from source
)
select * from renamed