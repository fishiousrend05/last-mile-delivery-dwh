with source as (
    select * from {{ source('landing_olist', 'product_category_translation') }}
),

renamed as (
    select
        product_category_name,
        product_category_name_english
    from source
)

select * from renamed