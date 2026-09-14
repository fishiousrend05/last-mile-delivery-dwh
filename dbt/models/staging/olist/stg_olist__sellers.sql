with source as (
    select * from {{ source('landing_olist', 'olist_sellers') }}
),
renamed as (
    select
        seller_id,
        lpad(seller_zip_code_prefix::text, 5, '0') as zip_code_prefix,
        seller_city as city,
        seller_state as state
    from source
)
select * from renamed