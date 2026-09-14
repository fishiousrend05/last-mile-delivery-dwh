with source as (
    select * from {{ source('landing_olist', 'olist_customers') }}
),
renamed as (
    select
        customer_id,
        customer_unique_id,
        -- Chuẩn hóa lại 5 chữ số cho zip_code_prefix (data gốc bị mất số 0 đầu do bị đọc thành kiểu số)
        lpad(customer_zip_code_prefix::text, 5, '0') as zip_code_prefix,
        customer_city as city,
        customer_state as state
    from source
)
select * from renamed