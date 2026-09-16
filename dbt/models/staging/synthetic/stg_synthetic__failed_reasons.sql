with source as (
    select * from {{ ref('failed_reasons') }}
),
renamed as (
    select
        failed_reason_id,
        category as failed_reason_category,
        reason_name as failed_reason_name
    from source
)
select * from renamed

