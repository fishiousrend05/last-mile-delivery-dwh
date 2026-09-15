with source as (
    select * from {{ source('landing_synthetic', 'delivery_attempts') }}
),
renamed as (
    select
        attempt_id,
        order_id,
        driver_id,
        zone_id,
        attempt_number::int as attempt_number,
        attempt_timestamp::timestamptz as attempt_timestamp,
        attempt_status,
        failed_reason_id
    from source
)
select * from renamed