-- dbt/models/intermediate/extension/int_delivery_attempts.sql

{{ config(tags=['extension']) }}

/*
    Enrich stg_synthetic__delivery_attempts — KHÔNG mô phỏng lại (Monte Carlo
    nằm hoàn toàn ở synthetic_generator.py, đã validate ngay lúc sinh). Việc
    của model này:

    1. Tính lại is_first_attempt_flag / is_final_attempt_flag bằng window
       function — staging không mang theo 2 flag này (ATTEMPT_COLUMNS chỉ có
       8 cột thô).
    2. Cast attempt_timestamp -> attempt_date để join.
    3. Join weather theo zone_id + NGÀY ATTEMPT (khác fact_order_lifecycle
       dùng delivery date) — vì đây là thời tiết lúc CỐ GẮNG giao, kể cả lần
       thất bại, không phải lúc giao thành công. Join thẳng staging
       (KHÔNG ref dim_weather — mart) để giữ layering intermediate chỉ phụ
       thuộc staging; attempt_weather_key tính bằng CÙNG công thức hash với
       dim_weather.weather_key nên vẫn resolve đúng FK — xem test
       relationships ở file yml, đây là lưới an toàn nếu công thức lệch nhau.
    4. Resolve driver theo POINT-IN-TIME: join thật (range), không phải hash
       thuần túy, nên làm ở đây — giống cách int_order_lifecycle resolve
       zone_id qua zip_zone_mapping (business join ở intermediate, mart chỉ
       việc dùng lại).
    5. is_home_zone_driver_flag: driver được gán có home_zone_id trùng
       zone_id của attempt hay không — đo tỷ lệ vi phạm rule "driver cùng
       zone" của generator (rule #6, generator fallback zone -> state ->
       global khi zone đó không có driver).
*/

with attempts as (

    select
        attempt_id,
        order_id,
        driver_id,
        zone_id,
        attempt_number,
        attempt_timestamp,
        attempt_timestamp::date as attempt_date,
        attempt_status,
        failed_reason_id
    from {{ ref('stg_synthetic__delivery_attempts') }}

),

flagged as (

    select
        *,
        attempt_number = 1 as is_first_attempt_flag,
        attempt_number = max(attempt_number) over (partition by order_id) as is_final_attempt_flag,
        count(*) over (partition by order_id) as total_attempts_for_order
    from attempts

),

weather_at_attempt as (

    select
        f.attempt_id,
        w.zone_id is not null as has_weather,
        {{ dbt_utils.generate_surrogate_key(['f.zone_id', 'f.attempt_date']) }} as attempt_weather_key
    from flagged f
    left join {{ ref('stg_external__weather_daily') }} w
        on w.zone_id = f.zone_id
        and w.weather_date = f.attempt_date

),

-- Đúng 1 version driver có row_effective_date <= attempt_date <= row_end_date.
-- Nếu >1 dòng khớp, test assert_no_overlapping_driver_versions (model trước)
-- đã chặn từ gốc — fan-out ở đây KHÔNG được xảy ra trên data sạch.
driver_at_attempt as (

    select
        f.attempt_id,
        dv.driver_version_id,
        dv.home_zone_id as driver_home_zone_id
    from flagged f
    left join {{ ref('int_driver_versions') }} dv
        on dv.driver_id = f.driver_id
        and f.attempt_date between dv.row_effective_date and dv.row_end_date

)

select

    f.attempt_id,
    f.order_id,
    f.zone_id,
    f.driver_id,
    f.attempt_number,
    f.attempt_timestamp,
    f.attempt_date,
    f.attempt_status,
    f.failed_reason_id,
    f.is_first_attempt_flag,
    f.is_final_attempt_flag,
    f.total_attempts_for_order,

    d.driver_version_id,
    d.driver_home_zone_id,
    d.driver_home_zone_id = f.zone_id as is_home_zone_driver_flag,

    case when w.has_weather then w.attempt_weather_key end as attempt_weather_key

from flagged f
left join driver_at_attempt d on d.attempt_id = f.attempt_id
left join weather_at_attempt w on w.attempt_id = f.attempt_id