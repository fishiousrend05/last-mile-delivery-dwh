-- dbt/tests/extension/assert_attempt_after_carrier_date.sql
{{ config(severity='warn') }}

/*
    KHÔNG fail cứng: rule #2b (attempt cuối PHẢI khớp order_delivered_customer_at
    thật) được ưu tiên cao hơn rule #5 (không attempt trước carrier_at). Với các
    order mà Olist tự ghi order_delivered_customer_at <= order_delivered_carrier_at
    (dữ liệu gốc bất thường, đã xác nhận ~32 order qua audit), generator vẫn phải
    neo attempt cuối vào đúng delivered_at thật — kết quả là attempt đó "trước"
    carrier_at. Xem synthetic_generator.py: validate_delivery_attempts() đã đếm
    số này vào metadata["validation"]["n_attempts_at_or_before_carrier_date"]
    (soft check, không raise) với đúng lý do trên.

    Test này ở mức WARN để bạn vẫn thấy số liệu mỗi lần build, nhưng không chặn
    pipeline vì đây là known limitation của dữ liệu Olist, không phải bug.
*/

select
    a.attempt_id,
    a.order_id,
    a.attempt_timestamp,
    ol.order_delivered_carrier_at
from {{ ref('int_delivery_attempts') }} a
join {{ ref('int_order_lifecycle') }} ol using (order_id)
where a.attempt_timestamp <= ol.order_delivered_carrier_at