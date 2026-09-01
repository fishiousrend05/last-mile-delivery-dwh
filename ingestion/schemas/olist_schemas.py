import pandera as pa
from pandera import Column, Check
 
 
# ---------------------------------------------------------------------------
# olist_orders_dataset.csv
# ---------------------------------------------------------------------------
olist_orders_schema = pa.DataFrameSchema(
    {
        "order_id": Column(str, nullable=False, unique=True),
        "customer_id": Column(str, nullable=False),
        "order_status": Column(
            str,
            Check.isin(
                [
                    "delivered", "shipped", "canceled", "unavailable",
                    "invoiced", "processing", "created", "approved",
                ]
            ),
            nullable=False,
        ),
        "order_purchase_timestamp": Column(pa.DateTime, nullable=False),
        # order_approved_at có thể null nếu đơn bị huỷ trước khi duyệt thanh toán
        "order_approved_at": Column(pa.DateTime, nullable=True),
        # carrier/customer date null với đơn chưa giao / bị huỷ giữa chừng
        "order_delivered_carrier_date": Column(pa.DateTime, nullable=True),
        "order_delivered_customer_date": Column(pa.DateTime, nullable=True),
        "order_estimated_delivery_date": Column(pa.DateTime, nullable=False),
    },
    strict=False,
    coerce=True,
)
 
 
# ---------------------------------------------------------------------------
# olist_order_items_dataset.csv
# ---------------------------------------------------------------------------
olist_order_items_schema = pa.DataFrameSchema(
    {
        "order_id": Column(str, nullable=False),
        "order_item_id": Column(int, Check.ge(1), nullable=False),
        "product_id": Column(str, nullable=False),
        "seller_id": Column(str, nullable=False),
        "shipping_limit_date": Column(pa.DateTime, nullable=False),
        "price": Column(float, Check.ge(0), nullable=False),
        "freight_value": Column(float, Check.ge(0), nullable=False),
    },
    strict=False,
    coerce=True,
)
 
 
# ---------------------------------------------------------------------------
# olist_customers_dataset.csv
# ---------------------------------------------------------------------------
olist_customers_schema = pa.DataFrameSchema(
    {
        "customer_id": Column(str, nullable=False, unique=True),
        "customer_unique_id": Column(str, nullable=False),
        # zip prefix có ~24-33% gap coverage theo audit trước đó -> nullable
        "customer_zip_code_prefix": Column(str, nullable=True),
        "customer_city": Column(str, nullable=False),
        "customer_state": Column(str, Check.str_length(2, 2), nullable=False),
    },
    strict=False,
    coerce=True,
)
 
 
# ---------------------------------------------------------------------------
# olist_products_dataset.csv
# ---------------------------------------------------------------------------
olist_products_schema = pa.DataFrameSchema(
    {
        "product_id": Column(str, nullable=False, unique=True),
        # category có thể null trong data gốc (sản phẩm chưa được gắn danh mục)
        "product_category_name": Column(str, nullable=True),
        "product_name_lenght": Column(float, Check.ge(0), nullable=True),
        "product_description_lenght": Column(float, Check.ge(0), nullable=True),
        "product_photos_qty": Column(float, Check.ge(0), nullable=True),
        "product_weight_g": Column(float, Check.ge(0), nullable=True),
        "product_length_cm": Column(float, Check.ge(0), nullable=True),
        "product_height_cm": Column(float, Check.ge(0), nullable=True),
        "product_width_cm": Column(float, Check.ge(0), nullable=True),
    },
    strict=False,
    coerce=True,
)
 
 
# ---------------------------------------------------------------------------
# olist_sellers_dataset.csv
# ---------------------------------------------------------------------------
olist_sellers_schema = pa.DataFrameSchema(
    {
        "seller_id": Column(str, nullable=False, unique=True),
        "seller_zip_code_prefix": Column(str, nullable=True),
        "seller_city": Column(str, nullable=False),
        "seller_state": Column(str, Check.str_length(2, 2), nullable=False),
    },
    strict=False,
    coerce=True,
)
 
 
# ---------------------------------------------------------------------------
# olist_order_payments_dataset.csv
# ---------------------------------------------------------------------------
olist_order_payments_schema = pa.DataFrameSchema(
    {
        "order_id": Column(str, nullable=False),
        "payment_sequential": Column(int, Check.ge(1), nullable=False),
        "payment_type": Column(
            str,
            Check.isin(["credit_card", "boleto", "voucher", "debit_card", "not_defined"]),
            nullable=False,
        ),
        "payment_installments": Column(int, Check.ge(0), nullable=False),
        "payment_value": Column(float, Check.ge(0), nullable=False),
    },
    strict=False,
    coerce=True,
)
 
 
# ---------------------------------------------------------------------------
# olist_order_reviews_dataset.csv
# ---------------------------------------------------------------------------
olist_order_reviews_schema = pa.DataFrameSchema(
    {
        "review_id": Column(str, nullable=False),
        "order_id": Column(str, nullable=False),
        "review_score": Column(int, Check.in_range(1, 5), nullable=False),
        # comment title/message thường null vì khách không bắt buộc để lại text
        "review_comment_title": Column(str, nullable=True),
        "review_comment_message": Column(str, nullable=True),
        "review_creation_date": Column(pa.DateTime, nullable=False),
        "review_answer_timestamp": Column(pa.DateTime, nullable=False),
    },
    strict=False,
    coerce=True,
)
 
 
# ---------------------------------------------------------------------------
# olist_geolocation_dataset.csv
# ---------------------------------------------------------------------------
# Lưu ý: bảng này có rất nhiều bản ghi trùng zip_code_prefix (nhiều lat/lng
# cho cùng 1 prefix) -> KHÔNG đặt unique ở đây, việc dedup xử lý ở dbt staging,
# không phải ở bước validate ingestion.
olist_geolocation_schema = pa.DataFrameSchema(
    {
        "geolocation_zip_code_prefix": Column(str, nullable=False),
        "geolocation_lat": Column(float, Check.in_range(-90, 90), nullable=False),
        "geolocation_lng": Column(float, Check.in_range(-180, 180), nullable=False),
        "geolocation_city": Column(str, nullable=False),
        "geolocation_state": Column(str, Check.str_length(2, 2), nullable=False),
    },
    strict=False,
    coerce=True,
)
 
 
# ---------------------------------------------------------------------------
# product_category_name_translation.csv
# ---------------------------------------------------------------------------
product_category_translation_schema = pa.DataFrameSchema(
    {
        "product_category_name": Column(str, nullable=False, unique=True),
        "product_category_name_english": Column(str, nullable=False),
    },
    strict=False,
    coerce=True,
)
 
 
# Registry nội bộ, dùng để validators/schema_validator.py import gọn 1 dict
# thay vì import từng schema riêng lẻ.
SCHEMAS = {
    "olist_orders": olist_orders_schema,
    "olist_order_items": olist_order_items_schema,
    "olist_customers": olist_customers_schema,
    "olist_products": olist_products_schema,
    "olist_sellers": olist_sellers_schema,
    "olist_order_payments": olist_order_payments_schema,
    "olist_order_reviews": olist_order_reviews_schema,
    "olist_geolocation": olist_geolocation_schema,
    "product_category_translation": product_category_translation_schema,
}