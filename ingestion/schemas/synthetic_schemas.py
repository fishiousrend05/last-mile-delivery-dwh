import pandera as pa
from pandera import Column, Check
 
 
# ---------------------------------------------------------------------------
# drivers.csv -> nguồn cho dim_driver (SCD2, versioning xử lý ở dbt snapshot,
# raw generator chỉ sinh trạng thái hiện tại của từng driver).
# ---------------------------------------------------------------------------
drivers_schema = pa.DataFrameSchema(
    {
        "driver_id": Column(str, nullable=False, unique=True),
        "full_name": Column(str, nullable=False),
        "vehicle_type": Column(
            str,
            Check.isin(["motorcycle", "car", "van", "bicycle"]),
            nullable=False,
        ),
        # zone_id chủ yếu hoạt động (H3 res5), dùng để phân bổ attempt hợp lý
        "zone_id": Column(str, Check.str_length(15, 15), nullable=False),
        "hire_date": Column(pa.DateTime, nullable=False),
        "status": Column(str, Check.isin(["active", "inactive"]), nullable=False),
    },
    strict=True,
    coerce=True,
)
 
 
# ---------------------------------------------------------------------------
# delivery_attempts.csv -> nguồn cho fact_delivery_attempts
# Grain: 1 dòng = 1 lần giao hàng được mô phỏng cho 1 order (không phải dữ
# liệu lịch sử thật) — anchor vào outcome cuối cùng của Olist + SLA delay +
# rủi ro zone/weather/holiday qua simulation xác suất.
# ---------------------------------------------------------------------------
delivery_attempts_schema = pa.DataFrameSchema(
    {
        "attempt_id": Column(str, nullable=False, unique=True),
        "order_id": Column(str, nullable=False),
        "driver_id": Column(str, nullable=False),
        "zone_id": Column(str, Check.str_length(15, 15), nullable=False),
        # thứ tự lần giao trong cùng 1 order (1 = lần đầu tiên)
        "attempt_number": Column(int, Check.ge(1), nullable=False),
        "attempt_timestamp": Column(pa.DateTime, nullable=False),
        "attempt_status": Column(str, Check.isin(["success", "failed"]), nullable=False),
        # chỉ có giá trị khi attempt_status == 'failed', check chéo bên dưới
        "failed_reason_id": Column(str, nullable=True),
    },
    checks=[
        # failed_reason_id bắt buộc có khi failed, bắt buộc rỗng khi success
        # -> tránh lỗi logic simulation (vd gán nhầm reason cho attempt thành công).
        Check(
            lambda df: (
                (df["attempt_status"] == "failed") == df["failed_reason_id"].notna()
            ).all(),
            error=(
                "failed_reason_id must be set if and only if attempt_status == 'failed'"
            ),
        ),
        # attempt_number phải liên tục từ 1 trong phạm vi mỗi order (không
        # được nhảy cóc 1 -> 3), bắt lỗi sớm trong logic sinh dữ liệu simulation.
        Check(
            lambda df: df.groupby("order_id")["attempt_number"]
            .apply(lambda s: sorted(s.tolist()) == list(range(1, len(s) + 1)))
            .all(),
            error="attempt_number must be a contiguous sequence starting at 1 per order_id",
        ),
    ],
    strict=True,
    coerce=True,
)
 
 
SCHEMAS = {
    "drivers": drivers_schema,
    "delivery_attempts": delivery_attempts_schema,
}