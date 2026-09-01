import pandera as pa
from pandera import Column, Check
 
 
holidays_schema = pa.DataFrameSchema(
    {
        "date": Column(pa.DateTime, nullable=False),
        # tên ngày lễ theo tiếng địa phương (Nager.Date field: localName)
        "local_name": Column(str, nullable=False),
        # tên tiếng Anh (Nager.Date field: name)
        "name": Column(str, nullable=False),
        "country_code": Column(str, Check.isin(["BR"]), nullable=False),
        # holiday cố định hàng năm (vd Natal 25/12) hay thay đổi ngày (vd Carnaval)
        "is_fixed": Column(bool, nullable=False),
        # áp dụng toàn quốc hay chỉ 1 số bang
        "is_global": Column(bool, nullable=False),
        # cột tự thêm ngoài response gốc của Nager.Date — DERIVED, để phân
        # nhóm ngày lễ khi build holiday_tier trong dim_date
        "holiday_type": Column(
            str,
            Check.isin(["national", "regional", "optional"]),
            nullable=False,
        ),
    },
    # Mỗi ngày chỉ có tối đa 1 ngày lễ (đã xác nhận Brazil không có same-day
    # multiple holidays) -> date phải unique trong phạm vi 1 country_code.
    checks=Check(
        lambda df: ~df.duplicated(subset=["date", "country_code"]).any(),
        error="Duplicate holiday found for the same date and country_code",
    ),
    strict=True,
    coerce=True,
)
 
 
SCHEMAS = {
    "holidays": holidays_schema,
}
 