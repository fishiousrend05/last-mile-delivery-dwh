"""
ingestion/generators/synthetic_generator.py — Sinh 2 bảng SYNTHETIC:
    - drivers.csv            (pool tài xế độc lập)
    - delivery_attempts.csv  (mô phỏng chuỗi lần giao hàng, NEO vào Olist thật)

Hợp đồng giống các extractor khác (dùng chung ExtractionResult):
    generate_drivers()/generate_delivery_attempts() -> ExtractionResult
    (pd.DataFrame THÔ + metadata) -> Validator (synthetic_schemas.py) -> Loader

=== TRIẾT LÝ MÔ PHỎNG delivery_attempts (đọc kỹ trước khi sửa) ===

1. Ranh giới last-mile đã chốt với GVHD: order_delivered_carrier_date ->
   order_delivered_customer_date. Đơn CHƯA từng rời kho
   (order_delivered_carrier_date rỗng) KHÔNG được sinh attempt nào — đơn đó
   chưa từng bước vào giai đoạn last-mile.

2. 3 sự thật từ Olist KHÔNG ĐƯỢC VI PHẠM khi mô phỏng:
   a. order_status — chỉ 'delivered' mới có attempt cuối thành công.
   b. order_delivered_customer_date — attempt cuối PHẢI khớp đúng timestamp này.
   c. sla_delay_days (delivered - estimated) — tín hiệu chính quyết định số
      lần fail trước đó.

3. Risk factor PHẢI neo vào dữ liệu REAL/DERIVED đã có, KHÔNG random thuần:
   - weather severity thật tại zone/ngày (heuristic RIÊNG cho simulation —
     xem _weather_is_severe(), KHÔNG phải weather_severity_bucket chính thức
     của dim_weather, cái đó tính trong dbt).
   - holiday_impact_tier / commercial_event_tier thật tại ngày attempt.
   - zone_risk: vì CHƯA có lịch sử fail thật để suy ra rủi ro theo zone
     (đang tạo ra chính dữ liệu đó), zone_risk là 1 giá trị ngẫu nhiên nhưng
     CỐ ĐỊNH theo từng zone (gán 1 lần, seed cố định) — không random lại mỗi
     đơn trong cùng zone, để nhất quán "zone X vốn khó giao hơn" xuyên suốt.

4. failed_reason_id chọn theo 3 nhóm trọng số (khách hàng 70% / vận hành 25%
   / ngoại cảnh 5%). Trong nhóm ngoại cảnh, lý do "thời tiết cực đoan" (FR09)
   CHỈ được chọn nếu weather heuristic tại đúng zone/ngày đó thật sự xấu;
   nếu không, nhóm ngoại cảnh chỉ còn "khu vực phong tỏa" (FR08).

5. Ngày attempt KHÔNG được lùi về trước order_delivered_carrier_date (không
   thể giao hàng trước khi kiện hàng rời kho).

6. driver_id gán cho mỗi attempt PHẢI: cùng zone_id với order, và đã được
   tuyển (hire_date <= ngày attempt) + status='active' tại thời điểm đó.
"""

from __future__ import annotations

from datetime import date as date_type
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

from ingestion.extractors.base import ExtractionResult
from ingestion.utils.geo import latlng_to_h3
from ingestion.utils.logger import get_logger
from ingestion.validators.file_validator import validate_file

logger = get_logger(__name__)

DEFAULT_N_DRIVERS = 200
DEFAULT_SEED = 42

VEHICLE_TYPES = ["motorcycle", "car", "van", "bicycle"]
VEHICLE_TYPE_WEIGHTS = [0.60, 0.25, 0.10, 0.05]

# Tài xế được tuyển TRƯỚC khi dữ liệu Olist bắt đầu (~2016-09) — đảm bảo mọi
# zone đều có tài xế active ngay từ ngày đầu tiên của dữ liệu, tránh tình
# huống "không tìm được tài xế hợp lệ" khi mô phỏng attempt.
HIRE_DATE_START = pd.Timestamp("2015-06-01")
HIRE_DATE_END = pd.Timestamp("2016-08-31")

# Danh mục failed_reason — theo domain research của người dùng, nhóm theo
# tần suất thực tế trong ngành last-mile.
FAILED_REASONS = [
    {"failed_reason_id": "FR01", "category": "customer", "reason_name": "Khách hàng vắng nhà / không có mặt tại địa chỉ"},
    {"failed_reason_id": "FR02", "category": "customer", "reason_name": "Không liên lạc được (thuê bao, không nghe máy)"},
    {"failed_reason_id": "FR03", "category": "customer", "reason_name": "Sai địa chỉ hoặc địa chỉ không tồn tại"},
    {"failed_reason_id": "FR04", "category": "customer", "reason_name": "Khách hàng đổi ý, từ chối nhận hàng"},
    {"failed_reason_id": "FR05", "category": "operations", "reason_name": "Quá tải tuyến đường / không kịp thời gian giao trong ngày"},
    {"failed_reason_id": "FR06", "category": "operations", "reason_name": "Hàng hóa bị hư hỏng hoặc thất lạc trong quá trình vận chuyển"},
    {"failed_reason_id": "FR07", "category": "operations", "reason_name": "Xe tải / xe máy gặp sự cố dọc đường"},
    {"failed_reason_id": "FR08", "category": "external", "reason_name": "Khu vực bị phong tỏa / không có đường vào"},
    {"failed_reason_id": "FR09", "category": "external", "reason_name": "Thời tiết quá cực đoan (ngập lụt, bão)"},
]
CATEGORY_WEIGHTS = {"customer": 0.70, "operations": 0.25, "external": 0.05}
_CUSTOMER_REASONS = [r["failed_reason_id"] for r in FAILED_REASONS if r["category"] == "customer"]
_OPERATIONS_REASONS = [r["failed_reason_id"] for r in FAILED_REASONS if r["category"] == "operations"]

# Ngưỡng "thời tiết xấu" CHỈ dùng nội bộ cho simulation này — KHÔNG phải
# weather_severity_bucket chính thức của dim_weather (cái đó tính trong dbt,
# có thể dùng nhiều ngưỡng/tiêu chí hơn). Mưa >20mm/ngày là ngưỡng phổ biến
# cho "mưa lớn" trong khí tượng học, phù hợp bối cảnh last-mile Brazil.
SIMULATION_HEAVY_RAIN_MM = 20.0

# Tier nào làm TĂNG rủi ro fail (quá tải vận hành do nhu cầu tăng vọt) — tier
# giảm (Mild_Drop/Severe_Drop) KHÔNG làm giảm rủi ro dưới baseline, chỉ neutral.
TIER_RISK_BONUS = {
    "Tier_1_Mega_Boost": 0.30,
    "Tier_2_High_Boost": 0.15,
}

DEFAULT_ZONE_CENTROIDS_PATH = Path("data/raw/synthetic/zone_centroids.csv")
DEFAULT_GEOLOCATION_PATH = Path("data/source/olist/olist_geolocation_dataset.csv")
DEFAULT_ORDERS_PATH = Path("data/source/olist/olist_orders_dataset.csv")
DEFAULT_CUSTOMERS_PATH = Path("data/source/olist/olist_customers_dataset.csv")
DEFAULT_HOLIDAYS_SEED_PATH = Path("dbt/seeds/holidays.csv")
DEFAULT_COMMERCIAL_EVENTS_SEED_PATH = Path("dbt/seeds/commercial_events.csv")


# ---------------------------------------------------------------------------
# drivers.csv
# ---------------------------------------------------------------------------
def generate_drivers(
    zone_centroids_df: pd.DataFrame,
    n_drivers: int = DEFAULT_N_DRIVERS,
    seed: int = DEFAULT_SEED,
) -> ExtractionResult:
    """
    Sinh pool tài xế — zone gán theo trọng số point_count (zone nhiều hoạt
    động hơn có nhiều tài xế hơn), vehicle_type theo phân phối thực tế Brazil
    (xe máy phổ biến nhất). hire_date luôn TRƯỚC khi dữ liệu Olist bắt đầu.
    """
    rng = np.random.default_rng(seed)
    Faker.seed(seed)
    fake = Faker("pt_BR")

    zone_ids = zone_centroids_df["zone_id"].to_numpy()
    weights = zone_centroids_df["point_count"].to_numpy(dtype=float)
    weights = weights / weights.sum()

    assigned_zones = rng.choice(zone_ids, size=n_drivers, p=weights)
    vehicle_types = rng.choice(VEHICLE_TYPES, size=n_drivers, p=VEHICLE_TYPE_WEIGHTS)
    statuses = rng.choice(["active", "inactive"], size=n_drivers, p=[0.9, 0.1])

    hire_range_days = (HIRE_DATE_END - HIRE_DATE_START).days
    hire_offsets = rng.integers(0, hire_range_days, size=n_drivers)

    records = []
    for i in range(n_drivers):
        records.append(
            {
                "driver_id": f"DRV{i + 1:04d}",
                "full_name": fake.name(),
                "vehicle_type": vehicle_types[i],
                "zone_id": assigned_zones[i],
                "hire_date": HIRE_DATE_START + pd.Timedelta(days=int(hire_offsets[i])),
                "status": statuses[i],
            }
        )
    df = pd.DataFrame(records)

    metadata = {
        "source_name": "synthetic",
        "table_name": "drivers",
        "n_drivers": n_drivers,
        "seed": seed,
        "n_zones_covered": df["zone_id"].nunique(),
    }
    logger.info(f"Generated {len(df)} driver(s) across {metadata['n_zones_covered']} zone(s)")
    return ExtractionResult(table_name="drivers", df=df, metadata=metadata)


# ---------------------------------------------------------------------------
# Order -> zone_id mapping (dependency còn thiếu, lấp ở đây)
# ---------------------------------------------------------------------------
def build_zip_to_zone_map(geolocation_df: pd.DataFrame) -> pd.DataFrame:
    """
    zip_code_prefix -> zone_id, qua toạ độ trung bình của mọi điểm geolocation
    thuộc prefix đó, rồi map sang H3 res5 — CÙNG resolution với
    zone_centroids.csv nên zone_id tính ra khớp thẳng vào bảng đó, không cần
    nearest-neighbor search.
    """
    avg_coords = (
        geolocation_df.groupby("geolocation_zip_code_prefix")[
            ["geolocation_lat", "geolocation_lng"]
        ]
        .mean()
        .reset_index()
    )
    avg_coords["zone_id"] = avg_coords.apply(
        lambda r: latlng_to_h3(r["geolocation_lat"], r["geolocation_lng"]), axis=1
    )
    return avg_coords[["geolocation_zip_code_prefix", "zone_id"]].rename(
        columns={"geolocation_zip_code_prefix": "customer_zip_code_prefix"}
    )


def map_orders_to_zone(
    orders_df: pd.DataFrame, customers_df: pd.DataFrame, zip_to_zone_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Gắn zone_id cho từng order qua customer_id -> customer_zip_code_prefix ->
    zone_id. Order không map được (thiếu geolocation cho zip đó — đã biết có
    ~24-33% gap coverage từ audit trước) bị LOẠI khỏi mô phỏng, không đoán mò.
    """
    merged = orders_df.merge(
        customers_df[["customer_id", "customer_zip_code_prefix"]], on="customer_id", how="left"
    )
    merged = merged.merge(zip_to_zone_df, on="customer_zip_code_prefix", how="left")

    unmapped = merged["zone_id"].isna().sum()
    if unmapped:
        logger.warning(
            f"{unmapped:,} order(s) could not be mapped to a zone "
            f"(missing zip/geolocation match) — excluded from attempt simulation"
        )
    return merged[merged["zone_id"].notna()].copy()


# ---------------------------------------------------------------------------
# Risk factor lookups — neo vào dữ liệu REAL/DERIVED đã có
# ---------------------------------------------------------------------------
def _build_weather_severity_lookup(weather_df: pd.DataFrame | None) -> dict:
    """(zone_id, date) -> bool thời tiết xấu. Trả về {} nếu không có weather_df."""
    if weather_df is None or weather_df.empty:
        logger.warning(
            "No weather data provided — weather-related risk/failed_reason disabled "
            "for this run (all is_weather_severe=False)"
        )
        return {}
    lookup = {}
    for _, row in weather_df.iterrows():
        key = (row["zone_id"], pd.Timestamp(row["date"]).normalize())
        lookup[key] = row["precipitation_mm"] > SIMULATION_HEAVY_RAIN_MM
    return lookup


def _build_date_tier_lookup(seed_path: Path, tier_col: str) -> dict:
    """date -> tier, đọc từ dbt/seeds/holidays.csv hoặc commercial_events.csv."""
    if not seed_path.exists():
        logger.warning(f"{seed_path} not found — event-based risk disabled for this run")
        return {}
    validated_path = validate_file(seed_path, expected_extension=".csv")
    df = pd.read_csv(validated_path)
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return dict(zip(df["date"], df[tier_col]))


def _event_risk_bonus(dt: pd.Timestamp, holiday_lookup: dict, event_lookup: dict) -> float:
    dt = pd.Timestamp(dt).normalize()
    tier = holiday_lookup.get(dt) or event_lookup.get(dt)
    return TIER_RISK_BONUS.get(tier, 0.0)


def _build_zone_risk_map(zone_ids: pd.Series, seed: int) -> dict:
    """
    Rủi ro CỐ ĐỊNH theo từng zone (không random lại mỗi đơn) — vì chưa có
    lịch sử fail thật để suy ra, chỉ có thể giả định "1 số zone vốn khó giao
    hơn" và giữ nhất quán xuyên suốt toàn bộ mô phỏng.
    """
    rng = np.random.default_rng(seed + 1)  # seed khác zone driver assignment
    unique_zones = zone_ids.dropna().unique()
    return {z: rng.uniform(0.0, 0.3) for z in unique_zones}


def _pick_failed_reason(rng: np.random.Generator, is_weather_severe: bool) -> str:
    category = rng.choice(["customer", "operations", "external"], p=[0.70, 0.25, 0.05])
    if category == "customer":
        return rng.choice(_CUSTOMER_REASONS)
    if category == "operations":
        return rng.choice(_OPERATIONS_REASONS)
    # external: "thời tiết cực đoan" (FR09) CHỈ chọn khi weather thật xấu;
    # nếu không, chỉ còn "khu vực phong tỏa" (FR08).
    return rng.choice(["FR08", "FR09"]) if is_weather_severe else "FR08"


def _build_driver_lookup(drivers_df: pd.DataFrame) -> dict:
    """zone_id -> list[(driver_id, hire_date)] cho tài xế active — dùng để chọn nhanh khi mô phỏng."""
    active = drivers_df[drivers_df["status"] == "active"]
    lookup: dict = {}
    for zone_id, group in active.groupby("zone_id"):
        lookup[zone_id] = list(zip(group["driver_id"], group["hire_date"]))
    return lookup


def _pick_driver(
    zone_id: str, attempt_date: pd.Timestamp, driver_lookup: dict, rng: np.random.Generator
) -> str | None:
    """
    Chọn tài xế active CÙNG zone, đã hire_date <= attempt_date. Fallback sang
    pool active toàn cục nếu zone đó không có tài xế hợp lệ tại thời điểm đó
    (hiếm — zone nhỏ có thể được gán 0 tài xế do trọng số point_count thấp).
    """
    candidates = [d for d, hd in driver_lookup.get(zone_id, []) if hd <= attempt_date]
    if candidates:
        return rng.choice(candidates)

    all_candidates = [
        d for zid, drivers in driver_lookup.items() for d, hd in drivers if hd <= attempt_date
    ]
    if all_candidates:
        return rng.choice(all_candidates)
    return None  # không còn tài xế nào active tại thời điểm này (rất hiếm, chỉ đầu kỳ)


# ---------------------------------------------------------------------------
# delivery_attempts.csv — mô phỏng chính
# ---------------------------------------------------------------------------
def _simulate_order_attempts(
    order_row: pd.Series,
    zone_risk_map: dict,
    weather_lookup: dict,
    holiday_lookup: dict,
    event_lookup: dict,
    driver_lookup: dict,
    rng: np.random.Generator,
) -> list[dict]:
    order_id = order_row["order_id"]
    zone_id = order_row["zone_id"]
    carrier_date = order_row["order_delivered_carrier_date"]

    # Quy tắc #1: chưa rời kho -> chưa từng bước vào last-mile -> 0 attempt.
    if pd.isna(carrier_date):
        return []

    zone_risk = zone_risk_map.get(zone_id, 0.1)
    attempts: list[dict] = []

    # --- Case 1: đã rời kho nhưng KHÔNG giao thành công ---
    if order_row["order_status"] != "delivered" or pd.isna(order_row["order_delivered_customer_date"]):
        num_fails = int(rng.choice([1, 2], p=[0.7, 0.3]))
        current_date = carrier_date + pd.Timedelta(days=1)
        for i in range(num_fails):
            if i > 0:
                current_date += pd.Timedelta(days=int(rng.integers(1, 3)))
            is_severe = weather_lookup.get((zone_id, current_date.normalize()), False)
            attempts.append(
                {
                    "attempt_id": f"{order_id}_{i + 1}",
                    "order_id": order_id,
                    "driver_id": _pick_driver(zone_id, current_date, driver_lookup, rng),
                    "zone_id": zone_id,
                    "attempt_number": i + 1,
                    "attempt_timestamp": current_date,
                    "attempt_status": "failed",
                    "failed_reason_id": _pick_failed_reason(rng, is_severe),
                }
            )
        return attempts

    # --- Case 2: delivered — sự thật tuyệt đối cần neo vào ---
    delivered_date = order_row["order_delivered_customer_date"]
    sla_delay = order_row["sla_delay_days"]

    is_severe_at_delivery = weather_lookup.get((zone_id, delivered_date.normalize()), False)
    weather_risk = 0.2 if is_severe_at_delivery else 0.0
    holiday_risk = _event_risk_bonus(delivered_date, holiday_lookup, event_lookup)

    base_fail_prob = min(0.05 + (sla_delay * 0.15) + zone_risk + weather_risk + holiday_risk, 0.85)

    dates = [delivered_date]
    current = delivered_date
    max_lookback = int(sla_delay) + 2

    for _ in range(max_lookback):
        if rng.random() >= base_fail_prob:
            break
        days_back = int(rng.integers(1, 3))
        candidate = current - pd.Timedelta(days=days_back)
        # Quy tắc #5: không được lùi về trước ngày rời kho.
        if candidate <= carrier_date:
            break
        current = candidate
        dates.insert(0, current)
        base_fail_prob *= 0.6  # rủi ro giảm dần khi lùi xa hơn (heuristic mô phỏng)

    total = len(dates)
    for idx, dt in enumerate(dates):
        is_final = idx == total - 1
        is_severe = weather_lookup.get((zone_id, dt.normalize()), False)
        attempts.append(
            {
                "attempt_id": f"{order_id}_{idx + 1}",
                "order_id": order_id,
                "driver_id": _pick_driver(zone_id, dt, driver_lookup, rng),
                "zone_id": zone_id,
                "attempt_number": idx + 1,
                "attempt_timestamp": dt,
                "attempt_status": "success" if is_final else "failed",
                "failed_reason_id": None if is_final else _pick_failed_reason(rng, is_severe),
            }
        )
    return attempts


def generate_delivery_attempts(
    orders_zoned_df: pd.DataFrame,
    drivers_df: pd.DataFrame,
    weather_df: pd.DataFrame | None = None,
    holidays_seed_path: Path = DEFAULT_HOLIDAYS_SEED_PATH,
    commercial_events_seed_path: Path = DEFAULT_COMMERCIAL_EVENTS_SEED_PATH,
    seed: int = DEFAULT_SEED,
) -> ExtractionResult:
    """
    Mô phỏng delivery_attempts cho từng order đã có zone_id (xem
    map_orders_to_zone). Xem docstring đầu file để hiểu đầy đủ triết lý neo
    vào Olist thật — hàm này CHỈ điều phối, logic mô phỏng nằm ở
    _simulate_order_attempts().
    """
    df = orders_zoned_df.copy()
    df["order_delivered_carrier_date"] = pd.to_datetime(df["order_delivered_carrier_date"])
    df["order_delivered_customer_date"] = pd.to_datetime(df["order_delivered_customer_date"])
    df["order_estimated_delivery_date"] = pd.to_datetime(df["order_estimated_delivery_date"])
    df["sla_delay_days"] = (
        (df["order_delivered_customer_date"] - df["order_estimated_delivery_date"]).dt.days
    ).clip(lower=0).fillna(0)

    weather_lookup = _build_weather_severity_lookup(weather_df)
    holiday_lookup = _build_date_tier_lookup(holidays_seed_path, "holiday_impact_tier")
    event_lookup = _build_date_tier_lookup(commercial_events_seed_path, "commercial_event_tier")
    zone_risk_map = _build_zone_risk_map(df["zone_id"], seed)
    driver_lookup = _build_driver_lookup(drivers_df)

    rng = np.random.default_rng(seed)
    all_attempts: list[dict] = []
    n_no_carrier = 0
    for _, row in df.iterrows():
        if pd.isna(row["order_delivered_carrier_date"]):
            n_no_carrier += 1
            continue
        all_attempts.extend(
            _simulate_order_attempts(
                row, zone_risk_map, weather_lookup, holiday_lookup, event_lookup, driver_lookup, rng
            )
        )

    result_df = pd.DataFrame(all_attempts)

    metadata = {
        "source_name": "synthetic",
        "table_name": "delivery_attempts",
        "n_orders_input": len(df),
        "n_orders_excluded_no_carrier_date": n_no_carrier,
        "n_attempts_generated": len(result_df),
        "seed": seed,
    }
    logger.info(
        f"Generated {len(result_df):,} attempt(s) for "
        f"{len(df) - n_no_carrier:,}/{len(df):,} order(s) "
        f"({n_no_carrier:,} excluded — chưa rời kho, ngoài phạm vi last-mile)"
    )
    return ExtractionResult(table_name="delivery_attempts", df=result_df, metadata=metadata)


# ---------------------------------------------------------------------------
# Orchestration — đọc toàn bộ input từ path mặc định, sinh cả 2 bảng
# ---------------------------------------------------------------------------
def main(
    zone_centroids_path: Path = DEFAULT_ZONE_CENTROIDS_PATH,
    geolocation_path: Path = DEFAULT_GEOLOCATION_PATH,
    orders_path: Path = DEFAULT_ORDERS_PATH,
    customers_path: Path = DEFAULT_CUSTOMERS_PATH,
    holidays_seed_path: Path = DEFAULT_HOLIDAYS_SEED_PATH,
    commercial_events_seed_path: Path = DEFAULT_COMMERCIAL_EVENTS_SEED_PATH,
    geolocation_df: pd.DataFrame | None = None,
    orders_df: pd.DataFrame | None = None,
    customers_df: pd.DataFrame | None = None,
    weather_df: pd.DataFrame | None = None,
    n_drivers: int = DEFAULT_N_DRIVERS,
    seed: int = DEFAULT_SEED,
) -> tuple[ExtractionResult, ExtractionResult]:
    """
    Chạy toàn bộ luồng: đọc input -> generate_drivers() -> map order sang
    zone -> generate_delivery_attempts(). Trả về 2 ExtractionResult (drivers,
    delivery_attempts) — Loader nhận thẳng .df của từng cái để ghi Postgres,
    không cần gọi lại pipeline nội bộ này.

    geolocation_df/orders_df/customers_df: nếu được TRUYỀN VÀO (vd từ
    ingestion_flow.py — bản ĐÃ QUA schema_validator ở bước ingest_olist),
    dùng trực tiếp thay vì tự đọc lại CSV thô. Nếu để None (gọi độc lập
    ngoài flow, vd lúc dev/test), tự đọc từ path mặc định như trước — giữ
    hàm này chạy độc lập được, không bắt buộc phải có flow.

    weather_df KHÔNG có path mặc định (khác holidays/commercial_events) vì
    weather_extractor.py hiện CHƯA ghi ra file CSV trung gian — cần truyền
    trực tiếp DataFrame đã extract, hoặc để None nếu chưa sẵn sàng (weather
    risk sẽ tự tắt, có log cảnh báo, KHÔNG raise lỗi).
    """
    zone_centroids_df = pd.read_csv(validate_file(zone_centroids_path, ".csv"))

    if geolocation_df is None:
        geolocation_df = pd.read_csv(validate_file(geolocation_path, ".csv"))
    if orders_df is None:
        orders_df = pd.read_csv(validate_file(orders_path, ".csv"))
    if customers_df is None:
        customers_df = pd.read_csv(validate_file(customers_path, ".csv"))

    drivers_result = generate_drivers(zone_centroids_df, n_drivers=n_drivers, seed=seed)

    zip_to_zone_df = build_zip_to_zone_map(geolocation_df)
    orders_zoned_df = map_orders_to_zone(orders_df, customers_df, zip_to_zone_df)

    attempts_result = generate_delivery_attempts(
        orders_zoned_df,
        drivers_result.df,
        weather_df=weather_df,
        holidays_seed_path=holidays_seed_path,
        commercial_events_seed_path=commercial_events_seed_path,
        seed=seed,
    )

    return drivers_result, attempts_result


if __name__ == "__main__":
    main()