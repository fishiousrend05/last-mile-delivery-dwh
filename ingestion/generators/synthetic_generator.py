"""
ingestion/generators/synthetic_generator.py — Sinh 2 bảng SYNTHETIC:
    - drivers.csv            (pool tài xế độc lập)
    - delivery_attempts.csv  (mô phỏng chuỗi lần giao hàng, NEO vào Olist thật)

Hợp đồng giống các extractor khác (dùng chung ExtractionResult):
    generate_drivers()/generate_delivery_attempts() -> ExtractionResult
    (pd.DataFrame THÔ + metadata) -> Validator (synthetic_schemas.py) -> Loader

=== TRIẾT LÝ MÔ PHỎNG delivery_attempts ===

1. Ranh giới last-mile đã chốt với GVHD: order_delivered_carrier_date ->
   order_delivered_customer_date. Đơn CHƯA từng rời kho
   (order_delivered_carrier_date rỗng) KHÔNG được sinh attempt nào — đơn đó
   chưa từng bước vào giai đoạn last-mile.

2. 3 sự thật từ Olist KHÔNG ĐƯỢC VI PHẠM khi mô phỏng:
   a. order_status — chỉ 'delivered' mới có attempt cuối thành công.
   b. order_delivered_customer_date — attempt cuối PHẢI khớp đúng timestamp này.
   c. estimated_delivery_delay_days (delivered - estimated, cắt dưới 0) — tín
      hiệu chính quyết định số lần fail trước đó. (Tên cũ: sla_delay_days —
      đã đổi để nhất quán với thuật ngữ "Estimated Delivery" của đề tài, vì
      estimated_delivery_date của Olist KHÔNG phải SLA hợp đồng chính thức.)

3. Risk factor PHẢI neo vào dữ liệu REAL/DERIVED đã có, KHÔNG random thuần:
   - weather severity thật tại zone/ngày (heuristic RIÊNG cho simulation —
     xem SIMULATION_HEAVY_RAIN_MM, KHÔNG phải weather_severity_bucket chính
     thức của dim_weather, cái đó tính trong dbt).
   - holiday_impact_tier / commercial_event_tier thật tại ngày attempt. Khi 1
     ngày vừa có commercial event vừa có public holiday thì Commercial Event
     được ưu tiên (đúng quyết định đã chốt ở Phase 1).
   - zone_risk: vì CHƯA có lịch sử fail thật để suy ra rủi ro theo zone
     (đang tạo ra chính dữ liệu đó), zone_risk là 1 giá trị ngẫu nhiên nhưng
     CỐ ĐỊNH theo từng zone (gán 1 lần, seed cố định) — không random lại mỗi
     đơn trong cùng zone, để nhất quán "zone X vốn khó giao hơn" xuyên suốt.

4. failed_reason_id chọn theo 3 nhóm trọng số (CATEGORY_WEIGHTS: khách hàng
   70% / vận hành 25% / ngoại cảnh 5%). Trong nhóm ngoại cảnh, lý do "thời
   tiết cực đoan" (FR09) CHỈ được chọn nếu weather heuristic tại đúng
   zone/ngày đó thật sự xấu; nếu không, nhóm ngoại cảnh chỉ còn "khu vực phong
   tỏa" (FR08).

5. Ngày attempt KHÔNG được lùi về trước order_delivered_carrier_date (không
   thể giao hàng trước khi kiện hàng rời kho).

6. driver_id gán cho mỗi attempt PHẢI đã được tuyển (hire_date <= ngày
   attempt) + status='active'. Ưu tiên chọn theo 3 bậc:
       (1) tài xế CÙNG zone_id với order,
       (2) tài xế CÙNG bang (dominant_state của zone),
       (3) tài xế bất kỳ (fallback toàn cục).
   Chỉ có ~180 tài xế active cho ~6.850 zone nên bậc (1) KHÔNG phủ hết mọi
   attempt — số attempt rơi vào từng bậc được ghi vào metadata
   ["driver_assignment"] để tài liệu hóa đúng giới hạn này. Tài xế được chọn
   NGẪU NHIÊN trong pool hợp lệ, không có tham số "tay nghề" — nên chênh lệch
   hiệu suất giữa các tài xế ở phía dbt chỉ phản ánh cơ cấu zone/thời tiết
   mà họ được gán, KHÔNG phải chất lượng tài xế.

=== KIỂM TRA TỰ ĐỘNG KHI SINH DỮ LIỆU ===

generate_delivery_attempts() gọi validate_delivery_attempts() ngay trước khi
trả kết quả. Nếu bảng sinh ra vi phạm bất kỳ quy tắc #1-#6 ở trên (hoặc các
bất biến cấu trúc: attempt_id duy nhất, attempt_number liên tục từ 1, chỉ
attempt cuối mới được success, ...) thì RAISE ValueError thay vì âm thầm ghi
dữ liệu sai xuống landing. Toàn bộ tham số mô phỏng được ghi vào metadata
["simulation_params"] để audit log lưu lại đúng bộ tham số của từng lần chạy.
"""

from __future__ import annotations

from collections import Counter
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
# tần suất thực tế trong ngành last-mile. PHẢI khớp dbt/seeds/failed_reasons.csv
# (có test trong ingestion/tests/test_synthetic_generator_v2.py canh lệch).
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
REASON_BLOCKED_AREA = "FR08"
REASON_EXTREME_WEATHER = "FR09"
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

# Hệ số mô phỏng — gom một chỗ để (1) sửa ở 1 nơi, (2) ghi vào metadata/audit
# log, (3) đối chiếu với kết quả truy vấn ở phía dbt (synthetic_fidelity_checks).
SIM_BASE_FAIL_PROB = 0.05  # xác suất nền phải giao lại
SIM_DELAY_RISK_PER_DAY = 0.15  # cộng thêm cho mỗi ngày giao trễ so với ước tính
SIM_WEATHER_RISK = 0.20  # cộng thêm nếu ngày giao thành công có mưa lớn
SIM_MAX_FAIL_PROB = 0.85  # trần xác suất
SIM_LOOKBACK_DECAY = 0.60  # nhân xác suất mỗi khi lùi thêm 1 lần fail
SIM_ZONE_RISK_MAX = 0.30  # zone_risk ~ Uniform(0, SIM_ZONE_RISK_MAX)
SIM_DEFAULT_ZONE_RISK = 0.10  # zone không có trong zone_risk_map
SIM_UNDELIVERED_FAIL_COUNTS = (1, 2)  # đơn rời kho nhưng không giao được: số lần fail
SIM_UNDELIVERED_FAIL_PROBS = (0.7, 0.3)
SIM_RETRY_GAP_DAYS = (1, 3)  # khoảng cách giữa 2 lần thử: rng.integers(1, 3) -> 1 hoặc 2 ngày

DRIVER_TIER_ZONE = "zone"
DRIVER_TIER_STATE = "state"
DRIVER_TIER_GLOBAL = "global"
DRIVER_TIER_NONE = "none"

ATTEMPT_COLUMNS = [
    "attempt_id",
    "order_id",
    "driver_id",
    "zone_id",
    "attempt_number",
    "attempt_timestamp",
    "attempt_status",
    "failed_reason_id",
]

DEFAULT_ZONE_CENTROIDS_PATH = Path("dbt/seeds/zone_centroids.csv")
# zone_centroids.csv từng nằm ở data/raw/synthetic/ trước khi chuyển vào dbt seeds;
# chỉ dùng làm fallback KHI người gọi không truyền path và seed dbt không tồn tại.
LEGACY_ZONE_CENTROIDS_PATH = Path("data/raw/synthetic/zone_centroids.csv")
DEFAULT_ZIP_ZONE_MAPPING_PATH = Path("dbt/seeds/zip_zone_mapping.csv")
DEFAULT_GEOLOCATION_PATH = Path("data/source/olist/olist_geolocation_dataset.csv")  # DEPRECATED
DEFAULT_ORDERS_PATH = Path("data/source/olist/olist_orders_dataset.csv")
DEFAULT_CUSTOMERS_PATH = Path("data/source/olist/olist_customers_dataset.csv")
DEFAULT_HOLIDAYS_SEED_PATH = Path("dbt/seeds/holidays.csv")
DEFAULT_COMMERCIAL_EVENTS_SEED_PATH = Path("dbt/seeds/commercial_events.csv")


def _check_constants() -> None:
    """Fail-fast khi ai đó sửa hằng số làm danh mục/trọng số mâu thuẫn nhau."""
    if abs(sum(CATEGORY_WEIGHTS.values()) - 1.0) > 1e-9:
        raise ValueError(f"CATEGORY_WEIGHTS must sum to 1.0, got {sum(CATEGORY_WEIGHTS.values())}")
    if abs(sum(SIM_UNDELIVERED_FAIL_PROBS) - 1.0) > 1e-9:
        raise ValueError("SIM_UNDELIVERED_FAIL_PROBS must sum to 1.0")
    category_by_id = {r["failed_reason_id"]: r["category"] for r in FAILED_REASONS}
    if len(category_by_id) != len(FAILED_REASONS):
        raise ValueError("FAILED_REASONS contains duplicate failed_reason_id")
    if set(CATEGORY_WEIGHTS) != set(category_by_id.values()):
        raise ValueError("CATEGORY_WEIGHTS keys must equal the set of FAILED_REASONS categories")
    for reason_id in (REASON_BLOCKED_AREA, REASON_EXTREME_WEATHER):
        if category_by_id.get(reason_id) != "external":
            raise ValueError(f"{reason_id} must exist in FAILED_REASONS with category 'external'")


_check_constants()


def get_simulation_params() -> dict:
    """Bộ tham số mô phỏng (JSON-safe) — ghi vào metadata để audit log lưu lại."""
    return {
        "base_fail_prob": SIM_BASE_FAIL_PROB,
        "delay_risk_per_day": SIM_DELAY_RISK_PER_DAY,
        "weather_risk": SIM_WEATHER_RISK,
        "max_fail_prob": SIM_MAX_FAIL_PROB,
        "lookback_decay": SIM_LOOKBACK_DECAY,
        "zone_risk_max": SIM_ZONE_RISK_MAX,
        "default_zone_risk": SIM_DEFAULT_ZONE_RISK,
        "heavy_rain_mm": SIMULATION_HEAVY_RAIN_MM,
        "tier_risk_bonus": dict(TIER_RISK_BONUS),
        "category_weights": dict(CATEGORY_WEIGHTS),
        "undelivered_fail_counts": list(SIM_UNDELIVERED_FAIL_COUNTS),
        "undelivered_fail_probs": list(SIM_UNDELIVERED_FAIL_PROBS),
        "retry_gap_days_range": [SIM_RETRY_GAP_DAYS[0], SIM_RETRY_GAP_DAYS[1] - 1],
    }


# ---------------------------------------------------------------------------
# Helpers dùng chung
# ---------------------------------------------------------------------------
def _to_naive_datetime(series: pd.Series) -> pd.Series:
    """to_datetime + bỏ timezone (nếu có) — lookup weather/holiday dùng key tz-naive."""
    s = pd.to_datetime(series)
    if getattr(s.dt, "tz", None) is not None:
        s = s.dt.tz_localize(None)
    return s


def _normalize_zip(series: pd.Series) -> pd.Series:
    """
    zip_code_prefix -> chuỗi 5 ký tự, giữ số 0 đầu. Phase 2 từng gặp lỗi mất số 0
    đầu (1001 thay vì 01001) khi CSV bị đọc thành số; áp dụng CÙNG hàm này cho cả
    2 phía join để không bao giờ lệch kiểu/độ dài giữa Olist và zip_zone_mapping.
    """
    s = series.astype("string").str.strip()
    s = s.str.replace(r"\.0$", "", regex=True)  # vết của float-cast ('1001.0')
    s = s.str.zfill(5)
    return s.astype(object).where(s.notna(), np.nan)


def _first_existing(*paths: Path) -> Path:
    for p in paths:
        if Path(p).exists():
            return Path(p)
    return Path(paths[0])


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

    Lưu ý: status ('active'/'inactive') là TĨNH — tài xế inactive không có bản
    ghi terminated_date nên không bao giờ được gán attempt nào trong mô phỏng.
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
        "n_zones_covered": int(df["zone_id"].nunique()),
    }
    logger.info(f"Generated {len(df)} driver(s) across {metadata['n_zones_covered']} zone(s)")
    return ExtractionResult(table_name="drivers", df=df, metadata=metadata)


# ---------------------------------------------------------------------------
# Order -> zone_id mapping
# ---------------------------------------------------------------------------
def load_zip_to_zone_map(
    path: Path = DEFAULT_ZIP_ZONE_MAPPING_PATH, known_zone_ids: set | None = None
) -> pd.DataFrame:
    """
    Đọc seed dbt/seeds/zip_zone_mapping.csv (zip_code_prefix -> zone_id, rule
    'mode' trong train_zone_clustering.py) — NGUỒN SỰ THẬT DUY NHẤT cho việc gán
    zone. Nhờ vậy zone_id của attempt luôn khớp customer_zone của order trong
    fact_order_lifecycle (dbt cũng resolve zone qua đúng seed này).

    Trả về DataFrame [customer_zip_code_prefix, zone_id] — cùng shape với
    build_zip_to_zone_map() cũ nên map_orders_to_zone() dùng được nguyên xi.
    RAISE nếu 1 zip bị map sang >1 zone khác nhau (seed hỏng, sẽ gây nhân dòng).
    """
    df = pd.read_csv(validate_file(Path(path), ".csv"), dtype=str)
    missing_cols = {"zip_code_prefix", "zone_id"} - set(df.columns)
    if missing_cols:
        raise ValueError(f"{path} is missing column(s): {sorted(missing_cols)}")

    df = df.dropna(subset=["zip_code_prefix", "zone_id"]).copy()
    df["customer_zip_code_prefix"] = _normalize_zip(df["zip_code_prefix"])
    df = df[["customer_zip_code_prefix", "zone_id"]].drop_duplicates()

    conflicting = df["customer_zip_code_prefix"].duplicated(keep=False)
    if conflicting.any():
        sample = df.loc[conflicting, "customer_zip_code_prefix"].unique()[:5].tolist()
        raise ValueError(
            f"{path}: {int(df.loc[conflicting, 'customer_zip_code_prefix'].nunique())} zip prefix(es) "
            f"map to more than one zone_id (e.g. {sample}) — seed must be 1 zip -> 1 zone"
        )

    if known_zone_ids is not None:
        unknown = ~df["zone_id"].isin(known_zone_ids)
        if unknown.any():
            logger.warning(
                f"{int(unknown.sum()):,} zip prefix(es) in {Path(path).name} map to a zone_id "
                f"missing from zone_centroids — those orders get no state-level driver fallback"
            )
    return df.reset_index(drop=True)


def build_zip_to_zone_map(geolocation_df: pd.DataFrame) -> pd.DataFrame:
    """
    DEPRECATED — không còn được main() gọi. Giữ lại để tương thích test cũ.

    zip_code_prefix -> zone_id qua toạ độ TRUNG BÌNH của mọi điểm geolocation
    thuộc prefix đó, rồi map sang H3 res5. Cách này có thể cho zone KHÁC với seed
    zip_zone_mapping (rule 'mode') khi 1 zip trải >1 H3 cell, gây lệch zone giữa
    attempt và fact_order_lifecycle — dùng load_zip_to_zone_map() thay thế.
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
    zone_id. Order không map được (zip không có trong zip_zone_mapping — đã biết
    ~279 order như vậy trong fact_order_lifecycle) bị LOẠI khỏi mô phỏng, không
    đoán mò.

    zip được chuẩn hóa về 5 ký tự ở cả 2 phía trước khi join, và hàm RAISE nếu
    phép join làm thay đổi số dòng (customer_id trùng hoặc zip map nhiều zone) —
    nhân dòng ở đây sẽ sinh attempt_id trùng ở bước sau.
    """
    n_orders = len(orders_df)

    customers = customers_df[["customer_id", "customer_zip_code_prefix"]].copy()
    customers["customer_zip_code_prefix"] = _normalize_zip(customers["customer_zip_code_prefix"])
    mapping = zip_to_zone_df[["customer_zip_code_prefix", "zone_id"]].copy()
    mapping["customer_zip_code_prefix"] = _normalize_zip(mapping["customer_zip_code_prefix"])

    merged = orders_df.merge(customers, on="customer_id", how="left")
    merged = merged.merge(mapping, on="customer_zip_code_prefix", how="left")

    if len(merged) != n_orders:
        raise ValueError(
            f"map_orders_to_zone changed row count ({n_orders:,} -> {len(merged):,}): "
            f"duplicate customer_id in customers, or a zip prefix mapped to several zones"
        )

    unmapped = int(merged["zone_id"].isna().sum())
    if unmapped:
        logger.warning(
            f"{unmapped:,} order(s) could not be mapped to a zone "
            f"(zip prefix missing from zip_zone_mapping) — excluded from attempt simulation"
        )
    return merged[merged["zone_id"].notna()].copy()


def build_zone_state_map(zone_centroids_df: pd.DataFrame) -> dict:
    """zone_id -> dominant_state (dùng cho bậc fallback theo bang). {} nếu thiếu cột."""
    if "dominant_state" not in zone_centroids_df.columns:
        logger.warning(
            "zone_centroids has no 'dominant_state' column — state-level driver "
            "fallback disabled (falls back straight to the global pool)"
        )
        return {}
    valid = zone_centroids_df.dropna(subset=["zone_id", "dominant_state"])
    return dict(zip(valid["zone_id"], valid["dominant_state"]))


# ---------------------------------------------------------------------------
# Risk factor lookups — neo vào dữ liệu REAL/DERIVED đã có
# ---------------------------------------------------------------------------
def _build_weather_severity_lookup(weather_df: pd.DataFrame | None) -> dict:
    """
    {(zone_id, date): True} CHỈ cho các zone/ngày có mưa > SIMULATION_HEAVY_RAIN_MM.
    Mọi nơi đọc dùng .get(key, False) nên key vắng mặt = thời tiết KHÔNG xấu —
    ngữ nghĩa giống bản cũ nhưng vector hóa (bản cũ iterrows trên ~5,4 triệu dòng
    và giữ cả các key False; bản này chỉ giữ ~vài trăm nghìn key mưa lớn).
    Trả về {} nếu không có weather_df.
    """
    if weather_df is None or weather_df.empty:
        logger.warning(
            "No weather data provided — weather-related risk/failed_reason disabled "
            "for this run (all is_weather_severe=False)"
        )
        return {}
    w = pd.DataFrame(
        {
            "zone_id": weather_df["zone_id"].to_numpy(),
            "date": _to_naive_datetime(weather_df["date"]).dt.normalize().to_numpy(),
            "severe": (
                pd.to_numeric(weather_df["precipitation_mm"], errors="coerce") > SIMULATION_HEAVY_RAIN_MM
            ).to_numpy(),
        }
    )
    # Trùng (zone_id, date): giữ dòng CUỐI — đúng ngữ nghĩa bản cũ (dict ghi đè). Dữ liệu
    # weather thật chỉ có trùng thuần túy (do checkpoint resume) nên không ảnh hưởng kết quả.
    w = w.drop_duplicates(["zone_id", "date"], keep="last")
    severe = w[w["severe"]]
    lookup = dict.fromkeys(zip(severe["zone_id"], pd.to_datetime(severe["date"])), True)
    logger.info(f"Weather lookup: {len(lookup):,} severe (zone, date) pair(s) from {len(weather_df):,} row(s)")
    return lookup


def _build_date_tier_lookup(seed_path: Path, tier_col: str) -> dict:
    """date -> tier, đọc từ dbt/seeds/holidays.csv hoặc commercial_events.csv."""
    if not seed_path.exists():
        logger.warning(f"{seed_path} not found — event-based risk disabled for this run")
        return {}
    validated_path = validate_file(seed_path, expected_extension=".csv")
    df = pd.read_csv(validated_path)
    df = df.dropna(subset=["date", tier_col]).copy()  # NaN là truthy — không để lọt vào lookup
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    return dict(zip(df["date"], df[tier_col]))


def _event_risk_bonus(dt: pd.Timestamp, holiday_lookup: dict, event_lookup: dict) -> float:
    """
    Commercial Event > Public Holiday khi trùng ngày (quyết định đã chốt ở Phase 1).
    Bản cũ đảo ngược thứ tự (holiday trước) nên event luôn bị bỏ qua trên ngày lễ.
    """
    dt = pd.Timestamp(dt).normalize()
    tier = event_lookup.get(dt) or holiday_lookup.get(dt)
    return TIER_RISK_BONUS.get(tier, 0.0)


def _build_zone_risk_map(zone_ids: pd.Series, seed: int) -> dict:
    """
    Rủi ro CỐ ĐỊNH theo từng zone (không random lại mỗi đơn) — vì chưa có
    lịch sử fail thật để suy ra, chỉ có thể giả định "1 số zone vốn khó giao
    hơn" và giữ nhất quán xuyên suốt toàn bộ mô phỏng.
    """
    rng = np.random.default_rng(seed + 1)  # seed khác zone driver assignment
    unique_zones = zone_ids.dropna().unique()
    return {z: rng.uniform(0.0, SIM_ZONE_RISK_MAX) for z in unique_zones}


def _pick_failed_reason(rng: np.random.Generator, is_weather_severe: bool) -> str:
    categories = list(CATEGORY_WEIGHTS)
    category = rng.choice(categories, p=[CATEGORY_WEIGHTS[c] for c in categories])
    if category == "customer":
        return rng.choice(_CUSTOMER_REASONS)
    if category == "operations":
        return rng.choice(_OPERATIONS_REASONS)
    # external: "thời tiết cực đoan" (FR09) CHỈ chọn khi weather thật xấu;
    # nếu không, chỉ còn "khu vực phong tỏa" (FR08).
    if is_weather_severe:
        return rng.choice([REASON_BLOCKED_AREA, REASON_EXTREME_WEATHER])
    return REASON_BLOCKED_AREA


def _build_driver_lookup(drivers_df: pd.DataFrame) -> dict:
    """zone_id -> list[(driver_id, hire_date)] cho tài xế active — dùng để chọn nhanh khi mô phỏng."""
    active = drivers_df[drivers_df["status"] == "active"]
    lookup: dict = {}
    for zone_id, group in active.groupby("zone_id"):
        lookup[zone_id] = list(zip(group["driver_id"], group["hire_date"]))
    return lookup


def _build_state_driver_lookup(drivers_df: pd.DataFrame, zone_state_map: dict | None) -> dict:
    """state -> list[(driver_id, hire_date)] cho tài xế active (bang = dominant_state của home zone)."""
    if not zone_state_map:
        return {}
    active = drivers_df[drivers_df["status"] == "active"]
    lookup: dict = {}
    for driver_id, zone_id, hire_date in zip(active["driver_id"], active["zone_id"], active["hire_date"]):
        state = zone_state_map.get(zone_id)
        if state is None:
            continue
        lookup.setdefault(state, []).append((driver_id, hire_date))
    return lookup


def _pick_driver_tiered(
    zone_id: str,
    attempt_date: pd.Timestamp,
    driver_lookup: dict,
    rng: np.random.Generator,
    state_lookup: dict | None = None,
    zone_state_map: dict | None = None,
) -> tuple[str | None, str]:
    """
    Chọn tài xế active đã hire_date <= attempt_date, theo 3 bậc:
      zone (cùng zone_id) -> state (cùng bang) -> global (bất kỳ).
    Trả về (driver_id, tier). Bậc 'state' chỉ chạy khi truyền cả state_lookup và
    zone_state_map; nếu không, hành vi y hệt bản 2 bậc cũ (zone -> global).
    """
    candidates = [d for d, hd in driver_lookup.get(zone_id, []) if hd <= attempt_date]
    if candidates:
        return rng.choice(candidates), DRIVER_TIER_ZONE

    if state_lookup and zone_state_map:
        state = zone_state_map.get(zone_id)
        if state is not None:
            state_candidates = [d for d, hd in state_lookup.get(state, []) if hd <= attempt_date]
            if state_candidates:
                return rng.choice(state_candidates), DRIVER_TIER_STATE

    all_candidates = [
        d for _zid, drivers in driver_lookup.items() for d, hd in drivers if hd <= attempt_date
    ]
    if all_candidates:
        return rng.choice(all_candidates), DRIVER_TIER_GLOBAL
    return None, DRIVER_TIER_NONE  # không còn tài xế nào active tại thời điểm này (rất hiếm, chỉ đầu kỳ)


def _pick_driver(
    zone_id: str,
    attempt_date: pd.Timestamp,
    driver_lookup: dict,
    rng: np.random.Generator,
    state_lookup: dict | None = None,
    zone_state_map: dict | None = None,
) -> str | None:
    """Wrapper của _pick_driver_tiered() chỉ trả driver_id (tương thích chữ ký cũ)."""
    return _pick_driver_tiered(
        zone_id, attempt_date, driver_lookup, rng, state_lookup, zone_state_map
    )[0]


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
    state_lookup: dict | None = None,
    zone_state_map: dict | None = None,
    tier_counts: Counter | None = None,
) -> list[dict]:
    order_id = order_row["order_id"]
    zone_id = order_row["zone_id"]
    carrier_date = order_row["order_delivered_carrier_date"]

    # Quy tắc #1: chưa rời kho -> chưa từng bước vào last-mile -> 0 attempt.
    if pd.isna(carrier_date):
        return []

    def _driver_for(dt: pd.Timestamp) -> str | None:
        driver, tier = _pick_driver_tiered(
            zone_id, dt, driver_lookup, rng, state_lookup, zone_state_map
        )
        if tier_counts is not None:
            tier_counts[tier] += 1
        return driver

    zone_risk = zone_risk_map.get(zone_id, SIM_DEFAULT_ZONE_RISK)
    attempts: list[dict] = []

    # --- Case 1: đã rời kho nhưng KHÔNG giao thành công ---
    if order_row["order_status"] != "delivered" or pd.isna(order_row["order_delivered_customer_date"]):
        num_fails = int(
            rng.choice(list(SIM_UNDELIVERED_FAIL_COUNTS), p=list(SIM_UNDELIVERED_FAIL_PROBS))
        )
        current_date = carrier_date + pd.Timedelta(days=1)
        for i in range(num_fails):
            if i > 0:
                current_date += pd.Timedelta(days=int(rng.integers(*SIM_RETRY_GAP_DAYS)))
            is_severe = weather_lookup.get((zone_id, current_date.normalize()), False)
            attempts.append(
                {
                    "attempt_id": f"{order_id}_{i + 1}",
                    "order_id": order_id,
                    "driver_id": _driver_for(current_date),
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
    delay_days = order_row["estimated_delivery_delay_days"]

    is_severe_at_delivery = weather_lookup.get((zone_id, delivered_date.normalize()), False)
    weather_risk = SIM_WEATHER_RISK if is_severe_at_delivery else 0.0
    holiday_risk = _event_risk_bonus(delivered_date, holiday_lookup, event_lookup)

    base_fail_prob = min(
        SIM_BASE_FAIL_PROB + (delay_days * SIM_DELAY_RISK_PER_DAY) + zone_risk + weather_risk + holiday_risk,
        SIM_MAX_FAIL_PROB,
    )

    dates = [delivered_date]
    current = delivered_date
    max_lookback = int(delay_days) + 2

    for _ in range(max_lookback):
        if rng.random() >= base_fail_prob:
            break
        days_back = int(rng.integers(*SIM_RETRY_GAP_DAYS))
        candidate = current - pd.Timedelta(days=days_back)
        # Quy tắc #5: không được lùi về trước ngày rời kho.
        if candidate <= carrier_date:
            break
        current = candidate
        dates.insert(0, current)
        base_fail_prob *= SIM_LOOKBACK_DECAY  # rủi ro giảm dần khi lùi xa hơn (heuristic mô phỏng)

    total = len(dates)
    for idx, dt in enumerate(dates):
        is_final = idx == total - 1
        is_severe = weather_lookup.get((zone_id, dt.normalize()), False)
        attempts.append(
            {
                "attempt_id": f"{order_id}_{idx + 1}",
                "order_id": order_id,
                "driver_id": _driver_for(dt),
                "zone_id": zone_id,
                "attempt_number": idx + 1,
                "attempt_timestamp": dt,
                "attempt_status": "success" if is_final else "failed",
                "failed_reason_id": None if is_final else _pick_failed_reason(rng, is_severe),
            }
        )
    return attempts


# ---------------------------------------------------------------------------
# Kiểm tra bảng vừa sinh — RAISE nếu vi phạm quy tắc mô phỏng
# ---------------------------------------------------------------------------
def validate_delivery_attempts(
    attempts_df: pd.DataFrame,
    orders_df: pd.DataFrame,
    drivers_df: pd.DataFrame,
    weather_lookup: dict,
) -> dict:
    """
    Kiểm tra bảng attempt vừa sinh so với quy tắc #1-#6 và các bất biến cấu trúc.
    RAISE ValueError (kèm số dòng vi phạm) nếu có vi phạm CỨNG. Trả về dict thống
    kê (vi phạm MỀM — do dữ liệu Olist bất thường — chỉ được đếm, không raise).

    orders_df: bảng order đã có zone_id + 3 cột ngày ở dạng datetime (chính là `df`
    trong generate_delivery_attempts).
    """
    stats: dict = {
        "n_attempts": int(len(attempts_df)),
        "n_orders_with_attempts": int(attempts_df["order_id"].nunique()) if len(attempts_df) else 0,
    }
    if attempts_df.empty:
        return stats

    errors: list[str] = []

    def _check(rule: str, mask: pd.Series) -> None:
        n = int(mask.sum())
        if n:
            errors.append(f"{rule}: {n:,} row(s)")

    a = attempts_df.sort_values(["order_id", "attempt_number"]).reset_index(drop=True)
    catalog_ids = {r["failed_reason_id"] for r in FAILED_REASONS}
    is_failed = a["attempt_status"] == "failed"
    is_success = a["attempt_status"] == "success"

    # --- bất biến cấu trúc ---
    _check("duplicate attempt_id", a["attempt_id"].duplicated())
    _check("attempt_status not in {success, failed}", ~(is_failed | is_success))
    _check("failed attempt without failed_reason_id", is_failed & a["failed_reason_id"].isna())
    _check("successful attempt carrying a failed_reason_id", is_success & a["failed_reason_id"].notna())
    _check(
        "failed_reason_id not in FAILED_REASONS",
        a["failed_reason_id"].notna() & ~a["failed_reason_id"].isin(catalog_ids),
    )

    by_order = a.groupby("order_id")
    expected_number = by_order.cumcount() + 1
    _check("attempt_number not contiguous from 1", a["attempt_number"] != expected_number)
    _check(
        "attempt_timestamp not strictly increasing within order",
        by_order["attempt_timestamp"].diff() <= pd.Timedelta(0),
    )
    is_last = by_order.cumcount(ascending=False) == 0
    _check("success attempt that is not the last attempt of its order", is_success & ~is_last)

    # --- neo vào order (quy tắc #1, #2a, #2b) ---
    o = orders_df[
        ["order_id", "order_status", "zone_id", "order_delivered_carrier_date", "order_delivered_customer_date"]
    ].drop_duplicates("order_id")
    m = a.merge(o, on="order_id", how="left", suffixes=("", "_order"), indicator=True)
    _check("attempt for an order that is not in the input orders", m["_merge"] == "left_only")
    _check("[rule 1] attempt for an order that never left the warehouse", m["order_delivered_carrier_date"].isna())
    _check("attempt zone_id differs from its order's zone_id", m["zone_id"] != m["zone_id_order"])

    last = m[is_last.to_numpy()]
    expected_success = (last["order_status"] == "delivered") & last["order_delivered_customer_date"].notna()
    _check(
        "[rule 2a] final attempt outcome disagrees with order_status/customer_date",
        (last["attempt_status"] == "success") != expected_success,
    )
    ok_success = m[is_success.to_numpy()]
    _check(
        "[rule 2b] successful attempt timestamp != order_delivered_customer_date",
        ok_success["attempt_timestamp"] != ok_success["order_delivered_customer_date"],
    )

    # --- FR09 chỉ khi mưa lớn tại đúng zone/ngày ---
    fr09 = a[a["failed_reason_id"] == REASON_EXTREME_WEATHER]
    if len(fr09):
        keys = zip(fr09["zone_id"], fr09["attempt_timestamp"].dt.normalize())
        not_severe = pd.Series([not weather_lookup.get(k, False) for k in keys], index=fr09.index)
        _check(f"[rule 4] {REASON_EXTREME_WEATHER} on a zone/day without heavy rain", not_severe)

    # --- tài xế (quy tắc #6) ---
    with_driver = m[m["driver_id"].notna()]
    d = drivers_df[["driver_id", "hire_date", "status"]].copy()
    d["hire_date"] = _to_naive_datetime(d["hire_date"])
    dm = with_driver.merge(d, on="driver_id", how="left", indicator="_driver_merge")
    _check("driver_id not found in drivers", dm["_driver_merge"] == "left_only")
    _check("[rule 6] driver hired after the attempt date", dm["hire_date"] > dm["attempt_timestamp"])
    _check("[rule 6] driver not active", (dm["_driver_merge"] == "both") & (dm["status"] != "active"))

    if errors:
        raise ValueError(
            "Generated delivery_attempts violate simulation rules — refusing to return bad data:\n  - "
            + "\n  - ".join(errors)
        )

    # --- vi phạm MỀM: bất thường của dữ liệu Olist, chỉ đếm ---
    stats["n_attempts_without_driver"] = int(a["driver_id"].isna().sum())
    stats["n_attempts_at_or_before_carrier_date"] = int(
        (m["attempt_timestamp"] <= m["order_delivered_carrier_date"]).sum()
    )
    return stats


def generate_delivery_attempts(
    orders_zoned_df: pd.DataFrame,
    drivers_df: pd.DataFrame,
    weather_df: pd.DataFrame | None = None,
    holidays_seed_path: Path = DEFAULT_HOLIDAYS_SEED_PATH,
    commercial_events_seed_path: Path = DEFAULT_COMMERCIAL_EVENTS_SEED_PATH,
    seed: int = DEFAULT_SEED,
    zone_state_map: dict | None = None,
) -> ExtractionResult:
    """
    Mô phỏng delivery_attempts cho từng order đã có zone_id (xem
    map_orders_to_zone). Xem docstring đầu file để hiểu đầy đủ triết lý neo
    vào Olist thật — hàm này CHỈ điều phối, logic mô phỏng nằm ở
    _simulate_order_attempts().

    zone_state_map (zone_id -> bang, xem build_zone_state_map): bật bậc fallback
    theo bang cho việc chọn tài xế. None = hành vi 2 bậc cũ (zone -> global).
    """
    df = orders_zoned_df.copy()
    for col in (
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ):
        df[col] = _to_naive_datetime(df[col])
    df["estimated_delivery_delay_days"] = (
        (df["order_delivered_customer_date"] - df["order_estimated_delivery_date"]).dt.days
    ).clip(lower=0).fillna(0)

    weather_lookup = _build_weather_severity_lookup(weather_df)
    holiday_lookup = _build_date_tier_lookup(holidays_seed_path, "holiday_impact_tier")
    event_lookup = _build_date_tier_lookup(commercial_events_seed_path, "commercial_event_tier")
    zone_risk_map = _build_zone_risk_map(df["zone_id"], seed)
    driver_lookup = _build_driver_lookup(drivers_df)
    state_lookup = _build_state_driver_lookup(drivers_df, zone_state_map)

    rng = np.random.default_rng(seed)
    tier_counts: Counter = Counter()
    all_attempts: list[dict] = []
    n_no_carrier = 0
    for _, row in df.iterrows():
        if pd.isna(row["order_delivered_carrier_date"]):
            n_no_carrier += 1
            continue
        all_attempts.extend(
            _simulate_order_attempts(
                row,
                zone_risk_map,
                weather_lookup,
                holiday_lookup,
                event_lookup,
                driver_lookup,
                rng,
                state_lookup=state_lookup,
                zone_state_map=zone_state_map,
                tier_counts=tier_counts,
            )
        )

    result_df = pd.DataFrame(all_attempts, columns=ATTEMPT_COLUMNS)
    validation = validate_delivery_attempts(result_df, df, drivers_df, weather_lookup)

    n_attempts = len(result_df)
    driver_assignment = {t: int(tier_counts.get(t, 0)) for t in (
        DRIVER_TIER_ZONE, DRIVER_TIER_STATE, DRIVER_TIER_GLOBAL, DRIVER_TIER_NONE
    )}
    driver_assignment["pct_same_zone"] = round(driver_assignment[DRIVER_TIER_ZONE] / n_attempts, 4) if n_attempts else 0.0
    driver_assignment["state_fallback_enabled"] = bool(state_lookup)

    metadata = {
        "source_name": "synthetic",
        "table_name": "delivery_attempts",
        "n_orders_input": int(len(df)),
        "n_orders_excluded_no_carrier_date": int(n_no_carrier),
        "n_attempts_generated": int(n_attempts),
        "seed": seed,
        "simulation_params": get_simulation_params(),
        "driver_assignment": driver_assignment,
        "n_severe_weather_zone_days": int(len(weather_lookup)),
        "n_holiday_dates": int(len(holiday_lookup)),
        "n_commercial_event_dates": int(len(event_lookup)),
        "validation": validation,
    }
    logger.info(
        f"Generated {n_attempts:,} attempt(s) for "
        f"{len(df) - n_no_carrier:,}/{len(df):,} order(s) "
        f"({n_no_carrier:,} excluded — chưa rời kho, ngoài phạm vi last-mile)"
    )
    logger.info(
        f"Driver assignment: {driver_assignment[DRIVER_TIER_ZONE]:,} same-zone, "
        f"{driver_assignment[DRIVER_TIER_STATE]:,} same-state, "
        f"{driver_assignment[DRIVER_TIER_GLOBAL]:,} global fallback "
        f"({driver_assignment['pct_same_zone']:.1%} same-zone)"
    )
    return ExtractionResult(table_name="delivery_attempts", df=result_df, metadata=metadata)


# ---------------------------------------------------------------------------
# Orchestration — đọc toàn bộ input từ path mặc định, sinh cả 2 bảng
# ---------------------------------------------------------------------------
def main(
    zone_centroids_path: Path | None = None,
    geolocation_path: Path = DEFAULT_GEOLOCATION_PATH,  # DEPRECATED — bị bỏ qua
    orders_path: Path = DEFAULT_ORDERS_PATH,
    customers_path: Path = DEFAULT_CUSTOMERS_PATH,
    holidays_seed_path: Path = DEFAULT_HOLIDAYS_SEED_PATH,
    commercial_events_seed_path: Path = DEFAULT_COMMERCIAL_EVENTS_SEED_PATH,
    geolocation_df: pd.DataFrame | None = None,  # DEPRECATED — bị bỏ qua
    orders_df: pd.DataFrame | None = None,
    customers_df: pd.DataFrame | None = None,
    weather_df: pd.DataFrame | None = None,
    n_drivers: int = DEFAULT_N_DRIVERS,
    seed: int = DEFAULT_SEED,
    zip_zone_mapping_path: Path | None = None,
) -> tuple[ExtractionResult, ExtractionResult]:
    """
    Chạy toàn bộ luồng: đọc input -> generate_drivers() -> map order sang
    zone -> generate_delivery_attempts(). Trả về 2 ExtractionResult (drivers,
    delivery_attempts) — Loader nhận thẳng .df của từng cái để ghi Postgres,
    không cần gọi lại pipeline nội bộ này.

    orders_df/customers_df: nếu được TRUYỀN VÀO (vd từ ingestion_flow.py — bản
    ĐÃ QUA schema_validator ở bước ingest_olist), dùng trực tiếp thay vì tự đọc
    lại CSV thô. Nếu để None (gọi độc lập ngoài flow, vd lúc dev/test), tự đọc
    từ path mặc định — giữ hàm này chạy độc lập được, không bắt buộc phải có flow.

    Zone của order được resolve qua seed dbt/seeds/zip_zone_mapping.csv (cùng
    nguồn với dbt) — geolocation_path/geolocation_df KHÔNG còn được dùng và chỉ
    giữ lại trong chữ ký để ingestion_flow.py cũ không bị vỡ. zone_centroids ưu
    tiên đọc từ dbt/seeds/zone_centroids.csv (fallback data/raw/synthetic/ nếu
    chưa có), để zone_id của driver/attempt luôn khớp dim_zone.

    weather_df KHÔNG có path mặc định (khác holidays/commercial_events) vì
    weather_extractor.py hiện CHƯA ghi ra file CSV trung gian — cần truyền
    trực tiếp DataFrame đã extract, hoặc để None nếu chưa sẵn sàng (weather
    risk sẽ tự tắt, có log cảnh báo, KHÔNG raise lỗi).
    """
    if zone_centroids_path is None:
        zone_centroids_path = _first_existing(DEFAULT_ZONE_CENTROIDS_PATH, LEGACY_ZONE_CENTROIDS_PATH)
    if zip_zone_mapping_path is None:
        zip_zone_mapping_path = DEFAULT_ZIP_ZONE_MAPPING_PATH

    zone_centroids_df = pd.read_csv(validate_file(zone_centroids_path, ".csv"))

    if orders_df is None:
        orders_df = pd.read_csv(validate_file(orders_path, ".csv"))
    if customers_df is None:
        customers_df = pd.read_csv(validate_file(customers_path, ".csv"))

    drivers_result = generate_drivers(zone_centroids_df, n_drivers=n_drivers, seed=seed)

    zip_to_zone_df = load_zip_to_zone_map(
        zip_zone_mapping_path, known_zone_ids=set(zone_centroids_df["zone_id"])
    )
    orders_zoned_df = map_orders_to_zone(orders_df, customers_df, zip_to_zone_df)

    attempts_result = generate_delivery_attempts(
        orders_zoned_df,
        drivers_result.df,
        weather_df=weather_df,
        holidays_seed_path=holidays_seed_path,
        commercial_events_seed_path=commercial_events_seed_path,
        seed=seed,
        zone_state_map=build_zone_state_map(zone_centroids_df),
    )

    return drivers_result, attempts_result


if __name__ == "__main__":
    main()