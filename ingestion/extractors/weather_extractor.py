"""
extractors/weather_extractor.py — Gọi Open-Meteo Historical Weather API theo
BATCH/ZONE: 1 request cho TOÀN BỘ date range của 1 zone, không phải 1 request
/ zone x ngày (đúng theo quyết định "dim_weather ETL: batch by zone" đã chốt,
để tránh gọi API hàng chục nghìn lần).
 
Input:  data/raw/synthetic/zone_centroids.csv (do scripts/train_zone_clustering.py
        sinh ra) — cần cột zone_id, centroid_lat, centroid_lng.
Output: ExtractionResult với DataFrame có cột date/zone_id/temp_max_c/
        temp_min_c/temp_avg_c/precipitation_mm/source — khớp đúng schema
        weather_daily_schema trong ingestion/schemas/weather_schema.py.
 
Hợp đồng Extractor (giống olist_extractor.py, dùng chung ExtractionResult
từ extractors/base.py):
    Extractor (module này)           -> pd.DataFrame THÔ + metadata
    Validator (schema_validator.py)  -> pd.DataFrame SẠCH
    Loader                            -> ghi vào Postgres
 
Module này gọi api_validator.validate_response() (vòng ngoài) TRƯỚC khi parse
JSON — vẫn là việc của Extractor, giống hệt cách olist_extractor.py gọi
file_validator TRƯỚC khi đọc CSV. KHÔNG tự ép kiểu/lọc range ở đây — đó là
việc của schema_validator.py ở bước sau.
"""
 
from __future__ import annotations
 
import time
from datetime import datetime, timezone
from pathlib import Path
 
import pandas as pd
import requests
import yaml
 
from ingestion.extractors.base import ExtractionResult
from ingestion.utils.logger import get_logger
from ingestion.validators.api_validator import ApiValidationError, validate_response
from ingestion.validators.file_validator import validate_file
 
logger = get_logger(__name__)
 
_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "ingestion_config.yaml"
DEFAULT_ZONE_CENTROIDS_PATH = Path("data/raw/synthetic/zone_centroids.csv")
 
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
DAILY_VARS = "temperature_2m_max,temperature_2m_min,temperature_2m_mean,precipitation_sum"
TIMEZONE = "America/Sao_Paulo"
 
# --- Cấu hình retry CỤC BỘ cho 1 zone (Circuit Breaker cấp zone) ---
# Triết lý: Prefect task-level retry chỉ nên dùng cho lỗi DIỆN RỘNG (DB chết,
# cả API Open-Meteo bảo trì) — vì nó chạy lại TOÀN BỘ task từ đầu, tốn công +
# hạn mức API kéo lại những zone ĐÃ thành công trước đó. Lỗi CỤC BỘ (rớt mạng
# thoáng qua ở 1 zone, rate limit tạm thời) phải được Extractor tự đứng lên
# xử lý tại chỗ, không để lan ra ngoài làm hỏng cả batch đang chạy dở.
MAX_RETRIES_PER_ZONE = 3
RETRY_BACKOFF_BASE_SECONDS = 2.0  # exponential: 2s, 4s, 8s...
# Status code coi là TẠM THỜI, đáng để retry (rate limit, server quá tải chớp
# nhoáng) — khác với 4xx (sai tham số) vốn retry lại cũng ra y hệt kết quả.
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}
 
 
class ZoneFetchError(Exception):
    """
    Raise khi 1 zone đã retry hết MAX_RETRIES_PER_ZONE lần vẫn thất bại vì lý
    do TẠM THỜI (mạng/rate limit) — đây là lỗi CỤC BỘ của riêng zone đó.
    extract_all() bắt lỗi này, ghi vào failed_zones, và tiếp tục zone kế tiếp
    — KHÔNG để lỗi này văng lên tới Prefect để nó retry lại cả task.
    """
 
_DEFAULT_WEATHER_CONFIG = {
    "start": "2016-09-01",
    "end": "2018-10-31",
    "rate_limit_per_minute": 60,
}
 
 
def _load_weather_config() -> dict:
    """Đọc date_range + rate_limit_per_minute của nguồn weather từ ingestion_config.yaml."""
    if not _CONFIG_PATH.exists():
        logger.warning(f"Config not found at {_CONFIG_PATH}, using default weather config")
        return dict(_DEFAULT_WEATHER_CONFIG)
 
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}
    weather_cfg = config.get("sources", {}).get("weather", {})
    date_range = weather_cfg.get("date_range", {})
    return {
        "start": date_range.get("start", _DEFAULT_WEATHER_CONFIG["start"]),
        "end": date_range.get("end", _DEFAULT_WEATHER_CONFIG["end"]),
        "rate_limit_per_minute": weather_cfg.get(
            "rate_limit_per_minute", _DEFAULT_WEATHER_CONFIG["rate_limit_per_minute"]
        ),
    }
 
 
def load_zone_centroids(path: str | Path = DEFAULT_ZONE_CENTROIDS_PATH) -> pd.DataFrame:
    """
    Đọc bảng zone centroid do scripts/train_zone_clustering.py sinh ra.
 
    Đây LÀ input của module này, KHÔNG phải output — nếu file thiếu, extractor
    chỉ báo lỗi rõ ràng (gợi ý chạy lại script clustering), KHÔNG tự chạy lại
    clustering thay người dùng.
    """
    validated_path = validate_file(path, expected_extension=".csv")
    df = pd.read_csv(validated_path)
 
    required_cols = {"zone_id", "centroid_lat", "centroid_lng"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"'{validated_path}' missing required column(s): {missing}. "
            f"Re-run scripts/train_zone_clustering.py to regenerate it."
        )
 
    logger.info(f"Loaded {len(df):,} zone centroid(s) from {validated_path}")
    return df
 
 
def _parse_daily_response(zone_id: str, body: dict) -> pd.DataFrame:
    """Reshape response 'daily' của Open-Meteo thành DataFrame thô 1 zone."""
    daily = body["daily"]
    df_zone = pd.DataFrame(
        {
            "date": daily["time"],
            "temp_max_c": daily["temperature_2m_max"],
            "temp_min_c": daily["temperature_2m_min"],
            "temp_avg_c": daily["temperature_2m_mean"],
            "precipitation_mm": daily["precipitation_sum"],
        }
    )
    df_zone["zone_id"] = zone_id
    df_zone["source"] = "open-meteo"
    return df_zone
 
 
def _fetch_zone_weather(
    zone_id: str,
    lat: float,
    lng: float,
    start_date: str,
    end_date: str,
    session: requests.Session | None = None,
) -> pd.DataFrame:
    """
    Gọi Open-Meteo cho 1 zone, TOÀN BỘ date range trong 1 request duy nhất.
 
    Tự retry TẠI CHỖ (không văng exception ra ngoài ngay) khi gặp lỗi TẠM
    THỜI — timeout/connection error, hoặc status code trong RETRYABLE_STATUS_CODES
    (429/5xx). Backoff tăng dần (2s, 4s, 8s) để không dồn dập gọi lại ngay khi
    server đang quá tải.
 
    Lỗi KHÔNG tạm thời (4xx, JSON hỏng, thiếu field 'daily') raise NGAY LẬP
    TỨC, không phí lượt retry — vì gọi lại với cùng tham số chắc chắn ra cùng
    kết quả lỗi.
 
    `session` cho phép truyền vào 1 requests.Session dùng chung (connection
    pooling khi gọi ~6800 zone) hoặc 1 object giả lập khi unit test.
 
    Raises:
        ZoneFetchError nếu hết MAX_RETRIES_PER_ZONE lần mà vẫn lỗi tạm thời.
        ApiValidationError NGAY LẬP TỨC nếu lỗi không tạm thời (không retry).
    """
    http_get = session.get if session is not None else requests.get
    params = {
        "latitude": lat,
        "longitude": lng,
        "start_date": start_date,
        "end_date": end_date,
        "daily": DAILY_VARS,
        "timezone": TIMEZONE,
    }
 
    last_error: Exception | None = None
 
    for attempt in range(1, MAX_RETRIES_PER_ZONE + 1):
        try:
            response = http_get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=30)
        except requests.exceptions.RequestException as e:
            # Rớt mạng thoáng qua (timeout, connection reset, DNS chập chờn...)
            # -> chính xác trường hợp bạn mô tả (rớt mạng ở zone 6500) -> retry
            # tại chỗ, KHÔNG để lan ra ngoài làm hỏng cả task Prefect.
            last_error = e
            logger.warning(
                f"[{zone_id}] Network error on attempt {attempt}/{MAX_RETRIES_PER_ZONE}: {e}"
            )
        else:
            if response.status_code == 200:
                body = validate_response(
                    response, expected_keys=["daily"], source_name="open-meteo"
                )
                return _parse_daily_response(zone_id, body)
 
            if response.status_code in RETRYABLE_STATUS_CODES:
                # Lỗi tạm thời phía server (rate limit 429, hoặc 5xx chớp
                # nhoáng) -> vẫn đáng retry.
                last_error = ApiValidationError(
                    f"Retryable status {response.status_code}: {response.text[:200]}"
                )
                logger.warning(
                    f"[{zone_id}] Retryable status {response.status_code} "
                    f"on attempt {attempt}/{MAX_RETRIES_PER_ZONE}"
                )
            else:
                # Lỗi KHÔNG nên retry (vd 400 sai tham số lat/lng) -> raise
                # NGAY, không đợi hết lượt retry vì retry cũng vô ích.
                validate_response(response, expected_keys=["daily"], source_name="open-meteo")
 
        if attempt < MAX_RETRIES_PER_ZONE:
            backoff = RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
            logger.info(f"[{zone_id}] Retrying in {backoff:.0f}s...")
            time.sleep(backoff)
 
    raise ZoneFetchError(
        f"Zone '{zone_id}' failed after {MAX_RETRIES_PER_ZONE} attempt(s): {last_error}"
    )
 
 
def extract_all(
    zone_centroids_path: str | Path = DEFAULT_ZONE_CENTROIDS_PATH,
    session: requests.Session | None = None,
) -> ExtractionResult:
    """
    Extract weather cho TẤT CẢ zone trong zone_centroids.csv, gộp thành 1
    ExtractionResult duy nhất (khác olist_extractor.py — ở đó mỗi bảng Olist
    là 1 ExtractionResult riêng; ở đây chỉ có đúng 1 bảng logic 'weather_daily').
 
    KHÔNG fail cả batch nếu 1 zone lỗi (rate limit tạm thời, timeout mạng) —
    log lỗi và tiếp tục zone tiếp theo, để vài zone lỗi không chặn toàn bộ
    hàng nghìn zone còn lại. Chỉ raise nếu TẤT CẢ zone đều lỗi (dấu hiệu API
    down hoàn toàn, không phải lỗi cục bộ).
    """
    zones_df = load_zone_centroids(zone_centroids_path)
    config = _load_weather_config()
    sleep_seconds = 60.0 / config["rate_limit_per_minute"]
 
    all_frames: list[pd.DataFrame] = []
    failed_zones: list[str] = []
 
    logger.info(
        f"Fetching weather for {len(zones_df):,} zone(s), "
        f"date range {config['start']} -> {config['end']}"
    )
 
    for _, row in zones_df.iterrows():
        zone_id = row["zone_id"]
        try:
            df_zone = _fetch_zone_weather(
                zone_id=zone_id,
                lat=row["centroid_lat"],
                lng=row["centroid_lng"],
                start_date=config["start"],
                end_date=config["end"],
                session=session,
            )
            all_frames.append(df_zone)
        except (ZoneFetchError, ApiValidationError, requests.RequestException) as e:
            # RequestException ở đây chỉ là lưới an toàn — bình thường
            # _fetch_zone_weather() đã tự bắt và retry lỗi này bên trong rồi.
            logger.error(f"Failed to fetch weather for zone '{zone_id}': {e}")
            failed_zones.append(zone_id)
 
        time.sleep(sleep_seconds)  # tôn trọng rate limit free tier của Open-Meteo
 
    if not all_frames:
        raise RuntimeError("Weather extraction failed for ALL zones — check API/network status")
 
    df = pd.concat(all_frames, ignore_index=True)
 
    metadata = {
        "source_name": "weather",
        "table_name": "weather_daily",
        "zones_requested": len(zones_df),
        "zones_succeeded": len(zones_df) - len(failed_zones),
        "zones_failed": failed_zones,
        "row_count_raw": len(df),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }
 
    logger.info(
        f"Weather extraction done — {metadata['zones_succeeded']}/{metadata['zones_requested']} "
        f"zone(s) succeeded, {len(df):,} total raw row(s)"
    )
    return ExtractionResult(table_name="weather_daily", df=df, metadata=metadata)