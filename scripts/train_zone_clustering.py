"""
scripts/train_zone_clustering.py — Sinh bảng zone theo H3 resolution 5, dùng
làm INPUT CHUNG cho các module downstream:
    - ingestion/extractors/weather_extractor.py (centroid lat/lng để gọi
      Open-Meteo theo batch/zone)
    - ingestion/generators/synthetic_generator.py (gán zone_id cho driver)
    - dbt seed cho dim_zone
 
Đây là SCRIPT ONE-OFF (không thuộc Prefect flow định kỳ) — chạy tay khi cần
rebuild lại danh sách zone. Logic dọn lại từ scripts/Dim_zone.ipynb (cell H3 chính
thức), bỏ toàn bộ phần thử nghiệm KMeans/so sánh weather (đã hoàn thành vai
trò làm bằng chứng cho quyết định chuyển từ KMeans sang H3 res5, không cần
chạy lại mỗi lần rebuild zone).
 
Input:  data/source/olist/olist_geolocation_dataset.csv
Output: data/raw/synthetic/zone_centroids.csv
        Cột: zone_id, centroid_lat, centroid_lng, point_count, dominant_state
 
"point_count" tính trên TOÀN BỘ điểm gốc (chưa dedup) join lại theo zone —
không chỉ đếm toạ độ unique — để phản ánh đúng mật độ hoạt động thật của zone.
"dominant_state" (state xuất hiện nhiều nhất trong zone) chỉ mang tính tham
khảo, dùng để hậu kiểm xem 1 zone có bị tràn qua nhiều bang không — H3 res5
(~10km/cell) gần như không xảy ra trường hợp này, khác với KMeans k=20 trước
đó (đã phát hiện cụm bán kính ~955km trong quá trình đánh giá rủi ro).
"""
 
from __future__ import annotations
 
from pathlib import Path
 
import h3
import pandas as pd
 
from ingestion.utils.logger import get_logger
from ingestion.validators.file_validator import validate_file
 
logger = get_logger(__name__)
 
RESOLUTION = 5  # đã chốt: H3 res5 (~10km/cell), thay cho KMeans ban đầu
 
# Bounding box gần đúng của Brazil — loại nhiễu toạ độ nằm ngoài lãnh thổ,
# đã phát hiện lúc audit 9 file Olist ở Phase thiết kế.
BRAZIL_LAT_RANGE = (-35.0, 5.0)
BRAZIL_LNG_RANGE = (-75.0, -30.0)
 
DEFAULT_INPUT_PATH = Path("data/source/olist/olist_geolocation_dataset.csv")
DEFAULT_OUTPUT_PATH = Path("data/raw/synthetic/zone_centroids.csv")
 
 
def _latlng_to_h3(lat: float, lng: float, resolution: int) -> str:
    """Tương thích ngược: h3 v4 dùng latlng_to_cell, v3 dùng geo_to_h3."""
    try:
        return h3.latlng_to_cell(lat, lng, resolution)
    except AttributeError:
        return h3.geo_to_h3(lat, lng, resolution)
 
 
def _h3_to_center(h3_index: str) -> tuple[float, float]:
    """Tương thích ngược: h3 v4 dùng cell_to_latlng, v3 dùng h3_to_geo."""
    try:
        return h3.cell_to_latlng(h3_index)
    except AttributeError:
        return h3.h3_to_geo(h3_index)
 
 
def load_geolocation(input_path: Path) -> pd.DataFrame:
    """Đọc geolocation CSV, qua file_validator (vòng ngoài) trước khi vào RAM."""
    validated_path = validate_file(input_path, expected_extension=".csv")
    logger.info(f"Reading geolocation data from {validated_path}")
    df = pd.read_csv(validated_path)
    logger.info(f"Loaded {len(df):,} raw geolocation rows")
    return df
 
 
def filter_brazil_bounds(df: pd.DataFrame) -> pd.DataFrame:
    """Loại các điểm ngoài bounding box Brazil — nhiễu toạ độ đã phát hiện lúc audit."""
    before = len(df)
    df_filtered = df[
        df["geolocation_lat"].between(*BRAZIL_LAT_RANGE)
        & df["geolocation_lng"].between(*BRAZIL_LNG_RANGE)
    ].copy()
    dropped = before - len(df_filtered)
    logger.info(f"Filtered to Brazil bounds: dropped {dropped:,} out-of-bounds row(s)")
    return df_filtered
 
 
def build_zone_table(df: pd.DataFrame, resolution: int = RESOLUTION) -> pd.DataFrame:
    """
    Map từng điểm toạ độ sang H3 index, rồi tổng hợp thành 1 dòng / zone:
    centroid (lat/lng), point_count, dominant_state.
    """
    df_unique = df.drop_duplicates(subset=["geolocation_lat", "geolocation_lng"]).copy()
    logger.info(f"{len(df_unique):,} unique coordinate pair(s) after dedup")
 
    logger.info(f"Mapping coordinates to H3 index (resolution={resolution})...")
    df_unique["zone_id"] = df_unique.apply(
        lambda row: _latlng_to_h3(row["geolocation_lat"], row["geolocation_lng"], resolution),
        axis=1,
    )
 
    # Join lại với df GỐC (chưa dedup) để đếm đúng tổng số điểm/zone — không
    # chỉ đếm số toạ độ unique, khớp đúng ý nghĩa "mật độ hoạt động" của zone.
    df_full = df.merge(
        df_unique[["geolocation_lat", "geolocation_lng", "zone_id"]],
        on=["geolocation_lat", "geolocation_lng"],
        how="inner",
    )
 
    zone_stats = (
        df_full.groupby("zone_id")
        .agg(
            point_count=("zone_id", "size"),
            dominant_state=("geolocation_state", lambda s: s.mode().iloc[0]),
        )
        .reset_index()
    )
 
    unique_zone_ids = df_unique["zone_id"].unique()
    centroids = pd.DataFrame(
        [
            {"zone_id": zid, **dict(zip(["centroid_lat", "centroid_lng"], _h3_to_center(zid)))}
            for zid in unique_zone_ids
        ]
    )
 
    zone_table = centroids.merge(zone_stats, on="zone_id", how="left")
    logger.info(f"Built {len(zone_table):,} H3 zone(s) at resolution {resolution}")
    return zone_table
 
 
def main(
    input_path: Path = DEFAULT_INPUT_PATH, output_path: Path = DEFAULT_OUTPUT_PATH
) -> pd.DataFrame:
    df_geo = load_geolocation(input_path)
    df_geo = filter_brazil_bounds(df_geo)
    zone_table = build_zone_table(df_geo)
 
    output_path.parent.mkdir(parents=True, exist_ok=True)
    zone_table.to_csv(output_path, index=False)
    logger.info(f"Saved zone centroid table to {output_path} ({len(zone_table):,} zone(s))")
 
    return zone_table
 
 
if __name__ == "__main__":
    main()