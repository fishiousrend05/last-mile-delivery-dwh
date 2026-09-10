"""
ingestion/utils/geo.py — Helper H3 dùng CHUNG cho mọi module cần map toạ độ
sang zone_id ở resolution 5 (đã chốt) — tách ra từ scripts/train_zone_clustering.py
để dùng lại ở ingestion/generators/synthetic_generator.py (map order ->
zone_id qua customer_zip_code_prefix) mà không lặp code tương thích H3 v3/v4.

RESOLUTION = 5 PHẢI khớp với giá trị đã dùng khi build zone_centroids.csv —
nếu đổi resolution ở đây mà không rebuild lại zone_centroids.csv, zone_id
tính ra sẽ KHÔNG khớp với zone nào trong bảng centroid đã có.
"""

from __future__ import annotations

import h3

RESOLUTION = 5  # đã chốt: H3 res5 (~10km/cell) — xem train_zone_clustering.py


def latlng_to_h3(lat: float, lng: float, resolution: int = RESOLUTION) -> str:
    """Tương thích ngược: h3 v4 dùng latlng_to_cell, v3 dùng geo_to_h3."""
    try:
        return h3.latlng_to_cell(lat, lng, resolution)
    except AttributeError:
        return h3.geo_to_h3(lat, lng, resolution)


def h3_to_center(h3_index: str) -> tuple[float, float]:
    """Tương thích ngược: h3 v4 dùng cell_to_latlng, v3 dùng h3_to_geo."""
    try:
        return h3.cell_to_latlng(h3_index)
    except AttributeError:
        return h3.h3_to_geo(h3_index)