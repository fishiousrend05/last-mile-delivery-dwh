"""
ingestion/tests/conftest.py — Fixture dùng chung cho toàn bộ test suite.

Mọi test trong ingestion/tests/ chạy HOÀN TOÀN offline: không gọi API thật
(Open-Meteo/Nager.Date qua MockSession/MockResponse), không cần Postgres
thật (postgres_loader test bằng monkeypatch ở boundary — xem test_loaders.py).
Đây là unit test — E2E thật với DB thật nằm ở bước Manual Test riêng, không
phải trách nhiệm của file này.
"""

from __future__ import annotations

import time as time_module

import pandas as pd
import pytest


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """
    Tắt time.sleep() TOÀN CỤC cho mọi test — các module có retry backoff
    (weather_extractor, holiday seed script) sẽ chạy tức thì thay vì đợi
    2s/4s/8s thật, giữ test suite nhanh.
    """
    monkeypatch.setattr(time_module, "sleep", lambda seconds: None)


class MockResponse:
    """Response giả lập cho requests, đủ field mà api_validator.validate_response() cần."""

    def __init__(self, status_code: int, json_data=None, text: str = ""):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text

    def json(self):
        return self._json_data


@pytest.fixture
def mock_response_factory():
    """Factory tạo MockResponse — dùng trong test thay vì import trực tiếp class."""
    return MockResponse


@pytest.fixture
def sample_zone_centroids_df() -> pd.DataFrame:
    """3 zone mẫu, đúng format zone_centroids.csv do train_zone_clustering.py sinh ra."""
    return pd.DataFrame(
        {
            "zone_id": ["852a1073fffffff", "852a1077fffffff", "852a107bfffffff"],
            "centroid_lat": [-23.55, -22.90, -19.92],
            "centroid_lng": [-46.63, -43.20, -43.94],
            "point_count": [500, 300, 50],
            "dominant_state": ["SP", "RJ", "MG"],
        }
    )


@pytest.fixture
def tmp_olist_dir(tmp_path):
    """
    Thư mục data/source/olist/ tạm với 1 file orders + 1 file customers tối
    thiểu — đủ cho test extract_table()/extract_all() mà không cần dataset
    Olist thật (~150MB).
    """
    olist_dir = tmp_path / "olist"
    olist_dir.mkdir()

    (olist_dir / "olist_orders_dataset.csv").write_text(
        "order_id,customer_id,order_status\no1,c1,delivered\no2,c2,shipped\n",
        encoding="utf-8",
    )
    (olist_dir / "olist_customers_dataset.csv").write_text(
        "customer_id,customer_zip_code_prefix\nc1,1001\nc2,2002\n",
        encoding="utf-8",
    )
    return olist_dir