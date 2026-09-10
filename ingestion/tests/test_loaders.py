"""
ingestion/tests/test_loaders.py — Test file_loader.py (thật, không mock, vì
chỉ đụng đĩa cục bộ) và postgres_loader.py (mock ở boundary — get_engine,
start_run, end_run, get_last_run_status — vì unit test không có Postgres
thật; kiểm tra ĐÚNG LUỒNG bằng Manual E2E test riêng, không phải ở đây).
"""
 
from __future__ import annotations
 
import pandas as pd
import pytest
 
from ingestion.loaders import file_loader, postgres_loader
from ingestion.validators.file_validator import FileValidationError
 
 
# ---------------------------------------------------------------------------
# file_loader.py
# ---------------------------------------------------------------------------
class TestFileLoader:
    def test_save_and_load_roundtrip(self, tmp_path):
        df = pd.DataFrame({"date": ["2018-01-01"], "zone_id": ["z1"]})
        saved_path = file_loader.save_to_raw_file(df, "weather", "weather_daily", raw_dir=tmp_path)
        assert saved_path.exists()
 
        loaded = file_loader.load_from_raw_file("weather", "weather_daily", raw_dir=tmp_path)
        assert loaded.equals(df)
 
    def test_load_missing_file_raises(self, tmp_path):
        with pytest.raises(FileValidationError):
            file_loader.load_from_raw_file("weather", "khong_ton_tai", raw_dir=tmp_path)
 
    def test_save_creates_directory_if_missing(self, tmp_path):
        target_dir = tmp_path / "nested" / "dir"
        df = pd.DataFrame({"a": [1]})
        saved_path = file_loader.save_to_raw_file(df, "synthetic", "drivers", raw_dir=target_dir)
        assert saved_path.parent == target_dir
        assert saved_path.exists()
 
    def test_get_raw_dir_falls_back_when_source_not_in_config(self):
        raw_dir = file_loader.get_raw_dir("khong_ton_tai_trong_config")
        assert raw_dir == file_loader.DEFAULT_RAW_ROOT / "khong_ton_tai_trong_config"
 
 
# ---------------------------------------------------------------------------
# postgres_loader.py — pure helpers (không cần Postgres thật)
# ---------------------------------------------------------------------------
class TestPostgresLoaderPureHelpers:
    def test_validate_identifier_accepts_valid_name(self):
        assert postgres_loader._validate_identifier("olist_orders", "table_name") == "olist_orders"
 
    @pytest.mark.parametrize(
        "bad_value",
        ["orders; DROP TABLE users;--", "orders-2", "1orders", "", "   "],
    )
    def test_validate_identifier_rejects_invalid_or_malicious(self, bad_value):
        with pytest.raises(ValueError):
            postgres_loader._validate_identifier(bad_value, "table_name")
 
    def test_normalise_load_mode_valid_and_case_insensitive(self):
        assert postgres_loader._normalise_load_mode("full_refresh") == "full_refresh"
        assert postgres_loader._normalise_load_mode("APPEND") == "append"
 
    def test_normalise_load_mode_rejects_unsupported(self):
        with pytest.raises(ValueError):
            postgres_loader._normalise_load_mode("incremental")
 
    def test_reads_landing_schema_and_batch_size_from_config(self):
        assert postgres_loader.get_landing_schema() == "landing"
        assert postgres_loader.get_batch_size() == 5000
 
 
# ---------------------------------------------------------------------------
# postgres_loader.py — load_dataframe() control flow (mock ở boundary)
# ---------------------------------------------------------------------------
class TestPostgresLoaderControlFlow:
    def test_empty_dataframe_raises_before_touching_db(self, monkeypatch):
        called = []
        monkeypatch.setattr(postgres_loader, "init_metadata_table", lambda: called.append("init"))
        with pytest.raises(ValueError):
            postgres_loader.load_dataframe(pd.DataFrame(), "src", "tbl", "full_refresh")
        assert called == []  # không được đụng DB nếu df rỗng
 
    def test_invalid_load_mode_raises_before_touching_db(self, monkeypatch):
        called = []
        monkeypatch.setattr(postgres_loader, "init_metadata_table", lambda: called.append("init"))
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(ValueError):
            postgres_loader.load_dataframe(df, "src", "tbl", "incremental")
        assert called == []
 
    def test_concurrent_load_blocked(self, monkeypatch):
        start_run_calls = []
        monkeypatch.setattr(postgres_loader, "init_metadata_table", lambda: None)
        monkeypatch.setattr(postgres_loader, "get_last_run_status", lambda s, t: "running")
        monkeypatch.setattr(
            postgres_loader, "start_run", lambda s, t, m: start_run_calls.append((s, t, m))
        )
 
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(postgres_loader.ConcurrentLoadError):
            postgres_loader.load_dataframe(df, "src", "tbl", "full_refresh")
        assert start_run_calls == []  # không được start 1 run mới khi đang có run chạy
 
    def test_success_path_calls_end_run_success_and_returns_row_count(self, monkeypatch):
        to_sql_calls = {}
 
        def fake_to_sql(self, name, con, schema, if_exists, index, chunksize, method):
            to_sql_calls.update(
                name=name, schema=schema, if_exists=if_exists, index=index, method=method
            )
 
        end_run_calls = []
        monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)
        monkeypatch.setattr(postgres_loader, "get_engine", lambda: _FakeEngine())
        monkeypatch.setattr(postgres_loader, "init_metadata_table", lambda: None)
        monkeypatch.setattr(postgres_loader, "get_last_run_status", lambda s, t: None)
        monkeypatch.setattr(postgres_loader, "start_run", lambda s, t, m: "run-123")
        monkeypatch.setattr(
            postgres_loader,
            "end_run",
            lambda run_id, status, row_count=None, error_message=None: end_run_calls.append(
                (run_id, status, row_count, error_message)
            ),
        )
 
        df = pd.DataFrame({"a": [1, 2, 3]})
        row_count = postgres_loader.load_dataframe(df, "src", "my_table", "full_refresh")
 
        assert row_count == 3
        assert to_sql_calls["name"] == "my_table"
        assert to_sql_calls["if_exists"] == "replace"
        assert end_run_calls == [("run-123", "success", 3, None)]
 
    def test_append_mode_uses_if_exists_append(self, monkeypatch):
        to_sql_calls = {}
 
        def fake_to_sql(self, name, con, schema, if_exists, index, chunksize, method):
            to_sql_calls["if_exists"] = if_exists
 
        monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql)
        monkeypatch.setattr(postgres_loader, "get_engine", lambda: _FakeEngine())
        monkeypatch.setattr(postgres_loader, "init_metadata_table", lambda: None)
        monkeypatch.setattr(postgres_loader, "get_last_run_status", lambda s, t: None)
        monkeypatch.setattr(postgres_loader, "start_run", lambda s, t, m: "run-456")
        monkeypatch.setattr(postgres_loader, "end_run", lambda *a, **kw: None)
 
        df = pd.DataFrame({"a": [1]})
        postgres_loader.load_dataframe(df, "weather", "weather_daily", "append")
        assert to_sql_calls["if_exists"] == "append"
 
    def test_failure_path_calls_end_run_failed_and_wraps_exception(self, monkeypatch):
        def raising_to_sql(self, *args, **kwargs):
            raise RuntimeError("connection lost mid-write")
 
        end_run_calls = []
        monkeypatch.setattr(pd.DataFrame, "to_sql", raising_to_sql)
        monkeypatch.setattr(postgres_loader, "get_engine", lambda: _FakeEngine())
        monkeypatch.setattr(postgres_loader, "init_metadata_table", lambda: None)
        monkeypatch.setattr(postgres_loader, "get_last_run_status", lambda s, t: None)
        monkeypatch.setattr(postgres_loader, "start_run", lambda s, t, m: "run-789")
        monkeypatch.setattr(
            postgres_loader,
            "end_run",
            lambda run_id, status, row_count=None, error_message=None: end_run_calls.append(
                (run_id, status, error_message)
            ),
        )
 
        df = pd.DataFrame({"a": [1]})
        with pytest.raises(postgres_loader.PostgresLoaderError):
            postgres_loader.load_dataframe(df, "src", "tbl", "full_refresh")
 
        assert len(end_run_calls) == 1
        run_id, status, error_message = end_run_calls[0]
        assert status == "failed"
        assert "connection lost mid-write" in error_message
 
 
class _FakeEngine:
    """Engine giả — chỉ cần hỗ trợ .begin() như 1 context manager, không thao tác DB thật."""
 
    def begin(self):
        return _FakeConnectionContext()
 
 
class _FakeConnectionContext:
    def __enter__(self):
        return self
 
    def __exit__(self, *exc_info):
        return False
 
    def exec_driver_sql(self, sql):
        return None