"""
ingestion/tests/test_validators.py — Test 3 lớp phòng thủ:
file_validator (vòng ngoài), api_validator (vòng ngoài), schema_validator
(vòng trong).
"""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion.validators import api_validator, file_validator, schema_validator


# ---------------------------------------------------------------------------
# file_validator.py
# ---------------------------------------------------------------------------
class TestFileValidator:
    def test_valid_file_passes(self, tmp_path):
        f = tmp_path / "good.csv"
        f.write_text("order_id,customer_id\n1,c1\n", encoding="utf-8")
        result = file_validator.validate_file(f)
        assert result == f

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(file_validator.FileValidationError):
            file_validator.validate_file(tmp_path / "missing.csv")

    def test_empty_file_raises(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_text("", encoding="utf-8")
        with pytest.raises(file_validator.FileValidationError):
            file_validator.validate_file(f)

    def test_wrong_extension_raises(self, tmp_path):
        f = tmp_path / "wrong.txt"
        f.write_text("order_id,customer_id\n1,c1\n", encoding="utf-8")
        with pytest.raises(file_validator.FileValidationError):
            file_validator.validate_file(f)

    def test_bad_encoding_raises(self, tmp_path):
        f = tmp_path / "bad_encoding.csv"
        f.write_bytes(b"\xff\xfeorder_id,customer_id\n1,c1\n")
        with pytest.raises(file_validator.FileValidationError):
            file_validator.validate_file(f)

    def test_directory_with_files_returns_sorted_list(self, tmp_path):
        (tmp_path / "b.csv").write_text("x\n1\n", encoding="utf-8")
        (tmp_path / "a.csv").write_text("x\n1\n", encoding="utf-8")
        files = file_validator.validate_directory_has_files(tmp_path)
        assert [f.name for f in files] == ["a.csv", "b.csv"]

    def test_empty_directory_raises(self, tmp_path):
        empty_dir = tmp_path / "empty_dir"
        empty_dir.mkdir()
        with pytest.raises(file_validator.FileValidationError):
            file_validator.validate_directory_has_files(empty_dir)

    def test_nonexistent_directory_raises(self, tmp_path):
        with pytest.raises(file_validator.FileValidationError):
            file_validator.validate_directory_has_files(tmp_path / "nope")


# ---------------------------------------------------------------------------
# api_validator.py
# ---------------------------------------------------------------------------
class TestApiValidator:
    def test_valid_dict_response_with_expected_keys(self, mock_response_factory):
        resp = mock_response_factory(200, {"daily": {"time": ["2018-01-01"]}})
        body = api_validator.validate_response(resp, expected_keys=["daily"], source_name="open-meteo")
        assert "daily" in body

    def test_valid_list_response_no_expected_keys(self, mock_response_factory):
        resp = mock_response_factory(200, [{"date": "2018-01-01"}])
        body = api_validator.validate_response(resp, source_name="nager-date")
        assert isinstance(body, list) and len(body) == 1

    def test_non_200_status_raises(self, mock_response_factory):
        resp = mock_response_factory(500, text="Internal Server Error")
        with pytest.raises(api_validator.ApiValidationError):
            api_validator.validate_response(resp, source_name="open-meteo")

    def test_invalid_json_raises(self, mock_response_factory):
        class BadJsonResponse:
            status_code = 200
            text = ""

            def json(self):
                raise ValueError("Expecting value")

        with pytest.raises(api_validator.ApiValidationError):
            api_validator.validate_response(BadJsonResponse(), source_name="open-meteo")

    def test_empty_body_raises(self, mock_response_factory):
        resp = mock_response_factory(200, {})
        with pytest.raises(api_validator.ApiValidationError):
            api_validator.validate_response(resp, source_name="open-meteo")

    def test_missing_expected_key_raises(self, mock_response_factory):
        resp = mock_response_factory(200, {"hourly": {}})
        with pytest.raises(api_validator.ApiValidationError):
            api_validator.validate_response(resp, expected_keys=["daily"], source_name="open-meteo")


# ---------------------------------------------------------------------------
# schema_validator.py
# ---------------------------------------------------------------------------
class TestSchemaValidator:
    def test_registry_has_14_tables(self):
        tables = schema_validator.list_registered_tables()
        assert len(tables) == 14

    def test_unknown_table_raises_keyerror(self):
        with pytest.raises(KeyError):
            schema_validator.validate(pd.DataFrame(), "khong_ton_tai")

    def test_happy_path_returns_validated_df(self):
        df = pd.DataFrame(
            {
                "customer_id": ["c1", "c2"],
                "customer_unique_id": ["u1", "u2"],
                "customer_zip_code_prefix": ["01001", None],
                "customer_city": ["sao paulo", "rio de janeiro"],
                "customer_state": ["SP", "RJ"],
            }
        )
        validated, errors = schema_validator.validate(df, "olist_customers")
        assert errors is None
        assert validated is not None

    def test_failure_path_returns_failure_cases(self):
        df = pd.DataFrame(
            {
                "customer_id": ["c1", "c2"],
                "customer_unique_id": ["u1", "u2"],
                "customer_zip_code_prefix": ["01001", "02002"],
                "customer_city": ["sao paulo", "rio de janeiro"],
                "customer_state": ["SPX", "RJ"],  # SPX sai độ dài (3 ký tự)
            }
        )
        validated, errors = schema_validator.validate(df, "olist_customers")
        assert validated is None
        assert errors is not None
        assert len(errors) >= 1

    def test_validate_or_raise_raises_on_bad_data(self):
        df = pd.DataFrame(
            {
                "customer_id": ["c1"],
                "customer_unique_id": ["u1"],
                "customer_zip_code_prefix": ["01001"],
                "customer_city": ["sao paulo"],
                "customer_state": ["SPX"],
            }
        )
        with pytest.raises(ValueError):
            schema_validator.validate_or_raise(df, "olist_customers")