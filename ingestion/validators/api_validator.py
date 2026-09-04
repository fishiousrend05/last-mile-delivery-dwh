"""
validators/api_validator.py — "Bảo vệ vòng ngoài" cho response API (Open-Meteo,
Nager.Date).
 
Kiểm tra CÁI RƯƠNG (response HTTP) từ bên ngoài — request có thành công
không, body có phải JSON hợp lệ không, có rỗng không, có đủ field cấp cao
nhất mong đợi không — TRƯỚC KHI bê vào nhà (parse thành DataFrame). Field
BÊN TRONG đúng kiểu/range (vd temp_avg_c phải trong khoảng -10..50) là việc
của schema_validator.py (vòng trong), chạy SAU KHI response này đã thành
DataFrame.
"""
 
from __future__ import annotations
 
from typing import Any
 
from ingestion.utils.logger import get_logger
 
logger = get_logger(__name__)
 
 
class ApiValidationError(Exception):
    """Raise khi 1 response API không đạt điều kiện tối thiểu để parse."""
 
 
def validate_response(
    response: Any,
    expected_keys: list[str] | None = None,
    source_name: str = "unknown",
) -> dict | list:
    """
    Kiểm tra 1 requests.Response TRƯỚC khi extractor parse thành DataFrame.
 
    Args:
        response: object trả về từ requests.get()/post() (chỉ cần có thuộc
            tính .status_code, .json(), .text — không ép kiểu requests.Response
            cụ thể để dễ test bằng mock, không cần cài requests lúc unit test).
        expected_keys: danh sách key cấp cao nhất bắt buộc có trong JSON —
            chỉ áp dụng khi body là dict (vd Open-Meteo trả {"daily": {...}}).
            Bỏ qua nếu body là list (vd Nager.Date trả thẳng list ngày lễ).
        source_name: tên nguồn, chỉ để log/raise lỗi rõ ràng từ API nào.
 
    Returns:
        Body JSON đã parse (dict hoặc list) — extractor dùng thẳng để build
        DataFrame, không cần gọi response.json() lại lần nữa.
 
    Raises:
        ApiValidationError nếu status code lỗi, JSON không hợp lệ, body rỗng,
        hoặc thiếu expected_keys.
    """
    status_code = getattr(response, "status_code", None)
    if status_code != 200:
        body_preview = getattr(response, "text", "")[:300]
        raise ApiValidationError(
            f"[{source_name}] Unexpected status code {status_code}: {body_preview}"
        )
 
    try:
        body = response.json()
    except ValueError as e:
        raise ApiValidationError(f"[{source_name}] Response is not valid JSON: {e}") from e
 
    if not body:
        raise ApiValidationError(f"[{source_name}] Response body is empty")
 
    if expected_keys and isinstance(body, dict):
        missing = [k for k in expected_keys if k not in body]
        if missing:
            raise ApiValidationError(
                f"[{source_name}] Response missing expected key(s): {missing}. "
                f"Got top-level keys: {list(body.keys())}"
            )
 
    logger.info(f"[{source_name}] API response validation passed")
    return body
 