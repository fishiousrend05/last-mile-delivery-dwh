"""
validators/file_validator.py — "Bảo vệ vòng ngoài" cho file trên đĩa.
 
Kiểm tra CÁI RƯƠNG từ bên ngoài (tồn tại, không rỗng, đúng định dạng, đọc
được) TRƯỚC KHI bê vào nhà (đưa vào RAM qua pandas.read_csv). KHÔNG quan tâm
bên trong rương có đúng thứ mình cần không (field đúng kiểu, đúng range) —
đó là việc của schema_validator.py (vòng trong), chạy SAU KHI file này đã
được đọc thành DataFrame.
"""
 
from __future__ import annotations
 
from pathlib import Path
 
from ingestion.utils.logger import get_logger
 
logger = get_logger(__name__)
 
MIN_FILE_SIZE_BYTES = 1  # rỗng tuyệt đối (0 byte) là lỗi rõ ràng nhất có thể bắt
 
 
class FileValidationError(Exception):
    """Raise khi 1 file không đạt điều kiện tối thiểu để đọc vào RAM."""
 
 
def validate_file(path: str | Path, expected_extension: str = ".csv") -> Path:
    """
    Kiểm tra 1 file: tồn tại, đúng phần mở rộng, không rỗng, đọc được UTF-8.
 
    Chỉ đọc thử dòng đầu tiên để bắt lỗi encoding/corruption sớm (vd file bị
    cắt giữa chừng lúc download, hoặc lỡ trỏ nhầm sang file binary) — KHÔNG
    đọc toàn bộ file ở đây, việc đó thuộc về pandas.read_csv() sau này.
 
    Returns:
        Path đã validate — extractor dùng thẳng, không cần validate lại.
 
    Raises:
        FileValidationError nếu bất kỳ điều kiện nào ở trên không đạt.
    """
    path = Path(path)
 
    if not path.exists():
        raise FileValidationError(f"File not found: {path}")
 
    if not path.is_file():
        raise FileValidationError(f"Path exists but is not a file: {path}")
 
    if path.suffix.lower() != expected_extension.lower():
        raise FileValidationError(
            f"Unexpected file extension for {path}: got '{path.suffix}', "
            f"expected '{expected_extension}'"
        )
 
    size = path.stat().st_size
    if size < MIN_FILE_SIZE_BYTES:
        raise FileValidationError(f"File is empty (0 bytes): {path}")
 
    try:
        with open(path, "r", encoding="utf-8") as f:
            first_line = f.readline()
    except UnicodeDecodeError as e:
        raise FileValidationError(f"File is not valid UTF-8: {path} ({e})") from e
 
    if not first_line.strip():
        raise FileValidationError(f"File header row is empty: {path}")
 
    logger.info(f"File validation passed: {path} ({size:,} bytes)")
    return path
 
 
def validate_directory_has_files(
    dir_path: str | Path, expected_extension: str = ".csv"
) -> list[Path]:
    """
    Kiểm tra 1 thư mục có ít nhất 1 file đúng định dạng — dùng ở đầu extractor
    trước khi loop qua từng file trong đó (vd data/source/olist/), để fail
    fast nếu thư mục trống hoặc path trong ingestion_config.yaml bị gõ sai,
    thay vì loop 0 lần rồi âm thầm "thành công" mà không load gì cả.
 
    Returns:
        Danh sách Path các file tìm được, đã sort để đảm bảo thứ tự xử lý
        ổn định giữa các lần chạy (không phụ thuộc thứ tự OS trả về).
    """
    dir_path = Path(dir_path)
    if not dir_path.exists() or not dir_path.is_dir():
        raise FileValidationError(f"Directory not found: {dir_path}")
 
    files = sorted(dir_path.glob(f"*{expected_extension}"))
    if not files:
        raise FileValidationError(
            f"No '{expected_extension}' file(s) found in directory: {dir_path}"
        )
 
    logger.info(f"Found {len(files)} '{expected_extension}' file(s) in {dir_path}")
    return files