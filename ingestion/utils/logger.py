# Định nghĩa cách ghi log (in ra terminal hoặc lưu vào file .log). Nó giúp bạn biết luồng chạy đến đâu và lỗi ở dòng nào.
"""
utils/logger.py — Cấu hình logging thống nhất cho toàn bộ ingestion pipeline.
 
Ghi đồng thời ra console (xem trực tiếp khi chạy `prefect deployment run`
hoặc chạy tay lúc dev) và ra file trong LOG_DIR (audit sau này, đặc biệt khi
flow chạy theo lịch, không có ai ngồi xem console). Dùng RotatingFileHandler
để file log không phình vô hạn theo thời gian.
"""
 
import logging
import os
import sys
from logging.handlers import RotatingFileHandler
 
from dotenv import load_dotenv
 
load_dotenv()
 
_LOG_DIR = os.getenv("LOG_DIR", "logs")
_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
 
# Cache logger đã cấu hình theo tên, tránh add handler trùng lặp nếu
# get_logger() bị gọi nhiều lần cho cùng 1 module (vd khi Prefect import
# lại module giữa các task).
_configured_loggers: dict[str, logging.Logger] = {}
 
 
def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Trả về logger đã cấu hình sẵn console handler + rotating file handler.
 
    Usage:
        logger = get_logger(__name__)
        logger.info("Extracted 99441 rows from olist_orders")
    """
    if name in _configured_loggers:
        return _configured_loggers[name]
 
    os.makedirs(_LOG_DIR, exist_ok=True)
 
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # tránh log bị in 2 lần nếu root logger cũng có handler
 
    formatter = logging.Formatter(_LOG_FORMAT, datefmt=_DATE_FORMAT)
 
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
 
    file_handler = RotatingFileHandler(
        filename=os.path.join(_LOG_DIR, "ingestion.log"),
        maxBytes=10 * 1024 * 1024,  # 10MB / file
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
 
    _configured_loggers[name] = logger
    return logger