"""
日志模块 - 统一日志管理
"""
import logging
import os
from pathlib import Path
from datetime import datetime

# 日志目录
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# 日志文件路径
LOG_FILE = LOG_DIR / "app.log"

# 配置日志格式
LOG_FORMAT = "%(asctime)s | %(levelname)-5s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

# 创建logger
logger = logging.getLogger("crypto_dashboard")
logger.setLevel(logging.INFO)

# 文件处理器
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))

# 控制台处理器
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT, DATE_FORMAT))

# 添加处理器
logger.addHandler(file_handler)
logger.addHandler(console_handler)


def get_log_file_path() -> str:
    """获取日志文件路径"""
    return str(LOG_FILE)


def read_logs(lines: int = 200) -> list:
    """
    读取最近的日志行

    Args:
        lines: 读取的行数

    Returns:
        list: 日志行列表（最新的在前）
    """
    if not LOG_FILE.exists():
        return []

    with open(LOG_FILE, "r", encoding="utf-8") as f:
        all_lines = f.readlines()

    # 返回最后 N 行，倒序
    return list(reversed(all_lines[-lines:]))


def clear_logs():
    """清空日志文件"""
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("")
    logger.info("日志已清空")
