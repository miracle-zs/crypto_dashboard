"""
日志模块 - 统一日志管理
"""
import logging
import os
from pathlib import Path
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

# 日志目录
LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# 日志文件路径
LOG_FILE = LOG_DIR / "app.log"

# 配置日志格式
LOG_FORMAT = "%(asctime)s | %(levelname)-5s | %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "7"))  # 默认保留7天历史文件

# 创建logger
logger = logging.getLogger("crypto_dashboard")
logger.setLevel(logging.INFO)
logger.propagate = False

# 避免重复添加 handler（例如 --reload 场景）
if not logger.handlers:
    # 文件处理器（按天轮转，午夜切分）
    file_handler = TimedRotatingFileHandler(
        LOG_FILE,
        when="midnight",
        interval=1,
        backupCount=max(1, LOG_BACKUP_COUNT),
        encoding="utf-8",
        utc=False
    )
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

    # 从文件尾部读取，避免整文件加载导致大日志卡顿
    lines = max(1, int(lines))
    chunk_size = 4096
    buffer = b""
    line_count = 0

    with open(LOG_FILE, "rb") as f:
        f.seek(0, os.SEEK_END)
        pos = f.tell()

        while pos > 0 and line_count <= lines:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            data = f.read(read_size)
            buffer = data + buffer
            line_count = buffer.count(b"\n")

    all_lines = buffer.decode("utf-8", errors="replace").splitlines(keepends=True)
    return list(reversed(all_lines[-lines:]))


def clear_logs():
    """清空日志文件"""
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        f.write("")
    logger.info("日志已清空")
