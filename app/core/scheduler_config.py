import os

from app.logger import logger


def read_int_env(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None:
        value = default
    else:
        try:
            value = int(raw)
        except ValueError:
            logger.warning(f"环境变量 {name}={raw} 非法，使用默认值 {default}")
            value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def read_float_env(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None:
        value = default
    else:
        try:
            value = float(raw)
        except ValueError:
            logger.warning(f"环境变量 {name}={raw} 非法，使用默认值 {default}")
            value = default
    if minimum is not None:
        value = max(minimum, value)
    return value
