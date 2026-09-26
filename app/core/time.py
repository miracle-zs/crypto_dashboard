import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Union

UTC = timezone.utc
UTC8 = timezone(timedelta(hours=8))


def now_ms() -> int:
    """Return current UTC timestamp in epoch milliseconds."""
    return int(time.time() * 1000)


def utc8_now() -> datetime:
    """Return current timezone-aware datetime in UTC+8."""
    return datetime.now(UTC8)


def ms_to_datetime_utc(ms: int) -> datetime:
    """Convert epoch milliseconds to timezone-aware UTC datetime."""
    return datetime.fromtimestamp(ms / 1000.0, tz=UTC)


def ms_to_utc8_str(ms: Optional[int], fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Convert epoch milliseconds to formatted string in UTC+8."""
    if ms is None:
        return ""
    dt = datetime.fromtimestamp(ms / 1000.0, tz=UTC8)
    return dt.strftime(fmt)


def parse_to_ms(val: Union[int, float, str, datetime, None]) -> Optional[int]:
    """Parse various datetime representations into integer epoch milliseconds.

    Handles:
    - int/float: treated as ms if > 1e11, else seconds
    - datetime: timezone-aware or naive (assumed UTC if naive)
    - str: ISO strings or '%Y-%m-%d %H:%M:%S' (assumed UTC+8 if naive '%Y-%m-%d %H:%M:%S')
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        # If timestamp is small (e.g. seconds), convert to ms
        if val < 1e11:
            return int(val * 1000)
        return int(val)
    if isinstance(val, datetime):
        if val.tzinfo is None:
            val = val.replace(tzinfo=UTC)
        return int(val.timestamp() * 1000)
    if isinstance(val, str):
        val = val.strip()
        if not val:
            return None
        # Try numeric string
        try:
            num = float(val)
            return parse_to_ms(num)
        except ValueError:
            pass
        # Try ISO format
        try:
            dt = datetime.fromisoformat(val)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass
        # Try standard YYYY-MM-DD HH:MM:SS (assumed UTC+8 historically)
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(val, fmt).replace(tzinfo=UTC8)
                return int(dt.timestamp() * 1000)
            except ValueError:
                pass
    return None
