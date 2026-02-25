from datetime import datetime, timezone
import time
from typing import Optional

from app.binance_client import BinanceFuturesRestClient


def calc_utc_day_start(timestamp: int) -> int:
    dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
    day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(day_start.timestamp() * 1000)


def get_price_change_from_utc_start(
    processor,
    *,
    symbol: str,
    timestamp: int,
    client: Optional[BinanceFuturesRestClient] = None,
) -> Optional[float]:
    try:
        day_start = calc_utc_day_start(timestamp)
        kline_start = processor.get_kline_data(symbol, "1h", day_start, 1, client=client)
        if not kline_start:
            return None
        return float(kline_start[0][1])
    except Exception:
        return None


def fetch_open_price_for_key(
    processor,
    *,
    symbol: str,
    utc_day_start_ms: int,
) -> tuple[str, int, Optional[float], float]:
    started_at = time.perf_counter()
    worker_client = processor._create_worker_client()
    try:
        kline_start = processor.get_kline_data(
            symbol=symbol,
            interval="1h",
            start_time=utc_day_start_ms,
            limit=1,
            client=worker_client,
        )
        if not kline_start:
            return symbol, utc_day_start_ms, None, time.perf_counter() - started_at
        open_price = float(kline_start[0][1])
        return symbol, utc_day_start_ms, open_price, time.perf_counter() - started_at
    except Exception:
        return symbol, utc_day_start_ms, None, time.perf_counter() - started_at
