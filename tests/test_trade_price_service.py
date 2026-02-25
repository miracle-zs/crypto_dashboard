from datetime import datetime, timezone

from app.services.trade_price_service import (
    calc_utc_day_start,
    fetch_open_price_for_key,
    get_price_change_from_utc_start,
)


class _FakeProcessor:
    def __init__(self, kline_rows):
        self._kline_rows = kline_rows
        self.calls = []

    def get_kline_data(self, symbol, interval, start_time, limit, client=None):
        self.calls.append((symbol, interval, start_time, limit, client))
        return list(self._kline_rows)

    def _create_worker_client(self):
        return "worker-client"


def test_calc_utc_day_start_returns_midnight_timestamp():
    dt = datetime(2026, 2, 25, 15, 42, tzinfo=timezone.utc)
    result = calc_utc_day_start(int(dt.timestamp() * 1000))
    expected = int(datetime(2026, 2, 25, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    assert result == expected


def test_get_price_change_from_utc_start_returns_open_price():
    processor = _FakeProcessor([[0, "123.45"]])
    ts = int(datetime(2026, 2, 25, 11, 0, tzinfo=timezone.utc).timestamp() * 1000)
    price = get_price_change_from_utc_start(processor, symbol="BTCUSDT", timestamp=ts)
    assert price == 123.45
    assert processor.calls[0][0] == "BTCUSDT"
    assert processor.calls[0][1] == "1h"
    assert processor.calls[0][3] == 1


def test_fetch_open_price_for_key_uses_worker_client_and_returns_tuple():
    processor = _FakeProcessor([[0, "456.7"]])
    symbol = "ETHUSDT"
    day_start = int(datetime(2026, 2, 25, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)

    fetched_symbol, fetched_day_start, price, elapsed = fetch_open_price_for_key(
        processor,
        symbol=symbol,
        utc_day_start_ms=day_start,
    )

    assert fetched_symbol == symbol
    assert fetched_day_start == day_start
    assert price == 456.7
    assert elapsed >= 0
    assert processor.calls[0][4] == "worker-client"
