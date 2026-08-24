from datetime import datetime, timezone
from types import SimpleNamespace

from app.services import market_snapshot_service


DAY_MS = 86_400_000


class _FakeRepo:
    def __init__(self):
        self.rows = []

    def latest_open_times(self, symbols):
        return {symbol: None for symbol in symbols}

    def upsert(self, rows):
        self.rows.extend(rows)
        return len(rows)


class _FakeClient:
    def __init__(self, klines=None):
        self.klines = klines or [[1_700_000_000_000, "10", "11", "8", "10"]]

    def public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/klines":
            return self.klines
        raise AssertionError(f"unexpected endpoint: {endpoint}")


class _FakeProcessor:
    def __init__(self, *, onboard_date=None, klines=None):
        self.client = _FakeClient(klines)
        self.exchange_info_calls = 0
        self.onboard_date = onboard_date

    def get_exchange_info(self, client=None):
        self.exchange_info_calls += 1
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                    "onboardDate": self.onboard_date,
                }
            ]
        }


def _scheduler(*, onboard_date=None, klines=None):
    return SimpleNamespace(
        processor=_FakeProcessor(onboard_date=onboard_date, klines=klines),
        daily_kline_repo=_FakeRepo(),
    )


def _reset_exchange_info_cache():
    market_snapshot_service._EXCHANGE_SYMBOLS_CACHE["symbols"] = None
    market_snapshot_service._EXCHANGE_SYMBOLS_CACHE["expires_at"] = 0.0


def test_exchange_info_cache_hits_between_daily_updates(monkeypatch):
    monkeypatch.setenv("EXCHANGE_INFO_CACHE_TTL_SECONDS", "300")
    _reset_exchange_info_cache()
    scheduler = _scheduler()

    market_snapshot_service.update_daily_kline_cache(scheduler, max_weight_per_60s=200)
    market_snapshot_service.update_daily_kline_cache(scheduler, max_weight_per_60s=200)

    assert scheduler.processor.exchange_info_calls == 1


def test_exchange_info_cache_can_be_disabled(monkeypatch):
    monkeypatch.setenv("EXCHANGE_INFO_CACHE_TTL_SECONDS", "0")
    _reset_exchange_info_cache()
    scheduler = _scheduler()

    market_snapshot_service.update_daily_kline_cache(scheduler, max_weight_per_60s=200)
    market_snapshot_service.update_daily_kline_cache(scheduler, max_weight_per_60s=200)

    assert scheduler.processor.exchange_info_calls == 2


def test_leaderboard_local_calculation_includes_drawdown_fields():
    now = datetime(2026, 8, 24, 1, tzinfo=timezone.utc)
    midnight = int(now.replace(hour=0).timestamp() * 1000)
    snapshot = market_snapshot_service.calculate_top_gainers_snapshot(
        ticker_data=[{"symbol": "BTCUSDT", "lastPrice": "100", "quoteVolume": "100000000"}],
        daily_klines_by_symbol={
            "BTCUSDT": [
                {"open_time": midnight - DAY_MS, "open": 90, "high": 140, "low": 80, "close": 100},
                {"open_time": midnight, "open": 80, "high": 130, "low": 70, "close": 100},
            ]
        },
        utc8=timezone.utc,
        now_utc=now,
        min_quote_volume=50_000_000,
        max_symbols=120,
        top_n=10,
    )

    row = snapshot["rows"][0]
    assert round(row["drawdown_from_7d_high_pct"], 2) == -28.57
    assert round(row["drawdown_from_window_high_pct"], 2) == -23.08


def test_backfill_excludes_listing_candle_before_persisting(monkeypatch):
    monkeypatch.setenv("EXCHANGE_INFO_CACHE_TTL_SECONDS", "0")
    _reset_exchange_info_cache()
    first_open = 1_700_000_000_000
    second_open = first_open + DAY_MS
    scheduler = _scheduler(
        onboard_date=first_open + 3_600_000,
        klines=[
            [first_open, "10", "11", "5", "10"],
            [second_open, "20", "21", "8", "20"],
        ],
    )

    market_snapshot_service.update_daily_kline_cache(
        scheduler,
        now_ms=second_open + 2 * DAY_MS,
        max_weight_per_60s=200,
    )

    assert [row["open_time"] for row in scheduler.daily_kline_repo.rows] == [second_open]


def test_backfill_keeps_first_candle_for_existing_contract(monkeypatch):
    monkeypatch.setenv("EXCHANGE_INFO_CACHE_TTL_SECONDS", "0")
    _reset_exchange_info_cache()
    first_open = 1_700_000_000_000
    second_open = first_open + DAY_MS
    scheduler = _scheduler(
        onboard_date=first_open - DAY_MS,
        klines=[
            [first_open, "10", "11", "5", "10"],
            [second_open, "20", "21", "8", "20"],
        ],
    )

    market_snapshot_service.update_daily_kline_cache(
        scheduler,
        now_ms=second_open + 2 * DAY_MS,
        max_weight_per_60s=200,
    )

    assert [row["open_time"] for row in scheduler.daily_kline_repo.rows] == [first_open, second_open]
