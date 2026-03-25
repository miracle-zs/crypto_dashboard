from datetime import timezone

from app.services import market_snapshot_service


class _FakeClient:
    def public_get(self, endpoint, params=None):
        if endpoint == "/fapi/v1/ticker/24hr":
            return [{"symbol": "OTHERUSDT", "lastPrice": "1", "quoteVolume": "1"}]
        if endpoint == "/fapi/v1/ticker/price":
            return [{"symbol": "OTHERUSDT", "price": "1"}]
        raise AssertionError(f"unexpected endpoint: {endpoint}")


class _FakeProcessor:
    def __init__(self):
        self.client = _FakeClient()
        self.exchange_info_calls = 0

    def get_exchange_info(self, client=None):
        self.exchange_info_calls += 1
        return {
            "symbols": [
                {
                    "symbol": "BTCUSDT",
                    "contractType": "PERPETUAL",
                    "quoteAsset": "USDT",
                    "status": "TRADING",
                }
            ]
        }

    def _create_worker_client(self):
        return _FakeClient()

    def get_price_change_from_utc_start(self, symbol, timestamp, client=None):
        return None


class _FakeScheduler:
    def __init__(self):
        self.processor = _FakeProcessor()
        self.leaderboard_min_quote_volume = 50_000_000
        self.leaderboard_max_symbols = 120
        self.leaderboard_weight_budget_per_minute = 900
        self.leaderboard_kline_workers = 6
        self.leaderboard_top_n = 10

    @staticmethod
    def _is_api_cooldown_active(source: str) -> bool:
        return False


def _reset_exchange_info_cache():
    market_snapshot_service._EXCHANGE_SYMBOLS_CACHE["symbols"] = None
    market_snapshot_service._EXCHANGE_SYMBOLS_CACHE["expires_at"] = 0.0


def test_exchange_info_cache_hits_between_snapshot_builds(monkeypatch):
    monkeypatch.setenv("EXCHANGE_INFO_CACHE_TTL_SECONDS", "300")
    _reset_exchange_info_cache()
    scheduler = _FakeScheduler()

    market_snapshot_service.build_top_gainers_snapshot(scheduler, timezone.utc)
    market_snapshot_service.build_rebound_snapshot(
        scheduler,
        utc8=timezone.utc,
        window_days=7,
        top_n=10,
        kline_workers=2,
        weight_budget_per_minute=120,
        label="test-rebound",
    )

    assert scheduler.processor.exchange_info_calls == 1


def test_exchange_info_cache_can_be_disabled(monkeypatch):
    monkeypatch.setenv("EXCHANGE_INFO_CACHE_TTL_SECONDS", "0")
    _reset_exchange_info_cache()
    scheduler = _FakeScheduler()

    market_snapshot_service.build_top_gainers_snapshot(scheduler, timezone.utc)
    market_snapshot_service.build_top_gainers_snapshot(scheduler, timezone.utc)

    assert scheduler.processor.exchange_info_calls == 2


def test_rebound_excludes_first_daily_kline_when_it_is_listing_candle(monkeypatch):
    monkeypatch.setenv("EXCHANGE_INFO_CACHE_TTL_SECONDS", "0")
    _reset_exchange_info_cache()

    first_open = 1_700_000_000_000
    second_open = first_open + 86_400_000

    class _ReboundClient(_FakeClient):
        def public_get(self, endpoint, params=None):
            if endpoint == "/fapi/v1/ticker/price":
                return [{"symbol": "BTCUSDT", "price": "100"}]
            if endpoint == "/fapi/v1/klines":
                return [
                    [first_open, "10", "11", "5", "10"],
                    [second_open, "20", "21", "8", "20"],
                ]
            return super().public_get(endpoint, params)

    class _ReboundProcessor(_FakeProcessor):
        def __init__(self):
            super().__init__()
            self.client = _ReboundClient()

        def get_exchange_info(self, client=None):
            self.exchange_info_calls += 1
            return {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "contractType": "PERPETUAL",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                        "onboardDate": first_open + 3_600_000,
                    }
                ]
            }

        def _create_worker_client(self):
            return _ReboundClient()

    class _ReboundScheduler(_FakeScheduler):
        def __init__(self):
            super().__init__()
            self.processor = _ReboundProcessor()

    snapshot = market_snapshot_service.build_rebound_snapshot(
        _ReboundScheduler(),
        utc8=timezone.utc,
        window_days=365,
        top_n=10,
        kline_workers=1,
        weight_budget_per_minute=120,
        label="test-rebound-365d",
    )

    assert snapshot["top"] == 1
    row = snapshot["rows"][0]
    assert row["low_365d"] == 8.0
    assert round(row["rebound_365d_pct"], 2) == 1150.0


def test_rebound_keeps_first_daily_kline_for_non_listing_contract(monkeypatch):
    monkeypatch.setenv("EXCHANGE_INFO_CACHE_TTL_SECONDS", "0")
    _reset_exchange_info_cache()

    first_open = 1_700_000_000_000
    second_open = first_open + 86_400_000

    class _ReboundClient(_FakeClient):
        def public_get(self, endpoint, params=None):
            if endpoint == "/fapi/v1/ticker/price":
                return [{"symbol": "BTCUSDT", "price": "100"}]
            if endpoint == "/fapi/v1/klines":
                return [
                    [first_open, "10", "11", "5", "10"],
                    [second_open, "20", "21", "8", "20"],
                ]
            return super().public_get(endpoint, params)

    class _ReboundProcessor(_FakeProcessor):
        def __init__(self):
            super().__init__()
            self.client = _ReboundClient()

        def get_exchange_info(self, client=None):
            self.exchange_info_calls += 1
            return {
                "symbols": [
                    {
                        "symbol": "BTCUSDT",
                        "contractType": "PERPETUAL",
                        "quoteAsset": "USDT",
                        "status": "TRADING",
                        "onboardDate": first_open - 86_400_000,
                    }
                ]
            }

        def _create_worker_client(self):
            return _ReboundClient()

    class _ReboundScheduler(_FakeScheduler):
        def __init__(self):
            super().__init__()
            self.processor = _ReboundProcessor()

    snapshot = market_snapshot_service.build_rebound_snapshot(
        _ReboundScheduler(),
        utc8=timezone.utc,
        window_days=365,
        top_n=10,
        kline_workers=1,
        weight_budget_per_minute=120,
        label="test-rebound-365d",
    )

    assert snapshot["top"] == 1
    row = snapshot["rows"][0]
    assert row["low_365d"] == 5.0
    assert round(row["rebound_365d_pct"], 2) == 1900.0
