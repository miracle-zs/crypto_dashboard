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
