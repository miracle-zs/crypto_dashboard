import pytest
import requests

from app.binance_client import BinanceFuturesRestClient
from app.services import market_snapshot_service
from app.core.binance_request_budget import RollingWeightLimiter, WeightedRequestSession


def test_binance_request_weight_uses_binance_endpoint_rules():
    weight = getattr(market_snapshot_service, "binance_request_weight", None)

    assert callable(weight), "market snapshots must share one endpoint-weight function"
    assert weight("/fapi/v1/klines", {"limit": 99}) == 1
    assert weight("/fapi/v1/klines", {"limit": 100}) == 2
    assert weight("/fapi/v1/klines", {"limit": 365}) == 2
    assert weight("/fapi/v1/klines", {"limit": 500}) == 5
    assert weight("/fapi/v1/klines", {"limit": 1000}) == 5
    assert weight("/fapi/v1/klines", {"limit": 1001}) == 10
    assert weight("/fapi/v1/ticker/24hr") == 40
    assert weight("/fapi/v1/ticker/24hr", {"symbol": "BTCUSDT"}) == 1
    assert weight("/fapi/v1/income") == 30
    assert weight("/fapi/v1/allOrders", {"symbol": "BTCUSDT"}) == 5
    assert weight("/fapi/v1/userTrades", {"symbol": "BTCUSDT"}) == 5


def test_task_budget_sums_every_request_actually_emitted():
    total = getattr(market_snapshot_service, "calculate_request_weight", None)

    assert callable(total), "task budgets must be calculated from the complete request list"
    requests = [
        ("/fapi/v1/ticker/24hr", None),
        ("/fapi/v1/klines", {"symbol": "BTCUSDT", "limit": 2}),
        ("/fapi/v1/klines", {"symbol": "BTCUSDT", "limit": 7}),
        ("/fapi/v1/klines", {"symbol": "BTCUSDT", "limit": 24}),
        ("/fapi/v1/klines", {"symbol": "ETHUSDT", "limit": 365}),
    ]

    assert total(requests) == 45


def test_365d_backfill_budget_is_not_underestimated_for_527_symbols():
    requests = [("/fapi/v1/exchangeInfo", None)] + [
        ("/fapi/v1/klines", {"symbol": f"S{index}USDT", "limit": 365})
        for index in range(527)
    ]

    assert market_snapshot_service.calculate_request_weight(requests) == 1055


def test_rolling_limiter_never_uses_natural_minute_boundaries():
    class FakeClock:
        now = 59.9

        def monotonic(self):
            return self.now

        def sleep(self, seconds):
            self.now += seconds

    clock = FakeClock()
    limiter = RollingWeightLimiter(
        5,
        window_seconds=60,
        clock=clock.monotonic,
        sleeper=clock.sleep,
    )

    limiter.acquire(3)
    limiter.acquire(2)
    waited = limiter.acquire(1)

    assert waited == pytest.approx(60.0)
    assert clock.now == pytest.approx(119.9)
    assert limiter.usage() == 1


def test_weighted_session_counts_the_calls_the_client_actually_receives():
    class FakeClient:
        def __init__(self):
            self.calls = []

        def public_get(self, path, params=None):
            self.calls.append((path, params))
            return []

    client = FakeClient()
    session = WeightedRequestSession(client)
    session.public_get("/fapi/v1/klines", {"symbol": "BTCUSDT", "limit": 365})
    session.public_get("/fapi/v1/klines", {"symbol": "BTCUSDT", "limit": 2})
    session.public_get("/fapi/v1/ticker/24hr")

    assert session.requests == client.calls
    assert session.total_weight == 43


def test_429_activates_cooldown_and_is_never_retried(monkeypatch):
    calls = []

    class FakeResponse:
        status_code = 429
        text = '{"code": -1003, "msg": "too many requests"}'

        def raise_for_status(self):
            raise requests.exceptions.HTTPError("429", response=self)

        def json(self):
            return {"code": -1003, "msg": "too many requests"}

    monkeypatch.setattr(
        "app.binance_client.requests.request",
        lambda **kwargs: calls.append(kwargs) or FakeResponse(),
    )
    BinanceFuturesRestClient._cooldown_until_ms = 0
    client = BinanceFuturesRestClient(min_request_interval=0)

    try:
        assert client.public_get("/fapi/v1/klines", {"symbol": "BTCUSDT", "limit": 365}) is None
        assert len(calls) == 1
        assert client.is_global_cooldown_active() is True
    finally:
        BinanceFuturesRestClient._cooldown_until_ms = 0


def test_legacy_full_sync_budget_cannot_override_200_hard_cap(monkeypatch):
    from app.jobs.sync_trades_job import _configure_full_sync_request_budget

    configured = []
    monkeypatch.setenv("FULL_SYNC_REQUEST_BUDGET_ENABLED", "1")
    monkeypatch.setenv("FULL_SYNC_REQUEST_BUDGET_PER_MINUTE", "900")
    monkeypatch.setattr(
        BinanceFuturesRestClient,
        "configure_global_request_budget",
        lambda **kwargs: configured.append(kwargs),
    )

    assert _configure_full_sync_request_budget(200) is True
    assert configured == [{"enabled": True, "per_minute": 200}]
