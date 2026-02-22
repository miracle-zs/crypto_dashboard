import threading

from app.trade_processor import TradeDataProcessor


class _FakeClient:
    def __init__(self):
        self.calls = 0

    def public_get(self, endpoint, params=None):
        assert endpoint == "/fapi/v1/exchangeInfo"
        self.calls += 1
        return {"symbols": [{"symbol": "BTCUSDT"}]}


def _make_processor(fake_client, ttl_seconds: float):
    processor = TradeDataProcessor.__new__(TradeDataProcessor)
    processor.client = fake_client
    processor._exchange_info_cache = None
    processor._exchange_info_expires_at = 0.0
    processor._exchange_info_lock = threading.Lock()
    processor._exchange_info_cache_ttl_seconds = ttl_seconds
    return processor


def test_get_exchange_info_uses_ttl_cache_for_default_client():
    fake_client = _FakeClient()
    processor = _make_processor(fake_client=fake_client, ttl_seconds=300.0)

    first = processor.get_exchange_info()
    second = processor.get_exchange_info()

    assert first == second
    assert fake_client.calls == 1


def test_get_exchange_info_cache_disabled_when_ttl_zero():
    fake_client = _FakeClient()
    processor = _make_processor(fake_client=fake_client, ttl_seconds=0.0)

    processor.get_exchange_info()
    processor.get_exchange_info()

    assert fake_client.calls == 2
