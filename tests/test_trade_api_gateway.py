import pytest
import importlib.util
from pathlib import Path


_MODULE_PATH = Path(__file__).resolve().parents[1] / "app" / "services" / "trade_api_gateway.py"
_SPEC = importlib.util.spec_from_file_location("trade_api_gateway_under_test", _MODULE_PATH)
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
fetch_all_orders = _MODULE.fetch_all_orders
fetch_income_history = _MODULE.fetch_income_history


class _WindowedOrdersClient:
    def __init__(self, old_window_end, new_order):
        self.old_window_end = old_window_end
        self.new_order = new_order
        self.calls = []

    def signed_get(self, endpoint, params=None):
        assert endpoint == "/fapi/v1/allOrders"
        params = params or {}
        self.calls.append(dict(params))
        if params["endTime"] <= self.old_window_end:
            return None
        return [self.new_order]


def test_fetch_all_orders_stops_on_failed_historical_window():
    seven_days_ms = 7 * 24 * 60 * 60 * 1000
    first_start = 1000
    first_end = first_start + seven_days_ms - 1
    second_start = first_end + 1
    second_end = second_start + 1000
    order = {"orderId": 42, "updateTime": second_start + 10}
    client = _WindowedOrdersClient(old_window_end=first_end, new_order=order)

    with pytest.raises(RuntimeError, match="allOrders request failed for HEIUSDT"):
        fetch_all_orders(
            client=client,
            symbol="HEIUSDT",
            start_time=first_start,
            end_time=second_end,
            fail_on_error=True,
        )

    assert client.calls[0]["startTime"] == first_start
    assert len(client.calls) == 1


def test_fetch_all_orders_still_raises_when_every_window_fails():
    client = _WindowedOrdersClient(old_window_end=10_000_000_000, new_order={"orderId": 42})

    with pytest.raises(RuntimeError, match="allOrders request failed for HEIUSDT"):
        fetch_all_orders(
            client=client,
            symbol="HEIUSDT",
            start_time=1000,
            end_time=2000,
            fail_on_error=True,
        )


def test_fetch_income_history_raises_when_requested():
    class FailingClient:
        def signed_get(self, endpoint, params=None):
            return None

    with pytest.raises(RuntimeError, match="income request failed"):
        fetch_income_history(
            client=FailingClient(),
            since=1000,
            until=2000,
            fail_on_error=True,
        )


def test_fetch_user_trades_deduplicates_trade_ids_with_overlap():
    fetch_user_trades = getattr(_MODULE, "fetch_user_trades", None)
    assert callable(fetch_user_trades)

    class FakeClient:
        def __init__(self):
            self.calls = []

        def signed_get(self, endpoint, params=None):
            self.calls.append((endpoint, dict(params or {})))
            return [
                {"id": 7, "time": 1500, "orderId": 70},
                {"id": 7, "time": 1500, "orderId": 70},
                {"id": 8, "time": 1600, "orderId": 80},
            ]

    client = FakeClient()
    rows = fetch_user_trades(
        client=client,
        symbol="BTCUSDT",
        start_time=1000,
        end_time=2000,
        limit=1000,
        fail_on_error=True,
    )

    assert [row["id"] for row in rows] == [7, 8]
    assert client.calls == [
        (
            "/fapi/v1/userTrades",
            {
                "symbol": "BTCUSDT",
                "limit": 1000,
                "startTime": 1000,
                "endTime": 2000,
            },
        )
    ]


def test_all_orders_paginates_same_timestamp_with_order_id():
    class FakeClient:
        def __init__(self):
            self.calls = []

        def signed_get(self, endpoint, params=None):
            params = dict(params or {})
            self.calls.append(params)
            if "orderId" not in params:
                return [
                    {"orderId": 1, "updateTime": 1500},
                    {"orderId": 2, "updateTime": 1500},
                ]
            return [{"orderId": 3, "updateTime": 1500}]

    client = FakeClient()
    rows = fetch_all_orders(
        client=client,
        symbol="BTCUSDT",
        start_time=1000,
        end_time=2000,
        limit=2,
        fail_on_error=True,
    )

    assert [row["orderId"] for row in rows] == [1, 2, 3]
    assert client.calls[1]["orderId"] == 3


def test_user_trades_paginates_with_from_id_without_time_params():
    fetch_user_trades = _MODULE.fetch_user_trades

    class FakeClient:
        def __init__(self):
            self.calls = []

        def signed_get(self, endpoint, params=None):
            params = dict(params or {})
            self.calls.append(params)
            if "fromId" not in params:
                return [{"id": 1, "time": 1500}, {"id": 2, "time": 1500}]
            return [{"id": 3, "time": 1600}]

    client = FakeClient()
    rows = fetch_user_trades(
        client=client,
        symbol="BTCUSDT",
        start_time=1000,
        end_time=2000,
        limit=2,
        fail_on_error=True,
    )

    assert [row["id"] for row in rows] == [1, 2, 3]
    assert client.calls[1] == {"symbol": "BTCUSDT", "limit": 2, "fromId": 3}
