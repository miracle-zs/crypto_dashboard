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
