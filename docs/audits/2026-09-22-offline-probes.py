"""Offline audit probes; assertions describe required behavior and currently fail.

Run explicitly with pytest. No exchange requests or production DB writes.
These intentionally failing audit probes are outside the default test suite.
"""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.core.binance_request_budget import binance_request_weight
from app.jobs.balance_sync_job import run_balance_sync_job
from app.services.trade_api_gateway import fetch_income_history, fetch_real_positions
from app.trade_processor import TradeDataProcessor


def test_income_identity_includes_income_type():
    records = [
        dict(tranId=123, incomeType="REALIZED_PNL", income="10", time=1000),
        dict(tranId=123, incomeType="COMMISSION", income="-1", time=1000),
    ]
    result = fetch_income_history(
        client=SimpleNamespace(signed_get=lambda *args: records), since=0, until=2000
    )
    assert len(result) == 2, "Different income types must not be deduplicated"


def test_income_pagination_preserves_same_millisecond_boundary():
    records = [dict(tranId=i, time=1000, incomeType="COMMISSION") for i in range(1001)]

    def get(_path, params):
        eligible = [r for r in records if params["startTime"] <= r["time"] <= params["endTime"]]
        page = int(params.get("page", 1))
        return eligible[(page - 1) * 1000:page * 1000]

    result = fetch_income_history(client=SimpleNamespace(signed_get=get), since=0, until=2000)
    assert len(result) == 1001, "A full page must not skip the remaining same-ms record"


@pytest.mark.parametrize("endpoint", ["/fapi/v3/account", "/fapi/v3/positionRisk"])
def test_account_endpoint_weights(endpoint):
    assert binance_request_weight(endpoint) == 5


def test_open_position_older_than_query_window_is_not_reported_flat():
    processor = TradeDataProcessor("offline", "offline")
    processor.get_real_positions = lambda: {"BTCUSDT": 1.0}
    processor.get_all_orders = lambda *args, **kwargs: []
    result = processor.get_open_positions(since=1_000_000, until=2_000_000)
    assert result is None or result, "An authoritative live position cannot become a successful empty snapshot"


def test_hedge_mode_preserves_both_position_sides():
    rows = [
        dict(symbol="BTCUSDT", positionSide="LONG", positionAmt="1"),
        dict(symbol="BTCUSDT", positionSide="SHORT", positionAmt="-2"),
    ]
    result = fetch_real_positions(client=SimpleNamespace(signed_get=lambda *args: rows))
    assert len(result) == 2, "Symbol-only keys overwrite one hedge-mode position"


def test_balance_fetch_failure_is_not_success():
    scheduler = SimpleNamespace(
        processor=SimpleNamespace(get_account_balance=lambda: None),
        _is_api_cooldown_active=lambda **kwargs: False,
        _try_enter_api_job_slot=lambda **kwargs: True,
        _release_api_job_slot=Mock(),
    )
    assert run_balance_sync_job(scheduler) == "error"


def test_incremental_closed_order_window_preserves_entry_context():
    orders = [
        dict(orderId=1, side="BUY", positionSide="LONG", executedQty="1", avgPrice="100", updateTime=1000),
        dict(orderId=2, side="SELL", positionSide="LONG", executedQty="1", avgPrice="110", updateTime=3_000_000),
    ]
    processor = TradeDataProcessor("offline", "offline")
    processor.get_all_orders = lambda *args, **kwargs: [
        row for row in orders if kwargs["start_time"] <= row["updateTime"] <= kwargs["end_time"]
    ]
    processor.get_user_trades = lambda *args, **kwargs: []
    complete, _ = processor._extract_symbol_closed_positions(
        "BTCUSDT", 0, 4_000_000, fee_totals_by_symbol={}
    )
    assert len(complete) == 1
    incremental, _, _ = processor._extract_symbol_closed_positions(
        "BTCUSDT", 0, 4_000_000, fee_totals_by_symbol={},
        order_cursor={"last_time_ms": 2_500_000},
        cursor_overlap_minutes=30, return_cursor_update=True,
    )
    assert len(incremental) == 1, "Moving the cursor drops the opening order and silently loses the closure"
