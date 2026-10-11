from app.core.trade_matching import match_orders_to_positions


def _make_order(order_id, side, price, qty, time_ms, position_side="LONG", fee=0.0):
    return {
        "orderId": order_id,
        "side": side,
        "positionSide": position_side,
        "executedQty": str(qty),
        "avgPrice": str(price),
        "updateTime": time_ms,
        "type": "MARKET",
        "fee": fee,
    }


def test_exit_before_entry_is_not_matched():
    # Exit order at t=1000, Entry order at t=2000
    exit_order = _make_order(1, "SELL", 50.0, 10.0, 1000, position_side="LONG")
    entry_order = _make_order(2, "BUY", 40.0, 10.0, 2000, position_side="LONG")

    # When exit arrives before entry (or when presorted/ordered)
    positions, remaining_lots = match_orders_to_positions(
        [exit_order, entry_order], "TESTUSDT", return_open_lots=True
    )

    # They must NOT be matched to produce a negative holding time!
    assert len(positions) == 0
    assert len(remaining_lots) == 1
    assert remaining_lots[0]["order_id"] == 2
    assert remaining_lots[0]["remaining_qty"] == 10.0


def test_initial_open_lots_from_future_not_consumed_by_past_exit():
    # Initial open lot from future (e.g. Oct 10, t=5000)
    future_lot = {
        "symbol": "TESTUSDT",
        "position_side": "LONG",
        "side": "LONG",
        "order_id": 999,
        "price": 0.017,
        "qty": 100.0,
        "remaining_qty": 100.0,
        "time_ms": 5000,
        "fee_rate": 0.0,
    }

    # Order list contains past exit order from Sep 26 (t=1000)
    past_exit = _make_order(101, "SELL", 0.058, 100.0, 1000, position_side="LONG")

    positions, remaining_lots = match_orders_to_positions(
        [past_exit],
        "TESTUSDT",
        initial_open_lots=[future_lot],
        return_open_lots=True,
    )

    # Must NOT produce a fake trade where exit at t=1000 closes entry at t=5000!
    assert len(positions) == 0
    assert len(remaining_lots) == 1
    assert remaining_lots[0]["order_id"] == 999
    assert remaining_lots[0]["remaining_qty"] == 100.0


def test_one_way_orders_causality():
    # BOTH position mode: SELL at t=1000, BUY lot at t=2000
    future_both_lot = {
        "symbol": "TESTUSDT",
        "position_side": "BOTH",
        "side": "LONG",
        "order_id": 888,
        "price": 100.0,
        "qty": 1.0,
        "remaining_qty": 1.0,
        "time_ms": 2000,
        "fee_rate": 0.0,
    }
    past_exit = _make_order(102, "SELL", 110.0, 1.0, 1000, position_side="BOTH")

    positions, remaining_lots = match_orders_to_positions(
        [past_exit],
        "TESTUSDT",
        initial_open_lots=[future_both_lot],
        return_open_lots=True,
    )

    assert len(positions) == 0
    remaining_ids = {l["order_id"] for l in remaining_lots}
    assert 888 in remaining_ids
