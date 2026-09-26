from app.core.trade_matching import match_orders_to_positions


def order(oid, side, time, price, fee=2.0):
    return {
        "orderId": oid,
        "side": side,
        "positionSide": "BOTH",
        "executedQty": "1",
        "avgPrice": str(price),
        "updateTime": time,
        "type": "MARKET",
        "commission": fee,  # explicit execution fee
    }


def test_deterministic_trade_matching_fee_invariance():
    """Verify that matching with fill-level commission produces identical fees regardless of batch boundaries."""
    # Two completed round-trips:
    # Trade 1: Entry at t=1000, Exit at t=2000 (holding 1000s). Entry fee 1.0, Exit fee 1.0 -> total fee 2.0
    # Trade 2: Entry at t=3000, Exit at t=6000 (holding 3000s). Entry fee 1.0, Exit fee 1.0 -> total fee 2.0
    orders = [
        order(1, "BUY", 1000, 100, fee=1.0),
        order(2, "SELL", 2000, 110, fee=1.0),
        order(3, "BUY", 3000, 100, fee=1.0),
        order(4, "SELL", 6000, 110, fee=1.0),
    ]

    # Whole batch matching
    whole = match_orders_to_positions(orders, "BTCUSDT")

    # Split batch matching (e.g. incremental sync)
    split = (
        match_orders_to_positions(orders[:2], "BTCUSDT")
        + match_orders_to_positions(orders[2:], "BTCUSDT")
    )

    # In deterministic matching:
    # Trade 1 fees must be -2.0, Trade 2 fees must be -2.0 in BOTH whole and split!
    assert len(whole) == 2
    assert len(split) == 2
    assert [x["fees"] for x in whole] == [-2.0, -2.0]
    assert [x["fees"] for x in split] == [-2.0, -2.0]
    assert [x["pnl"] for x in whole] == [x["pnl"] for x in split]
