from app.core.trade_matching import is_liquidation_order, match_orders_to_positions


def test_match_orders_to_positions_presorted_matches_default():
    orders = [
        {
            "orderId": 2,
            "side": "SELL",
            "positionSide": "LONG",
            "executedQty": "1",
            "avgPrice": "110",
            "updateTime": 2000,
        },
        {
            "orderId": 1,
            "side": "BUY",
            "positionSide": "LONG",
            "executedQty": "1",
            "avgPrice": "100",
            "updateTime": 1000,
        },
    ]
    fees_map = {1000: -1.0}

    default_result = match_orders_to_positions(orders, "BTCUSDT", fees_map=fees_map)
    sorted_orders = sorted(orders, key=lambda x: x["updateTime"])
    presorted_result = match_orders_to_positions(sorted_orders, "BTCUSDT", fees_map=fees_map, presorted=True)

    assert default_result == presorted_result
    assert len(default_result) == 1
    assert default_result[0]["symbol"] == "BTCUSDT"


def test_match_orders_marks_liquidation_exit():
    orders = [
        {
            "orderId": 1,
            "side": "SELL",
            "positionSide": "SHORT",
            "executedQty": "10",
            "avgPrice": "1.0",
            "updateTime": 1000,
            "clientOrderId": "manual-entry",
        },
        {
            "orderId": 2,
            "side": "BUY",
            "positionSide": "SHORT",
            "executedQty": "10",
            "avgPrice": "1.2",
            "updateTime": 2000,
            "clientOrderId": "autoclose-12345",
        },
    ]

    result = match_orders_to_positions(orders, "TESTUSDT", fees_map={}, presorted=True)

    assert len(result) == 1
    assert result[0]["is_liquidation"] is True


def test_is_liquidation_order_by_client_order_id():
    assert is_liquidation_order({"clientOrderId": "autoclose-abc"}) is True
    assert is_liquidation_order({"clientOrderId": "adl_autoclose_x"}) is True
    assert is_liquidation_order({"clientOrderId": "manual-entry"}) is False
