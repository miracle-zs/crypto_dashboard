from app.trade_processor import TradeDataProcessor


def test_match_orders_to_positions_presorted_matches_default():
    processor = TradeDataProcessor.__new__(TradeDataProcessor)
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

    default_result = processor.match_orders_to_positions(orders, "BTCUSDT", fees_map=fees_map)
    sorted_orders = sorted(orders, key=lambda x: x["updateTime"])
    presorted_result = processor.match_orders_to_positions(
        sorted_orders, "BTCUSDT", fees_map=fees_map, presorted=True
    )

    assert default_result == presorted_result
    assert len(default_result) == 1
    assert default_result[0]["symbol"] == "BTCUSDT"
