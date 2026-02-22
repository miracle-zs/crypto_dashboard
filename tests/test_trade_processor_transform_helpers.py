from app.trade_processor import TradeDataProcessor


def test_format_holding_time_cn_boundaries():
    processor = TradeDataProcessor.__new__(TradeDataProcessor)
    assert processor._format_holding_time_cn(59) == "59秒"
    assert processor._format_holding_time_cn(60) == "1分0秒"
    assert processor._format_holding_time_cn(3661) == "1小时1分"
    assert processor._format_holding_time_cn(90061) == "1天1小时"


def test_position_to_trade_row_basic_mapping():
    processor = TradeDataProcessor.__new__(TradeDataProcessor)
    pos = {
        "symbol": "BTCUSDT",
        "entry_time": 1_700_000_000_000,
        "exit_time": 1_700_000_600_000,
        "entry_price": 100.0,
        "exit_price": 110.0,
        "qty": 2.0,
        "side": "LONG",
        "pnl": 20.0,
        "fees": -1.0,
        "pnl_before_fees": 21.0,
        "entry_order_id": 1,
        "exit_order_id": "2",
    }
    day_start = processor.get_utc_day_start(pos["entry_time"])
    row = processor._position_to_trade_row(pos, idx=1, open_price_cache={("BTCUSDT", day_start): 90.0})

    assert row["No"] == 1
    assert row["Symbol"] == "BTC"
    assert row["PNL_Net"] == 20
    assert row["Fees"] == -1
    assert row["Close_Type"] == "止盈"
    assert row["Open_Price"] == 90.0


def test_fifo_helpers_trim_and_consume_entries():
    processor = TradeDataProcessor.__new__(TradeDataProcessor)
    entries = [
        {"qty": 2.0, "price": 100.0, "time": 1_700_000_000_000, "order_id": 1},
        {"qty": 3.0, "price": 101.0, "time": 1_700_000_100_000, "order_id": 2},
    ]

    processor._consume_fifo_entries(entries, 2.5)
    assert len(entries) == 1
    assert entries[0]["order_id"] == 2
    assert entries[0]["qty"] == 2.5

    processor._trim_entries_to_target_qty(entries, 1.0)
    assert entries[0]["qty"] == 1.0

    output = processor._build_open_position_output("BTCUSDT", "LONG", entries)
    assert output[0]["symbol"] == "BTC"
    assert output[0]["qty"] == 1.0
