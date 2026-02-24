import pandas as pd

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
    assert row["PNL_Net"] == 20.0
    assert row["Fees"] == -1.0
    assert row["Close_Type"] == "止盈"
    assert row["Open_Price"] == 90.0


def test_position_to_trade_row_marks_liquidation_close_type():
    processor = TradeDataProcessor.__new__(TradeDataProcessor)
    pos = {
        "symbol": "BTCUSDT",
        "entry_time": 1_700_000_000_000,
        "exit_time": 1_700_000_060_000,
        "entry_price": 100.0,
        "exit_price": 80.0,
        "qty": 1.0,
        "side": "LONG",
        "pnl": -20.123,
        "fees": -0.33,
        "pnl_before_fees": -19.793,
        "entry_order_id": 1,
        "exit_order_id": "2",
        "is_liquidation": True,
    }
    day_start = processor.get_utc_day_start(pos["entry_time"])
    row = processor._position_to_trade_row(pos, idx=1, open_price_cache={("BTCUSDT", day_start): 90.0})

    assert row["Close_Type"] == "爆仓"
    assert row["PNL_Net"] == -20.12
    assert row["Fees"] == -0.33


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


def test_merge_same_entry_positions_preserves_liquidation_close_type():
    processor = TradeDataProcessor.__new__(TradeDataProcessor)
    df = pd.DataFrame(
        [
            {
                "No": 1,
                "Date": "20260225",
                "Entry_Time": "2026-02-25 10:00:00",
                "Exit_Time": "2026-02-25 10:30:00",
                "Holding_Time": "30分0秒",
                "Symbol": "BTC",
                "Side": "LONG",
                "Price_Change_Pct": 0.0,
                "Entry_Amount": 100.0,
                "Entry_Price": 100.0,
                "Exit_Price": 90.0,
                "Qty": 1.0,
                "Fees": -0.1,
                "PNL_Net": -10.1,
                "Close_Type": "爆仓",
                "Return_Rate": "-10.10%",
                "Open_Price": 95.0,
                "PNL_Before_Fees": -10.0,
                "Entry_Order_ID": 1,
                "Exit_Order_ID": "2",
            },
            {
                "No": 2,
                "Date": "20260225",
                "Entry_Time": "2026-02-25 10:00:20",
                "Exit_Time": "2026-02-25 10:30:20",
                "Holding_Time": "30分0秒",
                "Symbol": "BTC",
                "Side": "LONG",
                "Price_Change_Pct": 0.0,
                "Entry_Amount": 200.0,
                "Entry_Price": 100.0,
                "Exit_Price": 92.0,
                "Qty": 2.0,
                "Fees": -0.2,
                "PNL_Net": -16.2,
                "Close_Type": "止损",
                "Return_Rate": "-8.10%",
                "Open_Price": 95.0,
                "PNL_Before_Fees": -16.0,
                "Entry_Order_ID": 3,
                "Exit_Order_ID": "4",
            },
        ]
    )

    merged = processor.merge_same_entry_positions(df)

    assert len(merged) == 1
    assert merged.iloc[0]["Close_Type"] == "爆仓"
