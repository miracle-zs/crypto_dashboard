from app.database import Database
from app.repositories.trade_repository import TradeRepository


def test_trade_repository_get_statistics_aggregates_in_single_result(tmp_path):
    db = Database(db_path=str(tmp_path / "trade_repo_stats.db"))
    repo = TradeRepository(db)

    conn = db._get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO trades (
            no, date, entry_time, exit_time, holding_time, symbol, side,
            price_change_pct, entry_amount, entry_price, exit_price, qty,
            fees, pnl_net, close_type, return_rate, open_price,
            pnl_before_fees, entry_order_id, exit_order_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1, "20260221", "2026-02-21 10:00:00", "2026-02-21 10:05:00", "5m",
            "BTC", "LONG", 0.1, 100.0, 100.0, 101.0, 1.0,
            0.1, 0.9, "tp", "0.9%", 99.0, 1.0, 101, "201",
        ),
    )
    cur.execute(
        """
        INSERT INTO trades (
            no, date, entry_time, exit_time, holding_time, symbol, side,
            price_change_pct, entry_amount, entry_price, exit_price, qty,
            fees, pnl_net, close_type, return_rate, open_price,
            pnl_before_fees, entry_order_id, exit_order_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            2, "20260222", "2026-02-22 11:00:00", "2026-02-22 11:10:00", "10m",
            "ETH", "SHORT", -0.2, 200.0, 200.0, 198.0, 1.0,
            0.2, -2.2, "sl", "-1.1%", 201.0, -2.0, 102, "202",
        ),
    )
    conn.commit()
    conn.close()

    stats = repo.get_statistics()
    assert stats == {
        "total_trades": 2,
        "earliest_trade": "2026-02-21 10:00:00",
        "latest_trade": "2026-02-22 11:00:00",
        "unique_symbols": 2,
    }


def test_cross_month_trade_realized_pnl_date_belongs_to_exit_month():
    from zoneinfo import ZoneInfo
    from app.core.trade_position_utils import position_to_trade_row

    utc8 = ZoneInfo("Asia/Shanghai")
    # Position opened in September, exited in October
    pos = {
        "symbol": "BTCUSDT",
        "side": "LONG",
        "entry_price": 50000.0,
        "exit_price": 48000.0,
        "entry_time": 1790555462000,  # 2026-09-28
        "exit_time": 1791000000000,   # 2026-10-03
        "qty": 1.0,
        "pnl": -2000.0,
        "pnl_before_fees": -2000.0,
        "entry_order_id": 1,
        "exit_order_id": 2,
    }

    formatted = position_to_trade_row(
        pos,
        idx=1,
        open_price_cache={},
        utc8=utc8,
        get_utc_day_start=lambda t: 0,
    )
    # The Date MUST belong to the exit date (October 2026), NOT September!
    assert formatted["Date"].startswith("202610")
    assert formatted["Exit_Time"].startswith("2026-10")

