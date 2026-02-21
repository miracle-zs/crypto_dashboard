import pandas as pd

from app.database import Database


def _trade_row(entry_time: str, pnl: float, entry_order_id: int, exit_order_id: str):
    return {
        "No": 1,
        "Date": "20260221",
        "Entry_Time": entry_time,
        "Exit_Time": "2026-02-21 10:15:00",
        "Holding_Time": "15分0秒",
        "Symbol": "BTC",
        "Side": "LONG",
        "Price_Change_Pct": 0.1,
        "Entry_Amount": 1000,
        "Entry_Price": 100,
        "Exit_Price": 110,
        "Qty": 10,
        "Fees": 1,
        "PNL_Net": pnl,
        "Close_Type": "止盈",
        "Return_Rate": "9.90%",
        "Open_Price": 95,
        "PNL_Before_Fees": pnl + 1,
        "Entry_Order_ID": entry_order_id,
        "Exit_Order_ID": exit_order_id,
    }


def test_save_trades_upsert_updates_existing_row(tmp_path):
    db = Database(db_path=str(tmp_path / "save_trades.db"))

    first = pd.DataFrame([_trade_row("2026-02-21 10:00:00", pnl=10, entry_order_id=1, exit_order_id="2")])
    second = pd.DataFrame([_trade_row("2026-02-21 10:00:00", pnl=99, entry_order_id=1, exit_order_id="2")])

    assert db.save_trades(first, overwrite=False) == 1
    assert db.save_trades(second, overwrite=False) == 1

    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) AS c FROM trades")
    assert int(cursor.fetchone()["c"]) == 1
    cursor.execute("SELECT pnl_net FROM trades LIMIT 1")
    assert float(cursor.fetchone()["pnl_net"]) == 99.0
    conn.close()


def test_save_trades_overwrite_replaces_time_window_only(tmp_path):
    db = Database(db_path=str(tmp_path / "save_trades_overwrite.db"))

    rows = pd.DataFrame(
        [
            _trade_row("2026-02-20 10:00:00", pnl=10, entry_order_id=1, exit_order_id="2"),
            _trade_row("2026-02-21 10:00:00", pnl=20, entry_order_id=3, exit_order_id="4"),
            _trade_row("2026-02-22 10:00:00", pnl=30, entry_order_id=5, exit_order_id="6"),
        ]
    )
    db.save_trades(rows, overwrite=False)

    replacement = pd.DataFrame([_trade_row("2026-02-21 10:00:00", pnl=88, entry_order_id=7, exit_order_id="8")])
    assert db.save_trades(replacement, overwrite=True) == 1

    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT entry_time, entry_order_id FROM trades ORDER BY entry_time ASC")
    rows = cursor.fetchall()
    conn.close()

    assert len(rows) == 3
    assert {str(row["entry_time"]) for row in rows} == {
        "2026-02-20 10:00:00",
        "2026-02-21 10:00:00",
        "2026-02-22 10:00:00",
    }
    middle_row = [row for row in rows if str(row["entry_time"]) == "2026-02-21 10:00:00"][0]
    assert int(middle_row["entry_order_id"]) == 7
