from app.database import Database
from app.repositories.sync_repository import SyncRepository


def test_sync_repository_batch_success_and_failure_updates(tmp_path):
    db = Database(db_path=str(tmp_path / "sync_state.db"))
    repo = SyncRepository(db)

    repo.update_symbol_sync_success_batch(["BTCUSDT", "ETHUSDT"], end_ms=1000)
    repo.update_symbol_sync_failure_batch({"BTCUSDT": "timeout", "XRPUSDT": "auth"}, end_ms=2000)

    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT symbol, last_success_end_ms, last_attempt_end_ms, last_error
        FROM symbol_sync_state
        ORDER BY symbol
        """
    )
    rows = {row["symbol"]: dict(row) for row in cursor.fetchall()}
    conn.close()

    assert set(rows.keys()) == {"BTCUSDT", "ETHUSDT", "XRPUSDT"}

    assert rows["BTCUSDT"]["last_success_end_ms"] == 1000
    assert rows["BTCUSDT"]["last_attempt_end_ms"] == 2000
    assert rows["BTCUSDT"]["last_error"] == "timeout"

    assert rows["ETHUSDT"]["last_success_end_ms"] == 1000
    assert rows["ETHUSDT"]["last_attempt_end_ms"] == 1000
    assert rows["ETHUSDT"]["last_error"] is None

    assert rows["XRPUSDT"]["last_success_end_ms"] is None
    assert rows["XRPUSDT"]["last_attempt_end_ms"] == 2000
    assert rows["XRPUSDT"]["last_error"] == "auth"


def test_save_open_positions_caches_state_columns(tmp_path):
    db = Database(db_path=str(tmp_path / "positions_cache.db"))
    repo = SyncRepository(db)

    rows = [
        {
            "date": "20260221",
            "symbol": "BTC",
            "side": "LONG",
            "entry_time": "2026-02-21 10:00:00",
            "entry_price": 100.0,
            "qty": 1.0,
            "entry_amount": 100.0,
            "order_id": 1,
        }
    ]
    repo.save_open_positions(rows)
    assert getattr(repo, "_open_positions_state_columns", None)


def test_save_open_positions_preserves_alert_state_for_surviving_rows(tmp_path):
    db = Database(db_path=str(tmp_path / "positions_state_preserve.db"))
    repo = SyncRepository(db)

    initial_rows = [
        {
            "date": "20260221",
            "symbol": "BTC",
            "side": "LONG",
            "entry_time": "2026-02-21 10:00:00",
            "entry_price": 100.0,
            "qty": 1.0,
            "entry_amount": 100.0,
            "order_id": 1,
        },
        {
            "date": "20260221",
            "symbol": "ETH",
            "side": "SHORT",
            "entry_time": "2026-02-21 11:00:00",
            "entry_price": 200.0,
            "qty": 2.0,
            "entry_amount": 400.0,
            "order_id": 2,
        },
    ]
    repo.save_open_positions(initial_rows)

    conn = db._get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE open_positions
        SET alerted = 1, profit_alerted = 1, reentry_alerted = 1
        WHERE symbol = 'BTC' AND order_id = 1
        """
    )
    conn.commit()
    conn.close()

    repo.save_open_positions([initial_rows[0]])

    conn = db._get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, order_id, alerted, profit_alerted, reentry_alerted
        FROM open_positions
        ORDER BY symbol
        """
    )
    rows = [dict(row) for row in cur.fetchall()]
    conn.close()

    assert rows == [
        {
            "symbol": "BTC",
            "order_id": 1,
            "alerted": 1,
            "profit_alerted": 1,
            "reentry_alerted": 1,
        }
    ]


def test_sync_repository_status_and_watermark_queries(tmp_path):
    db = Database(db_path=str(tmp_path / "sync_status.db"))
    repo = SyncRepository(db)

    import pandas as pd

    df = pd.DataFrame(
        [
            {
                "No": 1,
                "Date": "20260221",
                "Entry_Time": "2026-02-21 10:00:00",
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
                "PNL_Net": 99,
                "Close_Type": "止盈",
                "Return_Rate": "9.90%",
                "Open_Price": 95,
                "PNL_Before_Fees": 100,
                "Entry_Order_ID": 11,
                "Exit_Order_ID": "22",
            }
        ]
    )
    repo.save_trades(df, overwrite=False)
    repo.update_sync_status(status="idle")

    assert repo.get_last_entry_time() == "2026-02-21 10:00:00"
    repo.update_symbol_sync_success(symbol="BTCUSDT", end_ms=12345)
    marks = repo.get_symbol_sync_watermarks(["BTCUSDT", "ETHUSDT"])
    assert marks["BTCUSDT"] == 12345
    assert marks["ETHUSDT"] is None
