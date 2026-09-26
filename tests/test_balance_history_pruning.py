from datetime import datetime, timedelta
from app.database import Database
from app.repositories.trade_write_repository import TradeWriteRepository


def test_prune_balance_history(tmp_path):
    db_path = tmp_path / "test_prune.db"
    db = Database(str(db_path))
    repo = TradeWriteRepository(db)

    conn = db._get_connection()
    cursor = conn.cursor()
    now = datetime.utcnow()

    # 1. Recent data: 2 days ago (5 records within the same hour) -> all should be kept
    t_recent = now - timedelta(days=2)
    for i in range(5):
        ts = (t_recent + timedelta(minutes=i * 5)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("INSERT INTO balance_history (timestamp, balance, wallet_balance) VALUES (?, 100, 100)", (ts,))

    # 2. Medium data: 15 days ago (10 records in the same hour) -> only 1 should be kept
    t_medium = now - timedelta(days=15)
    for i in range(10):
        ts = (t_medium + timedelta(minutes=i * 2)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("INSERT INTO balance_history (timestamp, balance, wallet_balance) VALUES (?, 200, 200)", (ts,))

    # 3. Old data: 120 days ago (5 records on the same day across different hours) -> only 1 should be kept
    t_old = now - timedelta(days=120)
    for i in range(5):
        ts = (t_old + timedelta(hours=i * 2)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("INSERT INTO balance_history (timestamp, balance, wallet_balance) VALUES (?, 300, 300)", (ts,))

    # 4. Ancient data: 400 days ago (3 records) -> should be deleted
    t_ancient = now - timedelta(days=400)
    for i in range(3):
        ts = (t_ancient + timedelta(days=i)).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("INSERT INTO balance_history (timestamp, balance, wallet_balance) VALUES (?, 400, 400)", (ts,))

    conn.commit()
    conn.close()

    deleted = repo.prune_balance_history(days_to_keep_raw=7, days_to_keep_hourly=90, days_to_keep_daily=365)
    assert deleted > 0

    conn = db._get_connection()
    cursor = conn.cursor()

    # Check recent (should have 5)
    cursor.execute("SELECT COUNT(*) FROM balance_history WHERE timestamp > datetime('now', '-7 days')")
    assert cursor.fetchone()[0] == 5

    # Check medium (15 days ago: 10 records turned into 1)
    cursor.execute(
        "SELECT COUNT(*) FROM balance_history WHERE timestamp <= datetime('now', '-7 days') AND timestamp > datetime('now', '-90 days')"
    )
    assert cursor.fetchone()[0] == 1

    # Check old (120 days ago: 5 records turned into 1)
    cursor.execute(
        "SELECT COUNT(*) FROM balance_history WHERE timestamp <= datetime('now', '-90 days') AND timestamp > datetime('now', '-365 days')"
    )
    assert cursor.fetchone()[0] == 1

    # Check ancient (400 days ago: completely deleted)
    cursor.execute("SELECT COUNT(*) FROM balance_history WHERE timestamp <= datetime('now', '-365 days')")
    assert cursor.fetchone()[0] == 0

    conn.close()
