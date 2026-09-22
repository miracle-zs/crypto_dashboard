from datetime import datetime, timedelta, timezone
from app.database import Database
from app.repositories.trade_read_repository import TradeReadRepository


def test_get_balance_history_downsamples_large_result_sets(tmp_path):
    db_file = tmp_path / "test_balance.db"
    db = Database(str(db_file))
    repo = TradeReadRepository(db)

    conn = db._get_connection()

    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    # Insert 3000 rows at 1-minute intervals
    rows = []
    for i in range(3000):
        t = (base_time + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
        rows.append((t, 1000.0 + i, 1000.0 + i))

    conn.executemany(
        "INSERT INTO balance_history (timestamp, balance, wallet_balance) VALUES (?, ?, ?)",
        rows,
    )
    conn.commit()
    conn.close()

    start_time = base_time
    end_time = base_time + timedelta(minutes=2999)

    # Query with downsampling max_points=1000
    results = repo.get_balance_history(start_time=start_time, end_time=end_time, max_points=1000)

    # Must be downsampled to around 1000 points, not 3000
    assert 900 <= len(results) <= 1100
    # First point and last point must be preserved
    assert results[0]["balance"] == 1000.0
    assert results[-1]["balance"] == 3999.0
