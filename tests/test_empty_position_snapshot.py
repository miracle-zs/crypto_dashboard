from app.database import Database
from app.repositories.sync_write_repository import SyncWriteRepository


def test_empty_position_snapshot_preserves_cleared_state(tmp_path):
    db_path = tmp_path / "test_snapshot.db"
    db = Database(str(db_path))
    repo = SyncWriteRepository(db)

    # 1. First snapshot: 1 open position at t1
    t1 = "2026-09-26 10:00:00"
    pos1 = [{
        "symbol": "BTCUSDT",
        "position_side": "LONG",
        "qty": 1.0,
        "entry_price": 50000.0,
        "mark_price": 51000.0,
        "unrealized_pnl": 1000.0,
    }]
    repo.save_position_snapshots(t1, pos1)
    latest1 = repo.get_latest_position_snapshots()
    assert len(latest1) == 1
    assert latest1[0]["symbol"] == "BTCUSDT"

    # 2. Second snapshot: Account is completely flat (0 positions) at t2
    t2 = "2026-09-26 10:30:00"
    repo.save_position_snapshots(t2, [])

    # The latest snapshot should now be empty [], NOT the stale BTCUSDT from t1!
    latest2 = repo.get_latest_position_snapshots()
    assert latest2 == [], f"Expected empty list after flat snapshot, but got {latest2}"
