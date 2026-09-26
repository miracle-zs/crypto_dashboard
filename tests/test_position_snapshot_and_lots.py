from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from app.database import Database
from app.repositories.sync_write_repository import SyncWriteRepository


def test_hedge_incomplete_positions_do_not_collapse():
    """Verify that two incomplete positions (LONG and SHORT) of the same symbol do not collapse."""
    with TemporaryDirectory() as tmp:
        db = Database(str(Path(tmp) / "probe.db"))
        repo = SyncWriteRepository(db)

        base = {
            "date": "20260926",
            "symbol": "BTC",
            "entry_time": "2026-09-20 00:00:00",
            "entry_price": 50000.0,
            "qty": 1.0,
            "entry_amount": 50000.0,
            "order_id": 0,
            "is_incomplete": True,
        }

        # Save both LONG and SHORT with order_id=0
        count = repo.save_open_positions([{**base, "side": "LONG"}, {**base, "side": "SHORT"}])
        assert count == 2

        conn = db._get_connection()
        try:
            rows = [dict(row) for row in conn.execute("SELECT * FROM open_positions ORDER BY side ASC")]
        finally:
            conn.close()

        # Both sides must survive!
        assert len(rows) == 2
        assert rows[0]["side"] == "LONG"
        assert rows[1]["side"] == "SHORT"

        # is_incomplete must be preserved
        assert "is_incomplete" in rows[0]
        assert rows[0]["is_incomplete"] == 1
        assert rows[1]["is_incomplete"] == 1


def test_position_snapshot_persistence():
    """Verify that position_snapshots table correctly persists exchange authoritative facts."""
    with TemporaryDirectory() as tmp:
        db = Database(str(Path(tmp) / "probe.db"))
        repo = SyncWriteRepository(db)

        snapshot_time = "2026-09-26 10:00:00"
        positions = [
            {
                "symbol": "BTCUSDT",
                "position_side": "LONG",
                "qty": 1.5,
                "entry_price": 60000.0,
                "mark_price": 62000.0,
                "unrealized_pnl": 3000.0,
                "liquidation_price": 45000.0,
                "leverage": 10,
                "margin_type": "cross",
            },
            {
                "symbol": "ETHUSDT",
                "position_side": "SHORT",
                "qty": 10.0,
                "entry_price": 3000.0,
                "mark_price": 2900.0,
                "unrealized_pnl": 1000.0,
                "liquidation_price": 3500.0,
                "leverage": 20,
                "margin_type": "cross",
            },
        ]

        saved_count = repo.save_position_snapshots(snapshot_time, positions)
        assert saved_count == 2

        latest = repo.get_latest_position_snapshots()
        assert len(latest) == 2
        btc = next(p for p in latest if p["symbol"] == "BTCUSDT")
        assert btc["qty"] == 1.5
        assert btc["entry_price"] == 60000.0
        assert btc["unrealized_pnl"] == 3000.0
        assert btc["position_side"] == "LONG"
