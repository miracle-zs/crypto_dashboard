from app.database import Database
from app.repositories.trade_repository import TradeRepository


def test_get_transfer_timeline_returns_only_visible_transfer_rows(tmp_path):
    db = Database(db_path=str(tmp_path / "transfer_timeline.db"))
    repo = TradeRepository(db)

    conn = db._get_connection()
    cur = conn.cursor()
    cur.executemany(
        """
        INSERT INTO transfers (timestamp, amount, type, description, source_uid)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            ("2026-02-21 10:00:00", 100.0, "manual", "deposit", None),
            ("2026-02-21 11:00:00", -50.0, "auto", "ignored-auto", None),
            ("2026-02-21 12:00:00", 30.0, "auto", "income-transfer", "TRANSFER:1"),
        ],
    )
    conn.commit()
    conn.close()

    rows = repo.get_transfer_timeline()
    assert rows == [
        {"timestamp": "2026-02-21 10:00:00", "amount": 100.0},
        {"timestamp": "2026-02-21 12:00:00", "amount": 30.0},
    ]
