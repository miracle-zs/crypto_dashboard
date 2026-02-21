from app.database import Database


def test_repository_calls_database_backcompat():
    from app.repositories.settings_repository import SettingsRepository
    from app.repositories.snapshot_repository import SnapshotRepository
    from app.repositories.trade_repository import TradeRepository
    from app.repositories.watchnotes_repository import WatchNotesRepository

    db = Database()
    trade_repo = TradeRepository(db)
    snapshot_repo = SnapshotRepository(db)
    settings_repo = SettingsRepository(db)
    watchnotes_repo = WatchNotesRepository(db)

    assert hasattr(trade_repo, "get_open_positions")
    assert hasattr(snapshot_repo, "get_latest_leaderboard_snapshot")
    assert hasattr(settings_repo, "set_position_long_term")
    assert hasattr(watchnotes_repo, "get_watch_notes")


def test_sync_repository_exposes_existing_sync_methods():
    from app.repositories.sync_repository import SyncRepository

    repo = SyncRepository(db=None)
    assert hasattr(repo, "get_last_entry_time")


def test_settings_repository_can_mark_long_term(tmp_path):
    from app.repositories.settings_repository import SettingsRepository

    db = Database(db_path=str(tmp_path / "settings_repo.db"))
    repo = SettingsRepository(db)

    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO open_positions (date, symbol, side, entry_time, entry_price, qty, entry_amount, order_id, is_long_term)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("20260221", "BTC", "LONG", "2026-02-21 10:00:00", 100.0, 1.0, 100.0, 9, 0),
    )
    conn.commit()
    conn.close()

    repo.set_position_long_term("BTC", 9, True)

    conn = db._get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT is_long_term FROM open_positions WHERE symbol = ? AND order_id = ?", ("BTC", 9))
    row = cursor.fetchone()
    conn.close()
    assert int(row["is_long_term"]) == 1
