from app.database import Database


def test_repository_calls_database_backcompat():
    from app.repositories.settings_repository import SettingsRepository
    from app.repositories.snapshot_repository import SnapshotRepository
    from app.repositories.trade_repository import TradeRepository

    db = Database()
    trade_repo = TradeRepository(db)
    snapshot_repo = SnapshotRepository(db)
    settings_repo = SettingsRepository(db)

    assert hasattr(trade_repo, "get_open_positions")
    assert hasattr(snapshot_repo, "get_latest_leaderboard_snapshot")
    assert hasattr(settings_repo, "set_position_long_term")


def test_sync_repository_exposes_existing_sync_methods():
    from app.repositories.sync_repository import SyncRepository

    repo = SyncRepository(db=None)
    assert hasattr(repo, "get_last_entry_time")
