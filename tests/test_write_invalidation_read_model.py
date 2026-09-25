from app.core.read_model import get_read_model, reset_read_model


def test_sync_repository_save_open_positions_invalidates_read_model():
    reset_read_model()
    model = get_read_model()
    model.publish("positions:open:2026-01-01", {"stale": True})

    class FakeWrite:
        _open_positions_state_columns = None

        def save_open_positions(self, rows):
            return len(rows or [])

    class FakeRead:
        pass

    from app.repositories.sync_repository import SyncRepository

    repo = SyncRepository(db=object())
    repo._write = FakeWrite()
    repo._read = FakeRead()

    repo.save_open_positions([{"symbol": "BTCUSDT"}])
    assert model.get("positions:open:2026-01-01") is None


def test_leaderboard_snapshot_save_invalidates_read_model():
    reset_read_model()
    model = get_read_model()
    model.publish("leaderboard:snapshot:latest", {"stale": True})

    class FakeDb:
        def _get_connection(self):
            raise AssertionError("should not hit db after commit path simplified")

    # 直接测失效逻辑：save 成功后 prefix 失效
    from app.repositories.leaderboard_snapshot_repository import LeaderboardSnapshotRepository

    class FakeCursor:
        def execute(self, *args, **kwargs):
            return None

    class FakeConn:
        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

        def close(self):
            return None

    class FakeDb2:
        def _get_connection(self):
            return FakeConn()

    repo = LeaderboardSnapshotRepository(FakeDb2())
    repo.save_leaderboard_snapshot({"snapshot_date": "2026-01-01", "rows": [], "losers_rows": [], "all_rows": []})
    assert model.get("leaderboard:snapshot:latest") is None


def test_positions_service_invalidate_helper():
    reset_read_model()
    model = get_read_model()
    model.publish("positions:open:2026-01-01", {"x": 1})

    from app.services.positions_service import PositionsService

    PositionsService.invalidate_open_positions_cache()
    assert model.get("positions:open:2026-01-01") is None
