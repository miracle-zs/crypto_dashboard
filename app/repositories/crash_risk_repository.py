from app.repositories.snapshot_repository import SnapshotRepository


class CrashRiskRepository:
    def __init__(self, db):
        self.db = db
        self._snapshots = SnapshotRepository(db)

    def get_latest_leaderboard_snapshot(self):
        return self._snapshots.get_latest_leaderboard_snapshot()
