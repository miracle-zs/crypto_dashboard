class SnapshotRepository:
    def __init__(self, db):
        self.db = db

    def get_latest_leaderboard_snapshot(self):
        return self.db.get_latest_leaderboard_snapshot()

    def get_leaderboard_snapshot_by_date(self, snapshot_date: str):
        return self.db.get_leaderboard_snapshot_by_date(snapshot_date)

    def get_leaderboard_snapshots_between(self, start_date: str, end_date: str):
        return self.db.get_leaderboard_snapshots_between(start_date, end_date)

    def get_leaderboard_daily_metrics(self, snapshot_date: str):
        return self.db.get_leaderboard_daily_metrics(snapshot_date)

    def upsert_leaderboard_daily_metrics_for_date(self, snapshot_date: str):
        return self.db.upsert_leaderboard_daily_metrics_for_date(snapshot_date)

    def get_noon_loss_snapshot_by_date(self, snapshot_date: str):
        return self.db.get_noon_loss_snapshot_by_date(snapshot_date)

    def get_noon_loss_review_snapshot_by_date(self, snapshot_date: str):
        return self.db.get_noon_loss_review_snapshot_by_date(snapshot_date)

    def get_rebound_7d_snapshot_by_date(self, snapshot_date: str):
        return self.db.get_rebound_7d_snapshot_by_date(snapshot_date)

    def get_latest_rebound_7d_snapshot(self):
        return self.db.get_latest_rebound_7d_snapshot()

    def list_rebound_7d_snapshot_dates(self, limit: int):
        return self.db.list_rebound_7d_snapshot_dates(limit)

    def get_rebound_30d_snapshot_by_date(self, snapshot_date: str):
        return self.db.get_rebound_30d_snapshot_by_date(snapshot_date)

    def get_latest_rebound_30d_snapshot(self):
        return self.db.get_latest_rebound_30d_snapshot()

    def list_rebound_30d_snapshot_dates(self, limit: int):
        return self.db.list_rebound_30d_snapshot_dates(limit)

    def get_rebound_60d_snapshot_by_date(self, snapshot_date: str):
        return self.db.get_rebound_60d_snapshot_by_date(snapshot_date)

    def get_latest_rebound_60d_snapshot(self):
        return self.db.get_latest_rebound_60d_snapshot()

    def list_rebound_60d_snapshot_dates(self, limit: int):
        return self.db.list_rebound_60d_snapshot_dates(limit)
