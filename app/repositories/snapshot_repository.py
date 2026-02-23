from .leaderboard_snapshot_repository import LeaderboardSnapshotRepository
from .noon_loss_snapshot_repository import NoonLossSnapshotRepository
from .rebound_snapshot_repository import ReboundSnapshotRepository


class SnapshotRepository:
    """Backward-compatible facade around domain snapshot repositories."""

    def __init__(self, db):
        self.db = db
        self._leaderboard = LeaderboardSnapshotRepository(db)
        self._noon_loss = NoonLossSnapshotRepository(db)
        self._rebound = ReboundSnapshotRepository(db)

    def save_leaderboard_snapshot(self, snapshot):
        return self._leaderboard.save_leaderboard_snapshot(snapshot)

    def get_latest_leaderboard_snapshot(self):
        return self._leaderboard.get_latest_leaderboard_snapshot()

    def get_leaderboard_snapshot_by_date(self, snapshot_date: str):
        return self._leaderboard.get_leaderboard_snapshot_by_date(snapshot_date)

    def list_leaderboard_snapshot_dates(self, limit: int):
        return self._leaderboard.list_leaderboard_snapshot_dates(limit)

    def get_leaderboard_snapshots_between(self, start_date: str, end_date: str):
        return self._leaderboard.get_leaderboard_snapshots_between(start_date, end_date)

    def build_leaderboard_daily_metrics(self, snapshot_date: str, drop_threshold_pct: float = -10.0):
        return self._leaderboard.build_leaderboard_daily_metrics(snapshot_date, drop_threshold_pct)

    def upsert_leaderboard_daily_metrics_for_date(self, snapshot_date: str):
        return self._leaderboard.upsert_leaderboard_daily_metrics_for_date(snapshot_date)

    def upsert_leaderboard_daily_metrics_for_dates(self, dates):
        return self._leaderboard.upsert_leaderboard_daily_metrics_for_dates(dates)

    def get_leaderboard_daily_metrics(self, snapshot_date: str):
        return self._leaderboard.get_leaderboard_daily_metrics(snapshot_date)

    def get_leaderboard_daily_metrics_by_dates(self, dates):
        return self._leaderboard.get_leaderboard_daily_metrics_by_dates(dates)

    def get_noon_loss_snapshot_by_date(self, snapshot_date: str):
        return self._noon_loss.get_noon_loss_snapshot_by_date(snapshot_date)

    def get_noon_loss_review_snapshot_by_date(self, snapshot_date: str):
        return self._noon_loss.get_noon_loss_review_snapshot_by_date(snapshot_date)

    def list_noon_loss_review_history(self, limit: int = 7):
        return self._noon_loss.list_noon_loss_review_history(limit)

    def get_noon_loss_review_history_summary(self):
        return self._noon_loss.get_noon_loss_review_history_summary()

    def save_rebound_7d_snapshot(self, snapshot):
        return self._rebound.save_rebound_7d_snapshot(snapshot)

    def save_rebound_30d_snapshot(self, snapshot):
        return self._rebound.save_rebound_30d_snapshot(snapshot)

    def save_rebound_60d_snapshot(self, snapshot):
        return self._rebound.save_rebound_60d_snapshot(snapshot)

    def get_latest_rebound_7d_snapshot(self):
        return self._rebound.get_latest_rebound_7d_snapshot()

    def get_latest_rebound_30d_snapshot(self):
        return self._rebound.get_latest_rebound_30d_snapshot()

    def get_latest_rebound_60d_snapshot(self):
        return self._rebound.get_latest_rebound_60d_snapshot()

    def get_rebound_7d_snapshot_by_date(self, snapshot_date: str):
        return self._rebound.get_rebound_7d_snapshot_by_date(snapshot_date)

    def get_rebound_30d_snapshot_by_date(self, snapshot_date: str):
        return self._rebound.get_rebound_30d_snapshot_by_date(snapshot_date)

    def get_rebound_60d_snapshot_by_date(self, snapshot_date: str):
        return self._rebound.get_rebound_60d_snapshot_by_date(snapshot_date)

    def list_rebound_7d_snapshot_dates(self, limit: int):
        return self._rebound.list_rebound_7d_snapshot_dates(limit)

    def list_rebound_30d_snapshot_dates(self, limit: int):
        return self._rebound.list_rebound_30d_snapshot_dates(limit)

    def list_rebound_60d_snapshot_dates(self, limit: int):
        return self._rebound.list_rebound_60d_snapshot_dates(limit)
