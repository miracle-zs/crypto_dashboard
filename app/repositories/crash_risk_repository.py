from app.repositories.snapshot_repository import SnapshotRepository


class CrashRiskRepository:
    def __init__(self, db):
        self.db = db
        self._snapshots = SnapshotRepository(db)

    def get_latest_leaderboard_snapshot(self):
        return self._snapshots.get_latest_leaderboard_snapshot()

    @staticmethod
    def _extract_symbols(snapshot):
        if not snapshot:
            return []
        rows = snapshot.get("rows", []) or []
        symbols = []
        for row in rows:
            symbol = str(row.get("symbol", "")).upper().strip()
            if symbol:
                symbols.append(symbol)
        return symbols

    def get_candidate_symbols_snapshot_union(self):
        leaderboard_snapshot = self._snapshots.get_latest_leaderboard_snapshot()
        rebound_14d_snapshot = self._snapshots.get_latest_rebound_7d_snapshot()
        rebound_30d_snapshot = self._snapshots.get_latest_rebound_30d_snapshot()
        rebound_60d_snapshot = self._snapshots.get_latest_rebound_60d_snapshot()
        rebound_365d_snapshot = self._snapshots.get_latest_rebound_365d_snapshot()

        ordered_unique_symbols = []
        seen = set()
        for snapshot in (
            leaderboard_snapshot,
            rebound_14d_snapshot,
            rebound_30d_snapshot,
            rebound_60d_snapshot,
            rebound_365d_snapshot,
        ):
            for symbol in self._extract_symbols(snapshot):
                if symbol in seen:
                    continue
                seen.add(symbol)
                ordered_unique_symbols.append(symbol)

        primary_snapshot = leaderboard_snapshot or rebound_14d_snapshot or rebound_30d_snapshot or rebound_60d_snapshot or rebound_365d_snapshot
        if not primary_snapshot:
            return None

        return {
            "source": "leaderboard_and_rebound_union",
            "snapshot_date": primary_snapshot.get("snapshot_date"),
            "snapshot_time": primary_snapshot.get("snapshot_time"),
            "window_start_utc": primary_snapshot.get("window_start_utc"),
            "symbols": ordered_unique_symbols,
        }
