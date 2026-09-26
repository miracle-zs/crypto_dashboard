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

    def save_crash_risk_snapshot(self, snapshot: dict) -> None:
        if not snapshot:
            return
        import json
        from datetime import datetime

        source = snapshot.get("source_snapshot") or {}
        snapshot_date = str(source.get("snapshot_date") or snapshot.get("as_of") or datetime.now().strftime("%Y-%m-%d"))
        snapshot_time = str(source.get("snapshot_time") or snapshot.get("as_of") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        window_start_utc = str(source.get("window_start_utc") or "")
        payload_json = json.dumps(snapshot, ensure_ascii=False)

        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO crash_risk_snapshots (
                    snapshot_date, snapshot_time, window_start_utc, payload_json, created_at
                ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(snapshot_date) DO UPDATE SET
                    snapshot_time = excluded.snapshot_time,
                    window_start_utc = excluded.window_start_utc,
                    payload_json = excluded.payload_json,
                    created_at = CURRENT_TIMESTAMP
                """,
                (snapshot_date, snapshot_time, window_start_utc, payload_json),
            )
            conn.commit()
        finally:
            conn.close()

    def get_latest_crash_risk_snapshot(self) -> dict | None:
        import json

        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT payload_json FROM crash_risk_snapshots ORDER BY id DESC LIMIT 1"
            )
            row = cursor.fetchone()
            if row and row["payload_json"]:
                return json.loads(row["payload_json"])
            return None
        except Exception:
            return None
        finally:
            conn.close()

