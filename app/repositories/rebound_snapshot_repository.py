import json
from datetime import datetime
from typing import Dict


class ReboundSnapshotRepository:
    def __init__(self, db):
        self.db = db

    def _today_snapshot_date_utc8(self) -> str:
        helper = getattr(self.db, "_today_snapshot_date_utc8", None)
        if callable(helper):
            return helper()
        return datetime.now().strftime("%Y-%m-%d")

    def _save_rebound_snapshot(self, table_name: str, snapshot: Dict):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        rows_json = json.dumps(snapshot.get("rows", []), ensure_ascii=False)
        all_rows_json = json.dumps(snapshot.get("all_rows", []), ensure_ascii=False)
        cursor.execute(
            f"""
            INSERT INTO {table_name} (
                snapshot_date, snapshot_time, window_start_utc,
                candidates, effective, top_count, rows_json, all_rows_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                snapshot_time = excluded.snapshot_time,
                window_start_utc = excluded.window_start_utc,
                candidates = excluded.candidates,
                effective = excluded.effective,
                top_count = excluded.top_count,
                rows_json = excluded.rows_json,
                all_rows_json = excluded.all_rows_json,
                created_at = CURRENT_TIMESTAMP
            """,
            (
                str(snapshot.get("snapshot_date")),
                str(snapshot.get("snapshot_time")),
                str(snapshot.get("window_start_utc", "")),
                int(snapshot.get("candidates", 0)),
                int(snapshot.get("effective", 0)),
                int(snapshot.get("top", 0)),
                rows_json,
                all_rows_json,
            ),
        )
        conn.commit()
        conn.close()

    def save_rebound_7d_snapshot(self, snapshot):
        return self._save_rebound_snapshot("rebound_7d_snapshots", snapshot)

    def save_rebound_30d_snapshot(self, snapshot):
        return self._save_rebound_snapshot("rebound_30d_snapshots", snapshot)

    def save_rebound_60d_snapshot(self, snapshot):
        return self._save_rebound_snapshot("rebound_60d_snapshots", snapshot)

    @staticmethod
    def _row_to_rebound_snapshot(row) -> Dict:
        data = dict(row)
        rows_json = data.get("rows_json")
        all_rows_json = data.get("all_rows_json")
        try:
            data["rows"] = json.loads(rows_json) if rows_json else []
        except Exception:
            data["rows"] = []
        try:
            data["all_rows"] = json.loads(all_rows_json) if all_rows_json else []
        except Exception:
            data["all_rows"] = []
        data.pop("rows_json", None)
        data.pop("all_rows_json", None)
        data["top"] = data.pop("top_count", 0)
        return data

    def _get_latest_rebound_snapshot(self, table_name: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute(
            f"""
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, all_rows_json
            FROM {table_name}
            WHERE snapshot_date <= ?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT 1
            """,
            (today,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_rebound_snapshot(row)

    def get_latest_rebound_7d_snapshot(self):
        return self._get_latest_rebound_snapshot("rebound_7d_snapshots")

    def get_latest_rebound_30d_snapshot(self):
        return self._get_latest_rebound_snapshot("rebound_30d_snapshots")

    def get_latest_rebound_60d_snapshot(self):
        return self._get_latest_rebound_snapshot("rebound_60d_snapshots")

    def _get_rebound_snapshot_by_date(self, table_name: str, snapshot_date: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, all_rows_json
            FROM {table_name}
            WHERE snapshot_date = ?
            LIMIT 1
            """,
            (snapshot_date,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_rebound_snapshot(row)

    def get_rebound_7d_snapshot_by_date(self, snapshot_date: str):
        return self._get_rebound_snapshot_by_date("rebound_7d_snapshots", snapshot_date)

    def get_rebound_30d_snapshot_by_date(self, snapshot_date: str):
        return self._get_rebound_snapshot_by_date("rebound_30d_snapshots", snapshot_date)

    def get_rebound_60d_snapshot_by_date(self, snapshot_date: str):
        return self._get_rebound_snapshot_by_date("rebound_60d_snapshots", snapshot_date)

    def _list_rebound_snapshot_dates(self, table_name: str, limit: int = 90):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute(
            f"""
            SELECT snapshot_date
            FROM {table_name}
            WHERE snapshot_date <= ?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT ?
            """,
            (today, int(limit)),
        )
        rows = cursor.fetchall()
        conn.close()
        return [str(row["snapshot_date"]) for row in rows]

    def list_rebound_7d_snapshot_dates(self, limit: int):
        return self._list_rebound_snapshot_dates("rebound_7d_snapshots", limit)

    def list_rebound_30d_snapshot_dates(self, limit: int):
        return self._list_rebound_snapshot_dates("rebound_30d_snapshots", limit)

    def list_rebound_60d_snapshot_dates(self, limit: int):
        return self._list_rebound_snapshot_dates("rebound_60d_snapshots", limit)
