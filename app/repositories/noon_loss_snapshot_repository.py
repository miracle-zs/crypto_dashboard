import json
from datetime import datetime
from typing import Dict, List, Optional


class NoonLossSnapshotRepository:
    def __init__(self, db):
        self.db = db

    def _today_snapshot_date_utc8(self) -> str:
        helper = getattr(self.db, "_today_snapshot_date_utc8", None)
        if callable(helper):
            return helper()
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def _parse_snapshot_dt(value: str | None) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    @staticmethod
    def _row_to_noon_loss_snapshot(row) -> Dict:
        data = dict(row)
        rows_json = data.get("rows_json")
        try:
            data["rows"] = json.loads(rows_json) if rows_json else []
        except Exception:
            data["rows"] = []
        data.pop("rows_json", None)
        return data

    @staticmethod
    def _row_to_noon_loss_review_snapshot(row) -> Dict:
        data = dict(row)
        rows_json = data.get("rows_json")
        try:
            data["rows"] = json.loads(rows_json) if rows_json else []
        except Exception:
            data["rows"] = []
        data.pop("rows_json", None)
        return data

    def get_noon_loss_snapshot_by_date(self, snapshot_date: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT snapshot_date, snapshot_time, loss_count, total_stop_loss, pct_of_balance, balance, rows_json
            FROM noon_loss_snapshots
            WHERE snapshot_date = ?
            LIMIT 1
            """,
            (snapshot_date,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_noon_loss_snapshot(row)

    def get_noon_loss_review_snapshot_by_date(self, snapshot_date: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                snapshot_date, review_time, noon_loss_count, not_cut_count,
                noon_cut_loss_total, hold_loss_total, delta_loss_total,
                pct_of_balance, balance, rows_json
            FROM noon_loss_review_snapshots
            WHERE snapshot_date = ?
            LIMIT 1
            """,
            (snapshot_date,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_noon_loss_review_snapshot(row)

    def list_noon_loss_review_history(self, limit: int = 7) -> List[Dict]:
        conn = self.db._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute(
            """
            WITH all_dates AS (
                SELECT snapshot_date FROM noon_loss_snapshots
                UNION
                SELECT snapshot_date FROM noon_loss_review_snapshots
            )
            SELECT
                d.snapshot_date,
                n.snapshot_time AS noon_snapshot_time,
                COALESCE(n.loss_count, 0) AS noon_loss_count,
                COALESCE(r.noon_cut_loss_total, -COALESCE(n.total_stop_loss, 0.0)) AS noon_cut_loss_total,
                COALESCE(n.pct_of_balance, 0.0) AS noon_pct_of_balance,
                r.review_time,
                COALESCE(r.not_cut_count, 0) AS not_cut_count,
                COALESCE(r.hold_loss_total, 0.0) AS hold_loss_total,
                COALESCE(r.delta_loss_total, 0.0) AS delta_loss_total,
                COALESCE(r.pct_of_balance, 0.0) AS review_pct_of_balance,
                r.rows_json
            FROM all_dates d
            LEFT JOIN noon_loss_snapshots n ON n.snapshot_date = d.snapshot_date
            LEFT JOIN noon_loss_review_snapshots r ON r.snapshot_date = d.snapshot_date
            WHERE d.snapshot_date <= ?
            ORDER BY d.snapshot_date DESC
            LIMIT ?
            """,
            (today, int(limit)),
        )
        rows = cursor.fetchall()
        conn.close()

        result = []
        for row in rows:
            item = dict(row)
            snapshot_time_dt = self._parse_snapshot_dt(item.get("noon_snapshot_time"))
            review_time_dt = self._parse_snapshot_dt(item.get("review_time"))
            review_is_stale = (
                snapshot_time_dt is not None
                and review_time_dt is not None
                and review_time_dt < snapshot_time_dt
            )

            if review_is_stale:
                item["review_time"] = None
                item["not_cut_count"] = 0
                item["hold_loss_total"] = 0.0
                item["delta_loss_total"] = 0.0
                item["review_pct_of_balance"] = 0.0
                item["noon_cut_loss_total"] = -abs(float(item.get("noon_cut_loss_total", 0.0)))

            rows_json = item.get("rows_json")
            try:
                item["rows"] = [] if review_is_stale else (json.loads(rows_json) if rows_json else [])
            except Exception:
                item["rows"] = []
            item.pop("rows_json", None)
            result.append(item)
        return result

    def get_noon_loss_review_history_summary(self) -> Dict:
        rows = self.list_noon_loss_review_history(limit=10000)
        reviewed_rows = [row for row in rows if row.get("review_time")]
        delta_sum_all = sum(float(row.get("delta_loss_total") or 0.0) for row in reviewed_rows)
        return {
            "reviewed_count_all": len(reviewed_rows),
            "delta_sum_all": float(delta_sum_all),
        }
