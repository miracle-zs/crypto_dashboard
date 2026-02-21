import json

from app.repositories.open_positions_query import fetch_open_positions


class RiskRepository:
    def __init__(self, db):
        self.db = db

    def get_open_positions(self):
        return fetch_open_positions(self.db)

    def get_profit_alert_candidates(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM open_positions
            WHERE COALESCE(profit_alerted, 0) = 0
            ORDER BY entry_time DESC
            """
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_long_held_alert_candidates(self, entry_before: str, re_alert_before_utc: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM open_positions
            WHERE COALESCE(is_long_term, 0) = 0
              AND entry_time <= ?
              AND (
                  COALESCE(alerted, 0) = 0
                  OR last_alert_time IS NULL
                  OR last_alert_time <= ?
              )
            ORDER BY entry_time ASC
            """,
            (entry_before, re_alert_before_utc),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def save_noon_loss_snapshot(self, snapshot):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        rows_json = json.dumps(snapshot.get("rows", []), ensure_ascii=False)
        cursor.execute(
            """
            INSERT INTO noon_loss_snapshots (
                snapshot_date, snapshot_time, loss_count, total_stop_loss,
                pct_of_balance, balance, rows_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                snapshot_time = excluded.snapshot_time,
                loss_count = excluded.loss_count,
                total_stop_loss = excluded.total_stop_loss,
                pct_of_balance = excluded.pct_of_balance,
                balance = excluded.balance,
                rows_json = excluded.rows_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                str(snapshot.get("snapshot_date")),
                str(snapshot.get("snapshot_time")),
                int(snapshot.get("loss_count", 0)),
                float(snapshot.get("total_stop_loss", 0.0)),
                float(snapshot.get("pct_of_balance", 0.0)),
                float(snapshot.get("balance", 0.0)),
                rows_json,
            ),
        )
        conn.commit()
        conn.close()

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
        data = dict(row)
        rows_json = data.get("rows_json")
        try:
            data["rows"] = json.loads(rows_json) if rows_json else []
        except Exception:
            data["rows"] = []
        data.pop("rows_json", None)
        return data

    def save_noon_loss_review_snapshot(self, snapshot):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        rows_json = json.dumps(snapshot.get("rows", []), ensure_ascii=False)
        cursor.execute(
            """
            INSERT INTO noon_loss_review_snapshots (
                snapshot_date, review_time, noon_loss_count, not_cut_count,
                noon_cut_loss_total, hold_loss_total, delta_loss_total,
                pct_of_balance, balance, rows_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                review_time = excluded.review_time,
                noon_loss_count = excluded.noon_loss_count,
                not_cut_count = excluded.not_cut_count,
                noon_cut_loss_total = excluded.noon_cut_loss_total,
                hold_loss_total = excluded.hold_loss_total,
                delta_loss_total = excluded.delta_loss_total,
                pct_of_balance = excluded.pct_of_balance,
                balance = excluded.balance,
                rows_json = excluded.rows_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                str(snapshot.get("snapshot_date")),
                str(snapshot.get("review_time")),
                int(snapshot.get("noon_loss_count", 0)),
                int(snapshot.get("not_cut_count", 0)),
                float(snapshot.get("noon_cut_loss_total", 0.0)),
                float(snapshot.get("hold_loss_total", 0.0)),
                float(snapshot.get("delta_loss_total", 0.0)),
                float(snapshot.get("pct_of_balance", 0.0)),
                float(snapshot.get("balance", 0.0)),
                rows_json,
            ),
        )
        conn.commit()
        conn.close()

    def set_position_alerted(self, symbol: str, order_id: int):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE open_positions
            SET alerted = 1, last_alert_time = CURRENT_TIMESTAMP
            WHERE symbol = ? AND order_id = ?
            """,
            (symbol, order_id),
        )
        conn.commit()
        conn.close()

    def set_positions_alerted_batch(self, items):
        if not items:
            return 0
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.executemany(
            """
            UPDATE open_positions
            SET alerted = 1, last_alert_time = CURRENT_TIMESTAMP
            WHERE symbol = ? AND order_id = ?
            """,
            [(str(symbol), int(order_id)) for symbol, order_id in items],
        )
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return affected

    def set_position_reentry_alerted(self, symbol: str, order_id: int):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE open_positions
            SET reentry_alerted = 1, reentry_alert_time = CURRENT_TIMESTAMP
            WHERE symbol = ? AND order_id = ?
            """,
            (symbol, order_id),
        )
        conn.commit()
        conn.close()

    def set_positions_reentry_alerted_batch(self, items):
        if not items:
            return 0
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.executemany(
            """
            UPDATE open_positions
            SET reentry_alerted = 1, reentry_alert_time = CURRENT_TIMESTAMP
            WHERE symbol = ? AND order_id = ?
            """,
            [(str(symbol), int(order_id)) for symbol, order_id in items],
        )
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return affected

    def set_position_profit_alerted(self, symbol: str, order_id: int):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE open_positions
            SET profit_alerted = 1, profit_alert_time = CURRENT_TIMESTAMP
            WHERE symbol = ? AND order_id = ?
            """,
            (symbol, order_id),
        )
        conn.commit()
        conn.close()

    def set_positions_profit_alerted_batch(self, items):
        if not items:
            return 0
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.executemany(
            """
            UPDATE open_positions
            SET profit_alerted = 1, profit_alert_time = CURRENT_TIMESTAMP
            WHERE symbol = ? AND order_id = ?
            """,
            [(str(symbol), int(order_id)) for symbol, order_id in items],
        )
        conn.commit()
        affected = cursor.rowcount
        conn.close()
        return affected
