from datetime import datetime
from zoneinfo import ZoneInfo


class WatchNotesRepository:
    def __init__(self, db):
        self.db = db

    def add_watch_note(self, symbol: str):
        normalized_symbol = (symbol or "").strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol 不能为空")

        today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")
        noted_at = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, symbol, noted_at
            FROM watch_notes
            WHERE symbol = ? AND substr(noted_at, 1, 10) = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (normalized_symbol, today),
        )
        existing = cursor.fetchone()
        if existing:
            conn.close()
            item = dict(existing)
            item["exists_today"] = True
            return item

        cursor.execute(
            """
            INSERT INTO watch_notes (symbol, noted_at)
            VALUES (?, ?)
            """,
            (normalized_symbol, noted_at),
        )
        note_id = cursor.lastrowid
        conn.commit()
        conn.close()
        return {
            "id": note_id,
            "symbol": normalized_symbol,
            "noted_at": noted_at,
            "exists_today": False,
        }

    def get_watch_notes(self, limit: int = 200):
        safe_limit = max(1, min(int(limit), 1000))
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, symbol, noted_at
            FROM watch_notes
            ORDER BY noted_at DESC, id DESC
            LIMIT ?
            """,
            (safe_limit,),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def delete_watch_note(self, note_id: int):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM watch_notes WHERE id = ?", (int(note_id),))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted
