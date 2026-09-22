from app.repositories.open_positions_query import fetch_open_position_symbols, fetch_open_positions
from app.repositories.trade_repository import TradeRepository


class SyncReadRepository:
    def __init__(self, db):
        self.db = db
        self.trade_repo = TradeRepository(db) if db is not None else None

    def get_last_entry_time(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(entry_time) FROM trades")
        row = cursor.fetchone()
        conn.close()
        return row[0] if row and row[0] else None

    def get_symbol_sync_watermarks(self, symbols):
        if not symbols:
            return {}

        normalized = [str(symbol).upper() for symbol in symbols]
        placeholders = ",".join("?" for _ in normalized)

        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT symbol, last_success_end_ms
            FROM symbol_sync_state
            WHERE symbol IN ({placeholders})
            """,
            tuple(normalized),
        )
        rows = cursor.fetchall()
        conn.close()

        watermarks = {symbol: None for symbol in normalized}
        for row in rows:
            watermarks[row["symbol"]] = row["last_success_end_ms"]
        return watermarks

    def get_sync_cursor(self, stream: str, symbol: str = ""):
        conn = self.db._get_connection()
        try:
            row = conn.execute(
                """
                SELECT last_id, last_time_ms
                FROM sync_cursors
                WHERE stream = ? AND symbol = ?
                """,
                (str(stream), str(symbol or "").upper()),
            ).fetchone()
        finally:
            conn.close()
        if not row:
            return None
        return {
            "last_id": int(row["last_id"]) if row["last_id"] is not None else None,
            "last_time_ms": int(row["last_time_ms"]) if row["last_time_ms"] is not None else None,
        }

    def get_sync_cursors(self, stream: str, symbols):
        normalized = sorted({str(symbol).upper() for symbol in symbols or [] if symbol})
        if not normalized:
            return {}
        placeholders = ",".join("?" for _ in normalized)
        conn = self.db._get_connection()
        try:
            rows = conn.execute(
                f"""
                SELECT symbol, last_id, last_time_ms
                FROM sync_cursors
                WHERE stream = ? AND symbol IN ({placeholders})
                """,
                (str(stream), *normalized),
            ).fetchall()
        finally:
            conn.close()
        result = {symbol: None for symbol in normalized}
        for row in rows:
            result[str(row["symbol"])] = {
                "last_id": int(row["last_id"]) if row["last_id"] is not None else None,
                "last_time_ms": int(row["last_time_ms"]) if row["last_time_ms"] is not None else None,
            }
        return result

    def get_statistics(self):
        return self.trade_repo.get_statistics()

    def recompute_trade_summary(self):
        return self.trade_repo.recompute_trade_summary()

    def get_sync_status(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                id,
                last_sync_time,
                last_entry_time,
                total_trades,
                status,
                error_message,
                updated_at
            FROM sync_status
            WHERE id = 1
            """
        )
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else {}

    def list_sync_run_logs(self, limit: int = 100):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                id,
                run_type,
                mode,
                status,
                symbol_count,
                rows_count,
                trades_saved,
                open_saved,
                elapsed_ms,
                error_message,
                created_at
            FROM sync_run_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_open_positions(self):
        return fetch_open_positions(self.db)

    def get_open_position_symbols(self):
        return fetch_open_position_symbols(self.db)

    def get_latest_transfer_event_time(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT MAX(event_time) AS latest_event_time
            FROM transfers
            WHERE event_time IS NOT NULL
            """
        )
        row = cursor.fetchone()
        conn.close()
        if not row or row["latest_event_time"] is None:
            return None
        return int(row["latest_event_time"])

    def get_data_quality_summary(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    COUNT(*) as total_symbols,
                    SUM(CASE WHEN last_error IS NOT NULL AND last_error != '' THEN 1 ELSE 0 END) as failed_symbols,
                    MIN(last_success_end_ms) as oldest_watermark_ms,
                    MAX(last_success_end_ms) as newest_watermark_ms
                FROM symbol_sync_state
                """
            )
            row = cursor.fetchone()
            if not row:
                return {}
            total = int(row["total_symbols"] or 0)
            failed = int(row["failed_symbols"] or 0)
            health = "healthy"
            if total > 0 and failed > 0:
                health = "partial" if failed < total else "degraded"
            return {
                "total_tracked_symbols": total,
                "failed_symbols_count": failed,
                "data_health": health,
                "oldest_watermark_ms": row["oldest_watermark_ms"],
                "newest_watermark_ms": row["newest_watermark_ms"],
            }
        finally:
            conn.close()
