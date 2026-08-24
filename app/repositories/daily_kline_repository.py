class DailyKlineRepository:
    """Persistence adapter for the shared local daily-kline market view."""

    def __init__(self, db):
        self.db = db

    def upsert(self, rows) -> int:
        normalized = []
        for row in rows or []:
            try:
                normalized.append(
                    (
                        str(row["symbol"]).upper(),
                        int(row["open_time"]),
                        float(row["open"]),
                        float(row["high"]),
                        float(row["low"]),
                        float(row["close"]),
                        1 if bool(row.get("is_closed")) else 0,
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
        if not normalized:
            return 0

        conn = self.db._get_connection()
        try:
            conn.executemany(
                """
                INSERT INTO daily_klines (
                    symbol, open_time, open, high, low, close, is_closed, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(symbol, open_time) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    is_closed = excluded.is_closed,
                    updated_at = CURRENT_TIMESTAMP
                """,
                normalized,
            )
            conn.commit()
            return len(normalized)
        finally:
            conn.close()

    def latest_open_times(self, symbols) -> dict[str, int | None]:
        normalized = sorted({str(symbol).upper() for symbol in symbols or [] if symbol})
        if not normalized:
            return {}
        placeholders = ",".join("?" for _ in normalized)
        conn = self.db._get_connection()
        try:
            rows = conn.execute(
                f"""
                SELECT symbol, MAX(open_time) AS latest_open_time
                FROM daily_klines
                WHERE symbol IN ({placeholders})
                GROUP BY symbol
                """,
                tuple(normalized),
            ).fetchall()
        finally:
            conn.close()
        result = {symbol: None for symbol in normalized}
        for row in rows:
            result[str(row["symbol"])] = int(row["latest_open_time"])
        return result

    def load(self, symbols=None, *, since_open_time: int | None = None) -> dict[str, list[dict]]:
        normalized = None
        if symbols is not None:
            normalized = sorted({str(symbol).upper() for symbol in symbols if symbol})
            if not normalized:
                return {}

        clauses = []
        params = []
        if normalized is not None:
            placeholders = ",".join("?" for _ in normalized)
            clauses.append(f"symbol IN ({placeholders})")
            params.extend(normalized)
        if since_open_time is not None:
            clauses.append("open_time >= ?")
            params.append(int(since_open_time))
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        conn = self.db._get_connection()
        try:
            rows = conn.execute(
                f"""
                SELECT symbol, open_time, open, high, low, close, is_closed, updated_at
                FROM daily_klines
                {where_sql}
                ORDER BY symbol, open_time
                """,
                tuple(params),
            ).fetchall()
        finally:
            conn.close()

        result = {symbol: [] for symbol in normalized or []}
        for row in rows:
            symbol = str(row["symbol"])
            result.setdefault(symbol, []).append(
                {
                    "symbol": symbol,
                    "open_time": int(row["open_time"]),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "is_closed": bool(row["is_closed"]),
                    "updated_at": row["updated_at"],
                }
            )
        return result

