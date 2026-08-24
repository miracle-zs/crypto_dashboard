"""Add the local daily-kline cache and endpoint-specific sync cursors."""


def apply_v3_daily_klines_and_cursors(conn, logger):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_klines (
            symbol TEXT NOT NULL,
            open_time INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            is_closed INTEGER NOT NULL DEFAULT 0,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(symbol, open_time)
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_daily_klines_open_time
        ON daily_klines(open_time DESC)
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_daily_klines_symbol_closed
        ON daily_klines(symbol, is_closed, open_time DESC)
        """
    )

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sync_cursors (
            stream TEXT NOT NULL,
            symbol TEXT NOT NULL DEFAULT '',
            last_id INTEGER,
            last_time_ms INTEGER,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(stream, symbol)
        )
        """
    )
    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_sync_cursors_updated_at
        ON sync_cursors(updated_at DESC)
        """
    )
    logger.info("数据库迁移 v3 完成: 新增 daily_klines 与 sync_cursors")

