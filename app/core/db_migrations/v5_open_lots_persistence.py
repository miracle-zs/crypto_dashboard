"""Database schema migration v5: open lots persistence for resumable matching."""


def apply_v5_open_lots_persistence(conn, logger):
    logger.info("应用数据库迁移 v5: 创建未平仓买卖批次 (open_lots) 表...")
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS open_lots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            position_side TEXT NOT NULL DEFAULT 'BOTH',
            side TEXT NOT NULL,
            order_id INTEGER NOT NULL,
            price REAL NOT NULL,
            qty REAL NOT NULL,
            remaining_qty REAL NOT NULL,
            time_ms INTEGER NOT NULL,
            fee_rate REAL DEFAULT 0.0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_open_lots_symbol_pos ON open_lots(symbol, position_side)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_open_lots_time ON open_lots(time_ms ASC)"
    )

    logger.info("数据库迁移 v5 应用成功")
