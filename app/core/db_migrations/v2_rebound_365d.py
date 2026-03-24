def apply_v2_rebound_365d_schema(conn, logger):
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS rebound_365d_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL,
            snapshot_time TEXT NOT NULL,
            window_start_utc TEXT,
            candidates INTEGER DEFAULT 0,
            effective INTEGER DEFAULT 0,
            top_count INTEGER DEFAULT 0,
            rows_json TEXT,
            all_rows_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(snapshot_date)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_rebound_365d_snapshot_time ON rebound_365d_snapshots(snapshot_time DESC)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_rebound_365d_snapshot_date ON rebound_365d_snapshots(snapshot_date DESC)
    """)

    logger.info("数据库迁移 v2 完成: 新增 rebound_365d_snapshots 表")
