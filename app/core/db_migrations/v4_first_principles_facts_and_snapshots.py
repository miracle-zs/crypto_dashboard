"""Database schema migration v4: facts ledger and domain snapshots."""


def apply_v4_first_principles_facts_and_snapshots(conn, logger):
    logger.info("应用数据库迁移 v4: 建立事实账本与快照表...")
    cursor = conn.cursor()

    # 1. 崩盘风险快照表
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS crash_risk_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT,
            snapshot_time TEXT,
            window_start_utc TEXT,
            payload_json TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(snapshot_date)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_crash_risk_snapshots_date ON crash_risk_snapshots(snapshot_date)"
    )

    # 2. 交易所权威持仓快照表
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS position_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_time TEXT,
            symbol TEXT,
            position_side TEXT DEFAULT 'BOTH',
            qty REAL,
            entry_price REAL,
            mark_price REAL,
            unrealized_pnl REAL,
            liquidation_price REAL,
            leverage INTEGER,
            margin_type TEXT,
            is_complete INTEGER DEFAULT 1,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(snapshot_time, symbol, position_side)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_position_snapshots_time ON position_snapshots(snapshot_time DESC)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_position_snapshots_symbol ON position_snapshots(symbol)"
    )

    # 3. 为 open_positions 增加 is_incomplete 标记（若不存在）
    cursor.execute("PRAGMA table_info(open_positions)")
    existing_cols = {row[1] for row in cursor.fetchall()}
    if "is_incomplete" not in existing_cols:
        cursor.execute("ALTER TABLE open_positions ADD COLUMN is_incomplete INTEGER DEFAULT 0")

    # 4. 逐笔成交事实表 (不可变账本)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue TEXT DEFAULT 'binance_futures',
            symbol TEXT NOT NULL,
            trade_id INTEGER NOT NULL,
            order_id INTEGER NOT NULL,
            side TEXT NOT NULL,
            position_side TEXT NOT NULL DEFAULT 'BOTH',
            price REAL NOT NULL,
            qty REAL NOT NULL,
            realized_pnl REAL DEFAULT 0.0,
            quote_qty REAL NOT NULL,
            commission REAL NOT NULL DEFAULT 0.0,
            commission_asset TEXT NOT NULL DEFAULT 'USDT',
            time_ms INTEGER NOT NULL,
            is_buyer INTEGER NOT NULL DEFAULT 0,
            is_maker INTEGER NOT NULL DEFAULT 0,
            raw_payload TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(venue, symbol, trade_id)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_facts_time ON execution_facts(time_ms)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_execution_facts_symbol_time ON execution_facts(symbol, time_ms)"
    )

    # 5. 资金流水事实表 (不可变账本: 资金费、手续费返还等)
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS income_facts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            venue TEXT DEFAULT 'binance_futures',
            symbol TEXT,
            income_type TEXT NOT NULL,
            income REAL NOT NULL,
            asset TEXT NOT NULL DEFAULT 'USDT',
            time_ms INTEGER NOT NULL,
            tran_id INTEGER,
            trade_id INTEGER,
            info TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(venue, income_type, tran_id)
        )
        """
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_income_facts_time ON income_facts(time_ms)"
    )
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS idx_income_facts_symbol_time ON income_facts(symbol, time_ms)"
    )

    logger.info("数据库迁移 v4 应用成功")
