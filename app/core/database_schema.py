"""SQLite schema initialization routines."""

import sqlite3

CURRENT_SCHEMA_VERSION = 1


def init_database_schema(conn, logger):
        """初始化数据库表结构"""

        # 开启 WAL 模式以支持更高并发
        conn.execute("PRAGMA journal_mode=WAL;")

        cursor = conn.cursor()
        cursor.execute("PRAGMA user_version")
        user_version_row = cursor.fetchone()
        current_version = int(user_version_row[0]) if user_version_row else 0
        if current_version >= CURRENT_SCHEMA_VERSION:
            conn.close()
            return

        # 创建交易记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                no INTEGER,
                date TEXT,
                entry_time TEXT,
                exit_time TEXT,
                holding_time TEXT,
                symbol TEXT,
                side TEXT,
                price_change_pct REAL,
                entry_amount REAL,
                entry_price REAL,
                exit_price REAL,
                qty REAL,
                fees REAL,
                pnl_net REAL,
                close_type TEXT,
                return_rate TEXT,
                open_price REAL,
                pnl_before_fees REAL,
                entry_order_id INTEGER,
                exit_order_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, entry_order_id, exit_order_id)
            )
        """)

        # 创建索引
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_entry_time ON trades(entry_time)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol ON trades(symbol)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_date ON trades(date)
        """)

        # 创建同步状态表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_status (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_sync_time TIMESTAMP,
                last_entry_time TEXT,
                total_trades INTEGER DEFAULT 0,
                status TEXT DEFAULT 'idle',
                error_message TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 初始化同步状态记录（如果不存在）
        cursor.execute("""
            INSERT OR IGNORE INTO sync_status (id, last_sync_time, status)
            VALUES (1, NULL, 'idle')
        """)

        # 按 symbol 维护增量同步水位
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS symbol_sync_state (
                symbol TEXT PRIMARY KEY,
                last_success_end_ms INTEGER,
                last_attempt_end_ms INTEGER,
                last_error TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_sync_state_updated_at
            ON symbol_sync_state(updated_at DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_sync_state_last_attempt
            ON symbol_sync_state(last_attempt_end_ms DESC)
        """)

        # 同步运行审计日志（便于复盘同步链路问题）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sync_run_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_type TEXT NOT NULL,
                mode TEXT,
                status TEXT NOT NULL,
                symbol_count INTEGER DEFAULT 0,
                rows_count INTEGER DEFAULT 0,
                trades_saved INTEGER DEFAULT 0,
                open_saved INTEGER DEFAULT 0,
                elapsed_ms INTEGER DEFAULT 0,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sync_run_log_created_at ON sync_run_log(created_at DESC)
        """)

        # 创建余额历史表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS balance_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                balance REAL NOT NULL,
                wallet_balance REAL DEFAULT 0.0
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_balance_timestamp ON balance_history(timestamp)
        """)

        # 检查是否需要迁移 wallet_balance 列 (针对旧数据库)
        try:
            cursor.execute("SELECT wallet_balance FROM balance_history LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("正在迁移数据库: 添加 wallet_balance 列...")
            try:
                cursor.execute("ALTER TABLE balance_history ADD COLUMN wallet_balance REAL DEFAULT 0.0")
            except Exception as e:
                logger.warning(f"列添加失败(可能已存在): {e}")

        # 创建出入金记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                amount REAL NOT NULL,
                type TEXT DEFAULT 'auto',
                description TEXT
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transfers_timestamp ON transfers(timestamp)
        """)
        # 迁移 transfers 扩展字段（用于基于 Binance income 直接同步）
        cursor.execute("PRAGMA table_info(transfers)")
        transfer_columns = {row[1] for row in cursor.fetchall()}
        if 'event_time' not in transfer_columns:
            try:
                cursor.execute("ALTER TABLE transfers ADD COLUMN event_time INTEGER")
            except Exception as e:
                logger.warning(f"transfers 添加 event_time 失败(可能已存在): {e}")
        if 'asset' not in transfer_columns:
            try:
                cursor.execute("ALTER TABLE transfers ADD COLUMN asset TEXT")
            except Exception as e:
                logger.warning(f"transfers 添加 asset 失败(可能已存在): {e}")
        if 'income_type' not in transfer_columns:
            try:
                cursor.execute("ALTER TABLE transfers ADD COLUMN income_type TEXT")
            except Exception as e:
                logger.warning(f"transfers 添加 income_type 失败(可能已存在): {e}")
        if 'source_uid' not in transfer_columns:
            try:
                cursor.execute("ALTER TABLE transfers ADD COLUMN source_uid TEXT")
            except Exception as e:
                logger.warning(f"transfers 添加 source_uid 失败(可能已存在): {e}")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_transfers_event_time ON transfers(event_time)
        """)
        cursor.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS idx_transfers_source_uid
            ON transfers(source_uid)
            WHERE source_uid IS NOT NULL
        """)

        # Websocket 事件记录表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ws_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_type TEXT,
                event_time INTEGER,
                payload TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ws_events_time ON ws_events(event_time)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_ws_events_type ON ws_events(event_type)
        """)

        # 创建未平仓订单表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS open_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                symbol TEXT,
                side TEXT,
                entry_time TEXT,
                entry_price REAL,
                qty REAL,
                entry_amount REAL,
                order_id INTEGER,
                alerted INTEGER DEFAULT 0,
                last_alert_time TIMESTAMP,
                profit_alerted INTEGER DEFAULT 0,
                profit_alert_time TIMESTAMP,
                reentry_alerted INTEGER DEFAULT 0,
                reentry_alert_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, order_id)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_open_positions_date ON open_positions(date)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_open_positions_entry_time ON open_positions(entry_time DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_open_positions_symbol ON open_positions(symbol)
        """)

        # 检查是否需要迁移 alerted 列
        try:
            cursor.execute("SELECT alerted FROM open_positions LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("正在迁移数据库: 添加 alerted 列...")
            try:
                cursor.execute("ALTER TABLE open_positions ADD COLUMN alerted INTEGER DEFAULT 0")
            except Exception as e:
                logger.warning(f"列添加失败(可能已存在): {e}")

        # 检查是否需要迁移 last_alert_time 列
        try:
            cursor.execute("SELECT last_alert_time FROM open_positions LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("正在迁移数据库: 添加 last_alert_time 列...")
            try:
                cursor.execute("ALTER TABLE open_positions ADD COLUMN last_alert_time TIMESTAMP")
            except Exception as e:
                logger.warning(f"列添加失败(可能已存在): {e}")

        # 检查是否需要迁移 is_long_term 列
        try:
            cursor.execute("SELECT is_long_term FROM open_positions LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("正在迁移数据库: 添加 is_long_term 列...")
            try:
                cursor.execute("ALTER TABLE open_positions ADD COLUMN is_long_term INTEGER DEFAULT 0")
            except Exception as e:
                logger.warning(f"列添加失败(可能已存在): {e}")

        # 检查是否需要迁移 profit_alerted 列
        try:
            cursor.execute("SELECT profit_alerted FROM open_positions LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("正在迁移数据库: 添加 profit_alerted 列...")
            try:
                cursor.execute("ALTER TABLE open_positions ADD COLUMN profit_alerted INTEGER DEFAULT 0")
            except Exception as e:
                logger.warning(f"列添加失败(可能已存在): {e}")

        # 检查是否需要迁移 profit_alert_time 列
        try:
            cursor.execute("SELECT profit_alert_time FROM open_positions LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("正在迁移数据库: 添加 profit_alert_time 列...")
            try:
                cursor.execute("ALTER TABLE open_positions ADD COLUMN profit_alert_time TIMESTAMP")
            except Exception as e:
                logger.warning(f"列添加失败(可能已存在): {e}")

        # 检查是否需要迁移 reentry_alerted 列
        try:
            cursor.execute("SELECT reentry_alerted FROM open_positions LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("正在迁移数据库: 添加 reentry_alerted 列...")
            try:
                cursor.execute("ALTER TABLE open_positions ADD COLUMN reentry_alerted INTEGER DEFAULT 0")
            except Exception as e:
                logger.warning(f"列添加失败(可能已存在): {e}")

        # 检查是否需要迁移 reentry_alert_time 列
        try:
            cursor.execute("SELECT reentry_alert_time FROM open_positions LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("正在迁移数据库: 添加 reentry_alert_time 列...")
            try:
                cursor.execute("ALTER TABLE open_positions ADD COLUMN reentry_alert_time TIMESTAMP")
            except Exception as e:
                logger.warning(f"列添加失败(可能已存在): {e}")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_open_positions_profit_alerted_entry
            ON open_positions(profit_alerted, entry_time DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_open_positions_alerted_last_alert
            ON open_positions(alerted, last_alert_time)
        """)

        # 明日观察列表（仅记录 symbol 与自动时间）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS watch_notes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                noted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_watch_notes_noted_at ON watch_notes(noted_at DESC)
        """)

        # 创建用户设置表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                monthly_target REAL DEFAULT 30000,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # 初始化设置记录
        cursor.execute("""
            INSERT OR IGNORE INTO user_settings (id, monthly_target)
            VALUES (1, 30000)
        """)

        # 交易统计快照表
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_summary (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                total_pnl REAL,
                total_fees REAL,
                win_rate REAL,
                win_count INTEGER,
                loss_count INTEGER,
                total_trades INTEGER,
                equity_curve TEXT,
                current_streak INTEGER,
                best_win_streak INTEGER,
                worst_loss_streak INTEGER,
                max_single_loss REAL,
                max_drawdown REAL,
                profit_factor REAL,
                kelly_criterion REAL,
                sqn REAL,
                expected_value REAL,
                risk_reward_ratio REAL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            INSERT OR IGNORE INTO trade_summary (id)
            VALUES (1)
        """)
        try:
            cursor.execute("SELECT max_single_loss FROM trade_summary LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("正在迁移数据库: 添加 max_single_loss 列...")
            try:
                cursor.execute("ALTER TABLE trade_summary ADD COLUMN max_single_loss REAL DEFAULT 0.0")
                cursor.execute(
                    """
                    UPDATE trade_summary
                    SET max_single_loss = COALESCE(max_single_loss, max_drawdown, 0.0)
                    """
                )
            except Exception as e:
                logger.warning(f"列添加失败(可能已存在): {e}")

        # 交易聚合缓存（用于首页图表/榜单聚合接口提速）
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_aggregates_cache (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                trades_count INTEGER DEFAULT 0,
                latest_trade_updated_at TEXT,
                payload_json TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO trade_aggregates_cache (id, trades_count)
            VALUES (1, 0)
            """
        )

        # 涨幅榜历史快照表（每天07:40记录一次）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS leaderboard_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL,
                snapshot_time TEXT NOT NULL,
                window_start_utc TEXT,
                candidates INTEGER DEFAULT 0,
                effective INTEGER DEFAULT 0,
                top_count INTEGER DEFAULT 0,
                rows_json TEXT,
                losers_rows_json TEXT,
                all_rows_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(snapshot_date)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_leaderboard_snapshot_time ON leaderboard_snapshots(snapshot_time DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_leaderboard_snapshot_date ON leaderboard_snapshots(snapshot_date DESC)
        """)
        try:
            cursor.execute("SELECT losers_rows_json FROM leaderboard_snapshots LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("正在迁移数据库: 添加 losers_rows_json 列...")
            try:
                cursor.execute("ALTER TABLE leaderboard_snapshots ADD COLUMN losers_rows_json TEXT")
            except Exception as e:
                logger.warning(f"列添加失败(可能已存在): {e}")
        try:
            cursor.execute("SELECT all_rows_json FROM leaderboard_snapshots LIMIT 1")
        except sqlite3.OperationalError:
            logger.info("正在迁移数据库: 添加 all_rows_json 列...")
            try:
                cursor.execute("ALTER TABLE leaderboard_snapshots ADD COLUMN all_rows_json TEXT")
            except Exception as e:
                logger.warning(f"列添加失败(可能已存在): {e}")

        # 涨幅榜三指标日统计（按快照日保存）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS leaderboard_daily_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL UNIQUE,
                metric1_json TEXT,
                metric2_json TEXT,
                metric3_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_leaderboard_daily_metrics_date ON leaderboard_daily_metrics(snapshot_date DESC)
        """)

        # 7D反弹幅度榜快照（每天07:30记录一次）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rebound_7d_snapshots (
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
            CREATE INDEX IF NOT EXISTS idx_rebound_7d_snapshot_time ON rebound_7d_snapshots(snapshot_time DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rebound_7d_snapshot_date ON rebound_7d_snapshots(snapshot_date DESC)
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rebound_30d_snapshots (
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
            CREATE INDEX IF NOT EXISTS idx_rebound_30d_snapshot_time ON rebound_30d_snapshots(snapshot_time DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rebound_30d_snapshot_date ON rebound_30d_snapshots(snapshot_date DESC)
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rebound_60d_snapshots (
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
            CREATE INDEX IF NOT EXISTS idx_rebound_60d_snapshot_time ON rebound_60d_snapshots(snapshot_time DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_rebound_60d_snapshot_date ON rebound_60d_snapshots(snapshot_date DESC)
        """)

        # 午间浮亏快照（每天11:50记录一次）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS noon_loss_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL UNIQUE,
                snapshot_time TEXT NOT NULL,
                loss_count INTEGER DEFAULT 0,
                total_stop_loss REAL DEFAULT 0.0,
                pct_of_balance REAL DEFAULT 0.0,
                balance REAL DEFAULT 0.0,
                rows_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_noon_loss_snapshot_time ON noon_loss_snapshots(snapshot_time DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_noon_loss_snapshot_date ON noon_loss_snapshots(snapshot_date DESC)
        """)

        # 午间浮亏复盘快照（每天23:00后复盘一次）
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS noon_loss_review_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL UNIQUE,
                review_time TEXT NOT NULL,
                noon_loss_count INTEGER DEFAULT 0,
                not_cut_count INTEGER DEFAULT 0,
                noon_cut_loss_total REAL DEFAULT 0.0,
                hold_loss_total REAL DEFAULT 0.0,
                delta_loss_total REAL DEFAULT 0.0,
                pct_of_balance REAL DEFAULT 0.0,
                balance REAL DEFAULT 0.0,
                rows_json TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_noon_loss_review_snapshot_time ON noon_loss_review_snapshots(review_time DESC)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_noon_loss_review_snapshot_date ON noon_loss_review_snapshots(snapshot_date DESC)
        """)

        cursor.execute(f"PRAGMA user_version = {CURRENT_SCHEMA_VERSION}")
        conn.commit()
        conn.close()
