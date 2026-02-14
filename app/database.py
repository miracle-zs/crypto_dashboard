"""
数据库持久化层 - 使用SQLite存储交易数据
"""
import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
import os
from pathlib import Path
from app.logger import logger
import json
import numpy as np
from zoneinfo import ZoneInfo


class Database:
    """SQLite数据库管理类"""

    def __init__(self, db_path: str = None):
        if db_path is None:
            # 默认数据库路径
            project_root = Path(__file__).parent.parent
            db_path = project_root / "data" / "trades.db"

        self.db_path = db_path

        # 确保数据目录存在
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        # 初始化数据库
        self._init_database()

    def _get_connection(self):
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row  # 支持字典访问
        return conn

    @staticmethod
    def _today_snapshot_date_utc8() -> str:
        """按UTC+8返回今天日期，用于过滤未来测试快照。"""
        return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d")

    def _init_database(self):
        """初始化数据库表结构"""
        conn = self._get_connection()

        # 开启 WAL 模式以支持更高并发
        conn.execute("PRAGMA journal_mode=WAL;")

        cursor = conn.cursor()

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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, order_id)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_open_positions_date ON open_positions(date)
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

        conn.commit()
        conn.close()

    def save_trades(self, df: pd.DataFrame, overwrite: bool = False) -> int:
        """
        保存交易数据到数据库

        Args:
            df: 交易数据DataFrame
            overwrite: 是否覆盖模式（先删除该时间段内的所有记录，再插入）

        Returns:
            int: 新增或更新的记录数
        """
        if df.empty:
            return 0

        conn = self._get_connection()
        cursor = conn.cursor()

        if overwrite:
            # 获取数据的时间范围，只删除该范围内的旧数据
            if 'Entry_Time' in df.columns:
                min_time = df['Entry_Time'].min()
                max_time = df['Entry_Time'].max()
                logger.info(f"覆盖模式: 删除 {min_time} 至 {max_time} 期间的旧记录...")
                cursor.execute("DELETE FROM trades WHERE entry_time >= ? AND entry_time <= ?", (min_time, max_time))
            else:
                # 如果没有时间字段，甚至可以考虑清空全表（视需求而定，这里保守一点只清空相关的Symbol）
                # 但既然是overwrite模式且通常用于全量同步，按时间删是最安全的
                pass

        inserted_count = 0
        updated_count = 0

        for _, row in df.iterrows():
            # 检查是否已存在
            cursor.execute("""
                SELECT id FROM trades
                WHERE symbol = ? AND entry_order_id = ? AND exit_order_id = ?
            """, (row['Symbol'], int(row['Entry_Order_ID']), str(row['Exit_Order_ID'])))

            existing = cursor.fetchone()

            if existing:
                # 更新现有记录
                cursor.execute("""
                    UPDATE trades SET
                        no = ?, date = ?, entry_time = ?, exit_time = ?,
                        holding_time = ?, side = ?, price_change_pct = ?,
                        entry_amount = ?, entry_price = ?, exit_price = ?,
                        qty = ?, fees = ?, pnl_net = ?, close_type = ?,
                        return_rate = ?, open_price = ?, pnl_before_fees = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (
                    int(row['No']), row['Date'], row['Entry_Time'], row['Exit_Time'],
                    row['Holding_Time'], row['Side'], float(row['Price_Change_Pct']),
                    float(row['Entry_Amount']), float(row['Entry_Price']), float(row['Exit_Price']),
                    float(row['Qty']), float(row['Fees']), float(row['PNL_Net']), row['Close_Type'],
                    row['Return_Rate'], float(row['Open_Price']), float(row['PNL_Before_Fees']),
                    existing['id']
                ))
                updated_count += 1
            else:
                # 插入新记录
                cursor.execute("""
                    INSERT INTO trades (
                        no, date, entry_time, exit_time, holding_time, symbol, side,
                        price_change_pct, entry_amount, entry_price, exit_price, qty,
                        fees, pnl_net, close_type, return_rate, open_price,
                        pnl_before_fees, entry_order_id, exit_order_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    int(row['No']), row['Date'], row['Entry_Time'], row['Exit_Time'],
                    row['Holding_Time'], row['Symbol'], row['Side'], float(row['Price_Change_Pct']),
                    float(row['Entry_Amount']), float(row['Entry_Price']), float(row['Exit_Price']),
                    float(row['Qty']), float(row['Fees']), float(row['PNL_Net']), row['Close_Type'],
                    row['Return_Rate'], float(row['Open_Price']), float(row['PNL_Before_Fees']),
                    int(row['Entry_Order_ID']), str(row['Exit_Order_ID'])
                ))
                inserted_count += 1

        conn.commit()
        conn.close()

        logger.info(f"数据库操作完成: 新增 {inserted_count} 条, 更新 {updated_count} 条")
        return inserted_count + updated_count

    def get_all_trades(self, limit: int = None) -> pd.DataFrame:
        """
        获取所有交易记录

        Args:
            limit: 限制返回记录数

        Returns:
            pd.DataFrame: 交易数据
        """
        conn = self._get_connection()

        query = """
            SELECT no, date, entry_time, exit_time, holding_time, symbol, side,
                   price_change_pct, entry_amount, entry_price, exit_price, qty,
                   fees, pnl_net, close_type, return_rate, open_price,
                   pnl_before_fees, entry_order_id, exit_order_id
            FROM trades
            ORDER BY entry_time ASC
        """

        if limit:
            query += f" LIMIT {limit}"

        df = pd.read_sql_query(query, conn)
        conn.close()

        # 重命名列以匹配原始格式
        if not df.empty:
            df.columns = [
                'No', 'Date', 'Entry_Time', 'Exit_Time', 'Holding_Time', 'Symbol', 'Side',
                'Price_Change_Pct', 'Entry_Amount', 'Entry_Price', 'Exit_Price', 'Qty',
                'Fees', 'PNL_Net', 'Close_Type', 'Return_Rate', 'Open_Price',
                'PNL_Before_Fees', 'Entry_Order_ID', 'Exit_Order_ID'
            ]

        return df

    def get_trades_by_date_range(self, start_date: str = None, end_date: str = None) -> pd.DataFrame:
        """
        根据日期范围获取交易记录

        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)

        Returns:
            pd.DataFrame: 交易数据
        """
        conn = self._get_connection()

        query = """
            SELECT no, date, entry_time, exit_time, holding_time, symbol, side,
                   price_change_pct, entry_amount, entry_price, exit_price, qty,
                   fees, pnl_net, close_type, return_rate, open_price,
                   pnl_before_fees, entry_order_id, exit_order_id
            FROM trades
            WHERE 1=1
        """

        params = []
        if start_date:
            query += " AND entry_time >= ?"
            params.append(start_date)
        if end_date:
            query += " AND entry_time <= ?"
            params.append(end_date)

        query += " ORDER BY entry_time ASC"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()

        # 重命名列
        if not df.empty:
            df.columns = [
                'No', 'Date', 'Entry_Time', 'Exit_Time', 'Holding_Time', 'Symbol', 'Side',
                'Price_Change_Pct', 'Entry_Amount', 'Entry_Price', 'Exit_Price', 'Qty',
                'Fees', 'PNL_Net', 'Close_Type', 'Return_Rate', 'Open_Price',
                'PNL_Before_Fees', 'Entry_Order_ID', 'Exit_Order_ID'
            ]

        return df

    def get_last_entry_time(self) -> Optional[str]:
        """获取最后一条交易的入场时间"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT MAX(entry_time) FROM trades")
        result = cursor.fetchone()

        conn.close()

        return result[0] if result and result[0] else None

    def update_sync_status(self, status: str = 'idle', error_message: str = None):
        """
        更新同步状态

        Args:
            status: 状态 (idle, syncing, error)
            error_message: 错误消息
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        last_entry_time = self.get_last_entry_time()

        cursor.execute("SELECT COUNT(*) FROM trades")
        total_trades = cursor.fetchone()[0]

        cursor.execute("""
            UPDATE sync_status
            SET last_sync_time = CURRENT_TIMESTAMP,
                last_entry_time = ?,
                total_trades = ?,
                status = ?,
                error_message = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
        """, (last_entry_time, total_trades, status, error_message))

        conn.commit()
        conn.close()

    def get_sync_status(self) -> Dict:
        """获取同步状态"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM sync_status WHERE id = 1")
        row = cursor.fetchone()

        conn.close()

        if row:
            return dict(row)
        return {}

    def clear_all_trades(self):
        """清空所有交易记录（慎用）"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("DELETE FROM trades")
        cursor.execute("UPDATE sync_status SET total_trades = 0, last_entry_time = NULL WHERE id = 1")

        conn.commit()
        conn.close()

        logger.info("所有交易记录已清空")

    def get_statistics(self) -> Dict:
        """获取数据库统计信息"""
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM trades")
        total_trades = cursor.fetchone()[0]

        cursor.execute("SELECT MIN(entry_time), MAX(entry_time) FROM trades")
        date_range = cursor.fetchone()

        cursor.execute("SELECT COUNT(DISTINCT symbol) FROM trades")
        unique_symbols = cursor.fetchone()[0]

        conn.close()

        return {
            'total_trades': total_trades,
            'earliest_trade': date_range[0],
            'latest_trade': date_range[1],
            'unique_symbols': unique_symbols
        }

    def get_trade_summary(self) -> Optional[Dict]:
        """读取交易统计快照"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM trade_summary WHERE id = 1")
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None
        data = dict(row)
        data.pop('id', None)
        data.pop('updated_at', None)
        if data.get('equity_curve'):
            try:
                data['equity_curve'] = json.loads(data['equity_curve'])
            except Exception:
                data['equity_curve'] = []
        else:
            data['equity_curve'] = []
        # Backward-compatible alias: this field historically stored max single-trade loss.
        data['max_single_loss'] = float(data.get('max_drawdown', 0.0) or 0.0)
        return data

    def save_trade_summary(self, summary: Dict):
        """保存交易统计快照"""
        conn = self._get_connection()
        cursor = conn.cursor()

        equity_curve = summary.get('equity_curve', [])
        equity_curve_json = json.dumps(equity_curve, ensure_ascii=False)

        cursor.execute("""
            INSERT INTO trade_summary (
                id, total_pnl, total_fees, win_rate, win_count, loss_count,
                total_trades, equity_curve, current_streak, best_win_streak,
                worst_loss_streak, max_drawdown, profit_factor, kelly_criterion,
                sqn, expected_value, risk_reward_ratio, updated_at
            ) VALUES (
                1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
            )
            ON CONFLICT(id) DO UPDATE SET
                total_pnl = excluded.total_pnl,
                total_fees = excluded.total_fees,
                win_rate = excluded.win_rate,
                win_count = excluded.win_count,
                loss_count = excluded.loss_count,
                total_trades = excluded.total_trades,
                equity_curve = excluded.equity_curve,
                current_streak = excluded.current_streak,
                best_win_streak = excluded.best_win_streak,
                worst_loss_streak = excluded.worst_loss_streak,
                max_drawdown = excluded.max_drawdown,
                profit_factor = excluded.profit_factor,
                kelly_criterion = excluded.kelly_criterion,
                sqn = excluded.sqn,
                expected_value = excluded.expected_value,
                risk_reward_ratio = excluded.risk_reward_ratio,
                updated_at = CURRENT_TIMESTAMP
        """, (
            float(summary.get('total_pnl', 0.0)),
            float(summary.get('total_fees', 0.0)),
            float(summary.get('win_rate', 0.0)),
            int(summary.get('win_count', 0)),
            int(summary.get('loss_count', 0)),
            int(summary.get('total_trades', 0)),
            equity_curve_json,
            int(summary.get('current_streak', 0)),
            int(summary.get('best_win_streak', 0)),
            int(summary.get('worst_loss_streak', 0)),
            float(summary.get('max_single_loss', summary.get('max_drawdown', 0.0))),
            float(summary.get('profit_factor', 0.0)),
            float(summary.get('kelly_criterion', 0.0)),
            float(summary.get('sqn', 0.0)),
            float(summary.get('expected_value', 0.0)),
            float(summary.get('risk_reward_ratio', 0.0)),
        ))

        conn.commit()
        conn.close()

    def recompute_trade_summary(self) -> Dict:
        """全量重算交易统计并保存"""
        df = self.get_all_trades()

        if df.empty:
            summary = {
                'total_pnl': 0.0,
                'total_fees': 0.0,
                'win_rate': 0.0,
                'win_count': 0,
                'loss_count': 0,
                'total_trades': 0,
                'equity_curve': [],
                'current_streak': 0,
                'best_win_streak': 0,
                'worst_loss_streak': 0,
                'max_single_loss': 0.0,
                'max_drawdown': 0.0,
                'profit_factor': 0.0,
                'kelly_criterion': 0.0,
                'sqn': 0.0,
                'expected_value': 0.0,
                'risk_reward_ratio': 0.0
            }
            self.save_trade_summary(summary)
            return summary

        total_pnl = float(df['PNL_Net'].sum())
        total_fees = float(df['Fees'].sum())
        win_count = len(df[df['PNL_Net'] > 0])
        loss_count = len(df[df['PNL_Net'] < 0])
        total_trades = len(df)
        win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0.0
        equity_curve = df['PNL_Net'].cumsum().tolist()

        current_streak = 0
        if not df.empty:
            pnl_values = df['PNL_Net'].values
            # Get the sign of the most recent trade (last one in dataframe)
            last_pnl = pnl_values[-1]

            # Iterate backwards from the last trade
            for pnl in reversed(pnl_values):
                # If zero PnL, we can treat it as breaking the streak or ignore it
                # Here we treat 0 as breaking streak unless we want to skip it
                if pnl == 0:
                    break

                if last_pnl > 0:
                    if pnl > 0:
                        current_streak += 1
                    else:
                        break
                elif last_pnl < 0:
                    if pnl < 0:
                        current_streak -= 1
                    else:
                        break

        best_win_streak = 0
        worst_loss_streak = 0
        streak = 0
        for pnl in df['PNL_Net'].values:
            if pnl > 0:
                streak = streak + 1 if streak >= 0 else 1
            elif pnl < 0:
                streak = streak - 1 if streak <= 0 else -1
            else:
                streak = 0
            if streak > best_win_streak:
                best_win_streak = streak
            if streak < worst_loss_streak:
                worst_loss_streak = streak

        max_single_loss = float(df['PNL_Net'].min()) if not df.empty else 0.0
        total_wins = float(df[df['PNL_Net'] > 0]['PNL_Net'].sum())
        total_losses = abs(float(df[df['PNL_Net'] < 0]['PNL_Net'].sum()))
        profit_factor = (total_wins / total_losses) if total_losses > 0 else 0.0

        avg_win = total_wins / win_count if win_count > 0 else 0
        avg_loss = total_losses / loss_count if loss_count > 0 else 0
        win_prob = win_count / total_trades if total_trades > 0 else 0
        loss_prob = loss_count / total_trades if total_trades > 0 else 0

        if avg_loss > 0 and avg_win > 0:
            kelly_criterion = (win_prob * avg_win - loss_prob * avg_loss) / avg_win
        else:
            kelly_criterion = 0.0

        if total_trades > 0:
            pnl_mean = df['PNL_Net'].mean()
            pnl_std = df['PNL_Net'].std()
            sqn = (pnl_mean / pnl_std) * np.sqrt(total_trades) if pnl_std > 0 else 0.0
        else:
            sqn = 0.0

        expected_value = (win_prob * avg_win) - (loss_prob * avg_loss)
        risk_reward_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0

        summary = {
            'total_pnl': total_pnl,
            'total_fees': total_fees,
            'win_rate': win_rate,
            'win_count': win_count,
            'loss_count': loss_count,
            'total_trades': total_trades,
            'equity_curve': equity_curve,
            'current_streak': current_streak,
            'best_win_streak': best_win_streak,
            'worst_loss_streak': worst_loss_streak,
            'max_single_loss': max_single_loss,
            'max_drawdown': max_single_loss,  # legacy field name
            'profit_factor': profit_factor,
            'kelly_criterion': kelly_criterion,
            'sqn': float(sqn),
            'expected_value': expected_value,
            'risk_reward_ratio': risk_reward_ratio
        }

        self.save_trade_summary(summary)
        return summary

    def save_balance_history(self, balance: float, wallet_balance: float = 0.0):
        """保存新的余额记录"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO balance_history (timestamp, balance, wallet_balance) VALUES (?, ?, ?)",
            (datetime.utcnow(), balance, wallet_balance)
        )
        conn.commit()
        conn.close()

    def save_transfer(self, amount: float, type: str = 'auto', description: str = None):
        """保存出入金记录"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO transfers (timestamp, amount, type, description) VALUES (?, ?, ?, ?)",
            (datetime.utcnow(), amount, type, description)
        )
        conn.commit()
        conn.close()
        logger.info(f"已记录出入金: {amount} ({type})")

    def save_ws_event(self, event_type: str, event_time: int, payload: Dict):
        """保存 websocket 事件记录"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO ws_events (event_type, event_time, payload) VALUES (?, ?, ?)",
            (event_type, int(event_time), json.dumps(payload, ensure_ascii=False))
        )
        conn.commit()
        conn.close()

    def get_transfers(self) -> List[Dict]:
        """获取所有出入金记录"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM transfers ORDER BY timestamp ASC")
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_daily_stats(self) -> List[Dict]:
        """
        获取每日交易统计（开单数量、开单金额、盈亏等）
        包含已平仓和未平仓的订单

        Returns:
            List[Dict]: 每日统计数据列表
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # 合并已平仓和未平仓订单的统计
        cursor.execute("""
            SELECT
                date,
                SUM(trade_count) as trade_count,
                SUM(total_amount) as total_amount,
                SUM(total_pnl) as total_pnl,
                SUM(win_count) as win_count,
                SUM(loss_count) as loss_count
            FROM (
                -- 已平仓交易
                SELECT
                    date,
                    COUNT(*) as trade_count,
                    SUM(entry_amount) as total_amount,
                    SUM(pnl_net) as total_pnl,
                    SUM(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END) as win_count,
                    SUM(CASE WHEN pnl_net < 0 THEN 1 ELSE 0 END) as loss_count
                FROM trades
                GROUP BY date

                UNION ALL

                -- 未平仓订单
                SELECT
                    date,
                    COUNT(*) as trade_count,
                    SUM(entry_amount) as total_amount,
                    0 as total_pnl,
                    0 as win_count,
                    0 as loss_count
                FROM open_positions
                GROUP BY date
            )
            GROUP BY date
            ORDER BY date DESC
        """)

        rows = cursor.fetchall()
        conn.close()

        results = []
        for row in rows:
            trade_count = row['trade_count']
            win_count = row['win_count']
            win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0.0

            results.append({
                'date': row['date'],
                'trade_count': trade_count,
                'total_amount': float(row['total_amount'] or 0),
                'total_pnl': float(row['total_pnl'] or 0),
                'win_count': win_count,
                'loss_count': row['loss_count'],
                'win_rate': round(win_rate, 2)
            })

        return results


    def save_open_positions(self, positions: List[Dict]) -> int:
        """
        保存未平仓订单（全量替换，但保留告警与长期持仓状态）

        Args:
            positions: 未平仓订单列表

        Returns:
            int: 保存的记录数
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # 1. 获取现有记录状态
        state_map = {}
        try:
            # 检查列是否存在
            cursor.execute("PRAGMA table_info(open_positions)")
            columns = [info[1] for info in cursor.fetchall()]

            query_cols = ["symbol", "order_id", "alerted"]
            if 'last_alert_time' in columns:
                query_cols.append("last_alert_time")
            if 'profit_alerted' in columns:
                query_cols.append("profit_alerted")
            if 'profit_alert_time' in columns:
                query_cols.append("profit_alert_time")
            if 'is_long_term' in columns:
                query_cols.append("is_long_term")

            query = f"SELECT {', '.join(query_cols)} FROM open_positions"

            cursor.execute(query)
            for row in cursor.fetchall():
                key = f"{row['symbol']}_{row['order_id']}"
                state_data = {'alerted': row['alerted']}
                if 'last_alert_time' in columns:
                    state_data['last_alert_time'] = row['last_alert_time']
                if 'profit_alerted' in columns:
                    state_data['profit_alerted'] = row['profit_alerted']
                if 'profit_alert_time' in columns:
                    state_data['profit_alert_time'] = row['profit_alert_time']
                if 'is_long_term' in columns:
                    state_data['is_long_term'] = row['is_long_term']
                state_map[key] = state_data
        except Exception as e:
            logger.warning(f"读取状态失败: {e}")

        # 2. 清空现有未平仓记录
        cursor.execute("DELETE FROM open_positions")

        # 3. 插入新记录
        for pos in positions:
            key = f"{pos['symbol']}_{pos['order_id']}"
            saved_state = state_map.get(key, {})
            is_alerted = saved_state.get('alerted', 0)
            last_alert_time = saved_state.get('last_alert_time', None)
            profit_alerted = saved_state.get('profit_alerted', 0)
            profit_alert_time = saved_state.get('profit_alert_time', None)
            is_long_term = saved_state.get('is_long_term', 0)

            cursor.execute("""
                INSERT INTO open_positions (
                    date, symbol, side, entry_time, entry_price, qty, entry_amount, order_id,
                    alerted, last_alert_time, profit_alerted, profit_alert_time, is_long_term
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pos['date'],
                pos['symbol'],
                pos['side'],
                pos['entry_time'],
                pos['entry_price'],
                pos['qty'],
                pos['entry_amount'],
                pos['order_id'],
                is_alerted,
                last_alert_time,
                profit_alerted,
                profit_alert_time,
                is_long_term
            ))

        conn.commit()
        conn.close()

        return len(positions)

    def set_position_alerted(self, symbol: str, order_id: int):
        """标记订单为已通知，并更新最后通知时间"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE open_positions
            SET alerted = 1, last_alert_time = CURRENT_TIMESTAMP
            WHERE symbol = ? AND order_id = ?
        """, (symbol, order_id))
        conn.commit()
        conn.close()

    def set_position_profit_alerted(self, symbol: str, order_id: int):
        """标记订单为盈利提醒已发送，并更新提醒时间"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE open_positions
            SET profit_alerted = 1, profit_alert_time = CURRENT_TIMESTAMP
            WHERE symbol = ? AND order_id = ?
        """, (symbol, order_id))
        conn.commit()
        conn.close()

    def set_position_long_term(self, symbol: str, order_id: int, is_long_term: bool):
        """设置持仓是否为长期持仓"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE open_positions
            SET is_long_term = ?
            WHERE symbol = ? AND order_id = ?
        """, (1 if is_long_term else 0, symbol, order_id))
        conn.commit()
        conn.close()

    def get_open_positions(self) -> List[Dict]:
        """获取所有未平仓订单"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM open_positions ORDER BY entry_time DESC")
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def add_watch_note(self, symbol: str) -> Dict:
        """新增明日观察记录（自动写入北京时间）"""
        normalized_symbol = (symbol or "").strip().upper()
        if not normalized_symbol:
            raise ValueError("symbol 不能为空")

        today = self._today_snapshot_date_utc8()
        noted_at = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")

        conn = self._get_connection()
        cursor = conn.cursor()

        # 同一个 symbol 在北京时间当天只保留一条记录
        cursor.execute("""
            SELECT id, symbol, noted_at
            FROM watch_notes
            WHERE symbol = ? AND substr(noted_at, 1, 10) = ?
            ORDER BY id DESC
            LIMIT 1
        """, (normalized_symbol, today))
        existing = cursor.fetchone()
        if existing:
            conn.close()
            item = dict(existing)
            item["exists_today"] = True
            return item

        cursor.execute("""
            INSERT INTO watch_notes (symbol, noted_at)
            VALUES (?, ?)
        """, (normalized_symbol, noted_at))
        note_id = cursor.lastrowid
        conn.commit()
        conn.close()

        return {
            "id": note_id,
            "symbol": normalized_symbol,
            "noted_at": noted_at,
            "exists_today": False
        }

    def get_watch_notes(self, limit: int = 200) -> List[Dict]:
        """获取明日观察列表"""
        safe_limit = max(1, min(int(limit), 1000))

        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, symbol, noted_at
            FROM watch_notes
            ORDER BY noted_at DESC, id DESC
            LIMIT ?
        """, (safe_limit,))
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def delete_watch_note(self, note_id: int) -> bool:
        """删除明日观察记录"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM watch_notes WHERE id = ?", (note_id,))
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return deleted

    def get_monthly_target(self) -> float:
        """获取月度目标"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT monthly_target FROM user_settings WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        return row['monthly_target'] if row else 30000

    def set_monthly_target(self, target: float):
        """设置月度目标"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE user_settings
            SET monthly_target = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
        """, (target,))
        conn.commit()
        conn.close()

    def get_monthly_pnl(self) -> float:
        """获取本月盈亏 (北京时间)"""
        conn = self._get_connection()
        cursor = conn.cursor()

        # 获取本月第一天的日期格式 YYYYMM01 (UTC+8)
        from datetime import datetime, timezone, timedelta
        utc8 = timezone(timedelta(hours=8))
        now = datetime.now(utc8)
        month_start = now.strftime('%Y%m01')

        cursor.execute("""
            SELECT COALESCE(SUM(pnl_net), 0) as monthly_pnl
            FROM trades
            WHERE date >= ?
        """, (month_start,))

        row = cursor.fetchone()
        conn.close()
        return float(row['monthly_pnl']) if row else 0.0

    def get_balance_history(self, start_time: Optional[datetime] = None, end_time: Optional[datetime] = None, limit: Optional[int] = None) -> List[Dict]:
        """
        获取余额历史记录，可按时间范围和限制数量过滤
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        query = "SELECT timestamp, balance, wallet_balance FROM balance_history WHERE 1=1"
        params = []

        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time.isoformat().replace('T', ' '))
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time.isoformat().replace('T', ' '))

        query += " ORDER BY timestamp DESC" # 先倒序获取，再反转，确保获取到最新的数据

        if limit:
            query += " LIMIT ?"
            params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        # 返回按时间升序排列
        return [dict(row) for row in reversed(rows)]

    def save_noon_loss_snapshot(self, snapshot: Dict) -> None:
        """保存/覆盖某天午间浮亏快照（按 snapshot_date 唯一）。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        rows_json = json.dumps(snapshot.get("rows", []), ensure_ascii=False)
        cursor.execute("""
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
        """, (
            str(snapshot.get("snapshot_date")),
            str(snapshot.get("snapshot_time")),
            int(snapshot.get("loss_count", 0)),
            float(snapshot.get("total_stop_loss", 0.0)),
            float(snapshot.get("pct_of_balance", 0.0)),
            float(snapshot.get("balance", 0.0)),
            rows_json
        ))
        conn.commit()
        conn.close()

    def _row_to_noon_loss_snapshot(self, row: sqlite3.Row) -> Dict:
        data = dict(row)
        rows_json = data.get("rows_json")
        try:
            data["rows"] = json.loads(rows_json) if rows_json else []
        except Exception:
            data["rows"] = []
        data.pop("rows_json", None)
        return data

    def get_noon_loss_snapshot_by_date(self, snapshot_date: str) -> Optional[Dict]:
        """按日期读取午间浮亏快照，snapshot_date 格式 YYYY-MM-DD。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT snapshot_date, snapshot_time, loss_count, total_stop_loss, pct_of_balance, balance, rows_json
            FROM noon_loss_snapshots
            WHERE snapshot_date = ?
            LIMIT 1
        """, (snapshot_date,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_noon_loss_snapshot(row)

    def get_latest_noon_loss_snapshot(self) -> Optional[Dict]:
        """读取最新一条午间浮亏快照（不晚于今天UTC+8）。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute("""
            SELECT snapshot_date, snapshot_time, loss_count, total_stop_loss, pct_of_balance, balance, rows_json
            FROM noon_loss_snapshots
            WHERE snapshot_date <= ?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT 1
        """, (today,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_noon_loss_snapshot(row)

    def save_leaderboard_snapshot(self, snapshot: Dict) -> None:
        """保存/覆盖某天的涨幅榜快照（按 snapshot_date 唯一）。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        rows_json = json.dumps(snapshot.get("rows", []), ensure_ascii=False)
        losers_rows_json = json.dumps(snapshot.get("losers_rows", []), ensure_ascii=False)
        all_rows_json = json.dumps(snapshot.get("all_rows", []), ensure_ascii=False)
        cursor.execute("""
            INSERT INTO leaderboard_snapshots (
                snapshot_date, snapshot_time, window_start_utc,
                candidates, effective, top_count, rows_json, losers_rows_json, all_rows_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                snapshot_time = excluded.snapshot_time,
                window_start_utc = excluded.window_start_utc,
                candidates = excluded.candidates,
                effective = excluded.effective,
                top_count = excluded.top_count,
                rows_json = excluded.rows_json,
                losers_rows_json = excluded.losers_rows_json,
                all_rows_json = excluded.all_rows_json,
                created_at = CURRENT_TIMESTAMP
        """, (
            str(snapshot.get("snapshot_date")),
            str(snapshot.get("snapshot_time")),
            str(snapshot.get("window_start_utc", "")),
            int(snapshot.get("candidates", 0)),
            int(snapshot.get("effective", 0)),
            int(snapshot.get("top", 0)),
            rows_json,
            losers_rows_json,
            all_rows_json
        ))
        conn.commit()
        conn.close()

    def _row_to_leaderboard_snapshot(self, row: sqlite3.Row) -> Dict:
        data = dict(row)
        rows_json = data.get("rows_json")
        losers_rows_json = data.get("losers_rows_json")
        all_rows_json = data.get("all_rows_json")
        try:
            data["rows"] = json.loads(rows_json) if rows_json else []
        except Exception:
            data["rows"] = []
        try:
            data["losers_rows"] = json.loads(losers_rows_json) if losers_rows_json else []
        except Exception:
            data["losers_rows"] = []
        try:
            data["all_rows"] = json.loads(all_rows_json) if all_rows_json else []
        except Exception:
            data["all_rows"] = []
        data.pop("rows_json", None)
        data.pop("losers_rows_json", None)
        data.pop("all_rows_json", None)
        data["top"] = data.pop("top_count", 0)
        return data

    def get_latest_leaderboard_snapshot(self) -> Optional[Dict]:
        """获取最近一条涨幅榜快照。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute("""
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, losers_rows_json, all_rows_json
            FROM leaderboard_snapshots
            WHERE snapshot_date <= ?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT 1
        """, (today,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_leaderboard_snapshot(row)

    def get_leaderboard_snapshot_by_date(self, snapshot_date: str) -> Optional[Dict]:
        """按日期获取涨幅榜快照，snapshot_date 格式 YYYY-MM-DD。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, losers_rows_json, all_rows_json
            FROM leaderboard_snapshots
            WHERE snapshot_date = ?
            LIMIT 1
        """, (snapshot_date,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_leaderboard_snapshot(row)

    def list_leaderboard_snapshot_dates(self, limit: int = 90) -> List[str]:
        """按时间倒序列出已存快照日期。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute("""
            SELECT snapshot_date
            FROM leaderboard_snapshots
            WHERE snapshot_date <= ?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT ?
        """, (today, int(limit)))
        rows = cursor.fetchall()
        conn.close()
        return [str(row["snapshot_date"]) for row in rows]

    def get_leaderboard_snapshots_between(self, start_date: str, end_date: str) -> List[Dict]:
        """获取日期区间内的快照（按 snapshot_date 倒序）。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, losers_rows_json, all_rows_json
            FROM leaderboard_snapshots
            WHERE snapshot_date >= ? AND snapshot_date <= ?
            ORDER BY snapshot_date DESC
        """, (start_date, end_date))
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_leaderboard_snapshot(row) for row in rows]

    @staticmethod
    def _lb_safe_float(value) -> Optional[float]:
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
        if num != num:  # NaN
            return None
        return num

    def _extract_all_rows(self, snapshot: Dict) -> List[Dict]:
        all_rows = snapshot.get("all_rows", [])
        if all_rows:
            return all_rows

        merged = {}
        for row in snapshot.get("rows", []) + snapshot.get("losers_rows", []):
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            merged[symbol] = row
        return list(merged.values())

    def _build_symbol_maps(self, snapshot: Dict) -> tuple[Dict[str, float], Dict[str, float]]:
        change_map: Dict[str, float] = {}
        price_map: Dict[str, float] = {}
        for row in self._extract_all_rows(snapshot):
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue

            change_val = self._lb_safe_float(row.get("change"))
            if change_val is not None:
                change_map[symbol] = change_val

            price_val = self._lb_safe_float(row.get("last_price"))
            if price_val is None:
                price_val = self._lb_safe_float(row.get("price"))
            if price_val is not None and price_val > 0:
                price_map[symbol] = price_val
        return change_map, price_map

    def build_leaderboard_daily_metrics(
        self,
        snapshot_date: str,
        drop_threshold_pct: float = -10.0,
    ) -> Optional[Dict]:
        """基于已保存快照，计算某天的三个指标。"""
        current_snapshot = self.get_leaderboard_snapshot_by_date(snapshot_date)
        if not current_snapshot:
            return None

        try:
            snap_date = datetime.strptime(snapshot_date, "%Y-%m-%d").date()
        except Exception:
            return None

        prev_date = (snap_date - timedelta(days=1)).strftime("%Y-%m-%d")
        prev2_date = (snap_date - timedelta(days=2)).strftime("%Y-%m-%d")
        prev_snapshot = self.get_leaderboard_snapshot_by_date(prev_date)
        prev2_snapshot = self.get_leaderboard_snapshot_by_date(prev2_date)

        prev_rows = prev_snapshot.get("rows", []) if prev_snapshot else []
        current_losers_rows = current_snapshot.get("losers_rows", [])

        # 指标1：前一日涨幅Top10进入次日跌幅Top10概率
        prev_rank_map = {}
        for idx, row in enumerate(prev_rows, start=1):
            symbol = str(row.get("symbol", "")).upper()
            if symbol:
                prev_rank_map[symbol] = idx

        metric1_hits_details = []
        for idx, row in enumerate(current_losers_rows, start=1):
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            prev_rank = prev_rank_map.get(symbol)
            if prev_rank is None:
                continue
            metric1_hits_details.append({
                "symbol": symbol,
                "current_loser_rank": idx,
                "prev_gainer_rank": prev_rank,
            })

        metric1_hits = len(metric1_hits_details)
        metric1_base_count = len(prev_rows)
        metric1_prob = None
        if metric1_base_count > 0:
            metric1_prob = round(metric1_hits * 100.0 / metric1_base_count, 2)
        metric1 = {
            "prev_snapshot_date": prev_snapshot.get("snapshot_date") if prev_snapshot else None,
            "hits": metric1_hits,
            "base_count": metric1_base_count,
            "probability_pct": metric1_prob,
            "symbols": [item["symbol"] for item in metric1_hits_details],
            "details": metric1_hits_details,
        }

        # 当前快照symbol映射（change/price）
        current_change_map, current_price_map = self._build_symbol_maps(current_snapshot)

        # 指标2：前一日涨幅Top10在次日<-10%的概率
        metric2_details = []
        metric2_hit_symbols = []
        for idx, row in enumerate(prev_rows, start=1):
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            next_change = current_change_map.get(symbol)
            prev_change = self._lb_safe_float(row.get("change"))
            is_hit = next_change is not None and next_change <= drop_threshold_pct
            metric2_details.append({
                "prev_rank": idx,
                "symbol": symbol,
                "prev_change_pct": prev_change,
                "next_change_pct": next_change,
                "is_hit": is_hit,
            })
            if is_hit:
                metric2_hit_symbols.append({
                    "symbol": symbol,
                    "next_change_pct": round(next_change, 2),
                })

        metric2_evaluated_count = sum(
            1 for item in metric2_details if item.get("next_change_pct") is not None
        )
        metric2_hits = sum(1 for item in metric2_details if item.get("is_hit"))
        metric2_prob = None
        if metric2_evaluated_count > 0:
            metric2_prob = round(metric2_hits * 100.0 / metric2_evaluated_count, 2)
        metric2 = {
            "base_snapshot_date": prev_snapshot.get("snapshot_date") if prev_snapshot else None,
            "target_snapshot_date": current_snapshot.get("snapshot_date"),
            "threshold_pct": drop_threshold_pct,
            "sample_size": len(metric2_details),
            "evaluated_count": metric2_evaluated_count,
            "hits": metric2_hits,
            "probability_pct": metric2_prob,
            "hit_symbols": metric2_hit_symbols,
            "details": metric2_details,
        }

        # 指标3：前两日涨幅Top10在48h后的涨跌幅
        prev2_rows = prev2_snapshot.get("rows", []) if prev2_snapshot else []
        metric3_details = []
        metric3_changes = []
        for idx, row in enumerate(prev2_rows, start=1):
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue

            entry_price = self._lb_safe_float(row.get("last_price"))
            if entry_price is None:
                entry_price = self._lb_safe_float(row.get("price"))
            current_price = current_price_map.get(symbol)

            change_pct = None
            if (
                entry_price is not None and entry_price > 0
                and current_price is not None and current_price > 0
            ):
                change_pct = (current_price / entry_price - 1.0) * 100.0
                metric3_changes.append(change_pct)

            metric3_details.append({
                "prev_rank": idx,
                "symbol": symbol,
                "entry_price": entry_price,
                "current_price": current_price,
                "change_pct": None if change_pct is None else round(change_pct, 4),
            })

        metric3_evaluated_count = len(metric3_changes)
        dist_lt_neg10 = 0
        dist_mid = 0
        dist_gt_pos10 = 0
        if metric3_evaluated_count > 0:
            dist_lt_neg10 = sum(1 for val in metric3_changes if val < -10.0)
            dist_gt_pos10 = sum(1 for val in metric3_changes if val > 10.0)
            dist_mid = metric3_evaluated_count - dist_lt_neg10 - dist_gt_pos10

        metric3 = {
            "base_snapshot_date": prev2_snapshot.get("snapshot_date") if prev2_snapshot else None,
            "target_snapshot_date": current_snapshot.get("snapshot_date"),
            "hold_hours": 48,
            "sample_size": len(metric3_details),
            "evaluated_count": metric3_evaluated_count,
            "distribution": {
                "lt_neg10": dist_lt_neg10,
                "mid": dist_mid,
                "gt_pos10": dist_gt_pos10,
            },
            "details": metric3_details,
        }

        return {
            "snapshot_date": snapshot_date,
            "metric1": metric1,
            "metric2": metric2,
            "metric3": metric3,
        }

    def save_leaderboard_daily_metrics(self, payload: Dict) -> None:
        """保存某天的三指标计算结果。"""
        snapshot_date = str(payload.get("snapshot_date", ""))
        if not snapshot_date:
            return

        metric1_json = json.dumps(payload.get("metric1", {}), ensure_ascii=False)
        metric2_json = json.dumps(payload.get("metric2", {}), ensure_ascii=False)
        metric3_json = json.dumps(payload.get("metric3", {}), ensure_ascii=False)

        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO leaderboard_daily_metrics (
                snapshot_date, metric1_json, metric2_json, metric3_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                metric1_json = excluded.metric1_json,
                metric2_json = excluded.metric2_json,
                metric3_json = excluded.metric3_json,
                updated_at = CURRENT_TIMESTAMP
        """, (snapshot_date, metric1_json, metric2_json, metric3_json))
        conn.commit()
        conn.close()

    def upsert_leaderboard_daily_metrics_for_date(self, snapshot_date: str) -> Optional[Dict]:
        """计算并保存某天三指标。"""
        payload = self.build_leaderboard_daily_metrics(snapshot_date=snapshot_date)
        if not payload:
            return None
        self.save_leaderboard_daily_metrics(payload)
        return payload

    def get_leaderboard_daily_metrics(self, snapshot_date: str) -> Optional[Dict]:
        """读取某天三指标。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT snapshot_date, metric1_json, metric2_json, metric3_json, created_at, updated_at
            FROM leaderboard_daily_metrics
            WHERE snapshot_date = ?
            LIMIT 1
        """, (snapshot_date,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None

        data = dict(row)
        for key in ("metric1_json", "metric2_json", "metric3_json"):
            try:
                data[key.replace("_json", "")] = json.loads(data.get(key) or "{}")
            except Exception:
                data[key.replace("_json", "")] = {}
            data.pop(key, None)
        return data

    def list_leaderboard_daily_metrics(self, limit: int = 90) -> List[Dict]:
        """按日期倒序列出三指标历史。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT snapshot_date, metric1_json, metric2_json, metric3_json, created_at, updated_at
            FROM leaderboard_daily_metrics
            ORDER BY snapshot_date DESC
            LIMIT ?
        """, (int(limit),))
        rows = cursor.fetchall()
        conn.close()

        result = []
        for row in rows:
            item = dict(row)
            for key in ("metric1_json", "metric2_json", "metric3_json"):
                try:
                    item[key.replace("_json", "")] = json.loads(item.get(key) or "{}")
                except Exception:
                    item[key.replace("_json", "")] = {}
                item.pop(key, None)
            result.append(item)
        return result

    def get_leaderboard_daily_metrics_by_dates(self, dates: List[str]) -> Dict[str, Dict]:
        """按日期批量读取三指标，返回 {snapshot_date: payload}。"""
        cleaned_dates = sorted({str(d) for d in dates if d})
        if not cleaned_dates:
            return {}

        placeholders = ",".join("?" for _ in cleaned_dates)
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT snapshot_date, metric1_json, metric2_json, metric3_json, created_at, updated_at
            FROM leaderboard_daily_metrics
            WHERE snapshot_date IN ({placeholders})
        """, cleaned_dates)
        rows = cursor.fetchall()
        conn.close()

        result: Dict[str, Dict] = {}
        for row in rows:
            item = dict(row)
            for key in ("metric1_json", "metric2_json", "metric3_json"):
                try:
                    item[key.replace("_json", "")] = json.loads(item.get(key) or "{}")
                except Exception:
                    item[key.replace("_json", "")] = {}
                item.pop(key, None)
            snapshot_date = str(item.get("snapshot_date", ""))
            if snapshot_date:
                result[snapshot_date] = item
        return result

    def save_rebound_7d_snapshot(self, snapshot: Dict) -> None:
        """保存/覆盖某天的7D反弹幅度榜快照（按 snapshot_date 唯一）。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        rows_json = json.dumps(snapshot.get("rows", []), ensure_ascii=False)
        all_rows_json = json.dumps(snapshot.get("all_rows", []), ensure_ascii=False)
        cursor.execute("""
            INSERT INTO rebound_7d_snapshots (
                snapshot_date, snapshot_time, window_start_utc,
                candidates, effective, top_count, rows_json, all_rows_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                snapshot_time = excluded.snapshot_time,
                window_start_utc = excluded.window_start_utc,
                candidates = excluded.candidates,
                effective = excluded.effective,
                top_count = excluded.top_count,
                rows_json = excluded.rows_json,
                all_rows_json = excluded.all_rows_json,
                created_at = CURRENT_TIMESTAMP
        """, (
            str(snapshot.get("snapshot_date")),
            str(snapshot.get("snapshot_time")),
            str(snapshot.get("window_start_utc", "")),
            int(snapshot.get("candidates", 0)),
            int(snapshot.get("effective", 0)),
            int(snapshot.get("top", 0)),
            rows_json,
            all_rows_json
        ))
        conn.commit()
        conn.close()

    def _row_to_rebound_7d_snapshot(self, row: sqlite3.Row) -> Dict:
        data = dict(row)
        rows_json = data.get("rows_json")
        all_rows_json = data.get("all_rows_json")
        try:
            data["rows"] = json.loads(rows_json) if rows_json else []
        except Exception:
            data["rows"] = []
        try:
            data["all_rows"] = json.loads(all_rows_json) if all_rows_json else []
        except Exception:
            data["all_rows"] = []
        data.pop("rows_json", None)
        data.pop("all_rows_json", None)
        data["top"] = data.pop("top_count", 0)
        return data

    def get_latest_rebound_7d_snapshot(self) -> Optional[Dict]:
        """获取最近一条7D反弹幅度榜快照。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute("""
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, all_rows_json
            FROM rebound_7d_snapshots
            WHERE snapshot_date <= ?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT 1
        """, (today,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_rebound_7d_snapshot(row)

    def get_rebound_7d_snapshot_by_date(self, snapshot_date: str) -> Optional[Dict]:
        """按日期获取7D反弹幅度榜快照，snapshot_date 格式 YYYY-MM-DD。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, all_rows_json
            FROM rebound_7d_snapshots
            WHERE snapshot_date = ?
            LIMIT 1
        """, (snapshot_date,))
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_rebound_7d_snapshot(row)

    def list_rebound_7d_snapshot_dates(self, limit: int = 90) -> List[str]:
        """按时间倒序列出已存7D反弹幅度榜快照日期。"""
        conn = self._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute("""
            SELECT snapshot_date
            FROM rebound_7d_snapshots
            WHERE snapshot_date <= ?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT ?
        """, (today, int(limit)))
        rows = cursor.fetchall()
        conn.close()
        return [str(row["snapshot_date"]) for row in rows]
