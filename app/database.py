"""
数据库持久化层 - 使用SQLite存储交易数据
"""
import sqlite3
import pandas as pd
from datetime import datetime
from typing import Optional, List, Dict
import os
from pathlib import Path
from app.logger import logger


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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, order_id)
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_open_positions_date ON open_positions(date)
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

        conn.commit()
        conn.close()

    def save_trades(self, df: pd.DataFrame) -> int:
        """
        保存交易数据到数据库（批量插入或更新）

        Args:
            df: 交易数据DataFrame

        Returns:
            int: 新增或更新的记录数
        """
        if df.empty:
            return 0

        conn = self._get_connection()
        cursor = conn.cursor()

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
        保存未平仓订单（全量替换）

        Args:
            positions: 未平仓订单列表

        Returns:
            int: 保存的记录数
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        # 清空现有未平仓记录
        cursor.execute("DELETE FROM open_positions")

        # 插入新记录
        for pos in positions:
            cursor.execute("""
                INSERT INTO open_positions (
                    date, symbol, side, entry_time, entry_price, qty, entry_amount, order_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pos['date'],
                pos['symbol'],
                pos['side'],
                pos['entry_time'],
                pos['entry_price'],
                pos['qty'],
                pos['entry_amount'],
                pos['order_id']
            ))

        conn.commit()
        conn.close()

        return len(positions)

    def get_open_positions(self) -> List[Dict]:
        """获取所有未平仓订单"""
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM open_positions ORDER BY entry_time DESC")
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

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
