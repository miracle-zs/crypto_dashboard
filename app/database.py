"""
数据库持久化层 - 使用SQLite存储交易数据
"""
import sqlite3
import pandas as pd
import os
from pathlib import Path
import threading
from app.logger import logger
from app.core.database_schema import init_database_schema


class Database:
    """SQLite数据库管理类"""
    _init_lock = threading.Lock()
    _initialized_db_paths = set()

    def __init__(self, db_path: str = None):
        if db_path is None:
            # 默认数据库路径
            project_root = Path(__file__).parent.parent
            db_path = project_root / "data" / "trades.db"

        self.db_path = str(db_path)

        # 确保数据目录存在
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        # 初始化数据库（同一路径仅执行一次）
        self._init_database_once()

    def _db_identity(self) -> str:
        return str(Path(self.db_path).expanduser().resolve())

    def _init_database_once(self):
        identity = self._db_identity()
        if identity in self._initialized_db_paths:
            return
        with self._init_lock:
            if identity in self._initialized_db_paths:
                return
            self._init_database()
            self._initialized_db_paths.add(identity)

    def _get_connection(self):
        """获取数据库连接"""
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row  # 支持字典访问
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        return conn

    def _init_database(self):
        """初始化数据库表结构"""
        conn = self._get_connection()
        init_database_schema(conn, logger)

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

        upsert_rows = []
        for row in df.itertuples(index=False):
            upsert_rows.append(
                (
                    int(row.No),
                    row.Date,
                    row.Entry_Time,
                    row.Exit_Time,
                    row.Holding_Time,
                    row.Symbol,
                    row.Side,
                    float(row.Price_Change_Pct),
                    float(row.Entry_Amount),
                    float(row.Entry_Price),
                    float(row.Exit_Price),
                    float(row.Qty),
                    float(row.Fees),
                    float(row.PNL_Net),
                    row.Close_Type,
                    row.Return_Rate,
                    float(row.Open_Price),
                    float(row.PNL_Before_Fees),
                    int(row.Entry_Order_ID),
                    str(row.Exit_Order_ID),
                )
            )

        cursor.executemany(
            """
            INSERT INTO trades (
                no, date, entry_time, exit_time, holding_time, symbol, side,
                price_change_pct, entry_amount, entry_price, exit_price, qty,
                fees, pnl_net, close_type, return_rate, open_price,
                pnl_before_fees, entry_order_id, exit_order_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, entry_order_id, exit_order_id) DO UPDATE SET
                no = excluded.no,
                date = excluded.date,
                entry_time = excluded.entry_time,
                exit_time = excluded.exit_time,
                holding_time = excluded.holding_time,
                side = excluded.side,
                price_change_pct = excluded.price_change_pct,
                entry_amount = excluded.entry_amount,
                entry_price = excluded.entry_price,
                exit_price = excluded.exit_price,
                qty = excluded.qty,
                fees = excluded.fees,
                pnl_net = excluded.pnl_net,
                close_type = excluded.close_type,
                return_rate = excluded.return_rate,
                open_price = excluded.open_price,
                pnl_before_fees = excluded.pnl_before_fees,
                updated_at = CURRENT_TIMESTAMP
            """,
            upsert_rows,
        )

        conn.commit()
        conn.close()

        logger.info(f"数据库操作完成: 批量写入 {len(upsert_rows)} 条")
        return len(upsert_rows)
