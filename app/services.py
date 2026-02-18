"""
Services module - 从数据库读取交易数据
"""
from app.database import Database
from app.models import Trade, TradeSummary
from typing import List, Optional
from datetime import datetime


class TradeQueryService:
    """交易数据查询服务 - 从数据库读取 (Read-Only)"""

    def __init__(self, db: Optional[Database] = None, api_key: str = None, api_secret: str = None):
        # API密钥用于定时任务，这里只负责读取数据库
        self.db = db or Database()

    def get_summary(self) -> TradeSummary:
        """获取交易汇总数据和统计指标"""
        cached = self.db.get_trade_summary()
        if cached:
            stats = self.db.get_statistics()
            if cached.get('total_trades', 0) == stats.get('total_trades', 0):
                return TradeSummary(**cached)

        summary = self.db.recompute_trade_summary()
        return TradeSummary(**summary)

    def get_trades_list(self) -> List[Trade]:
        """获取交易记录列表"""
        df = self.db.get_all_trades()

        if df.empty:
            return []

        trades = []
        for _, row in df.iterrows():
            # Calculate duration in minutes
            try:
                entry_dt = datetime.strptime(str(row['Entry_Time']), "%Y-%m-%d %H:%M:%S")
                exit_dt = datetime.strptime(str(row['Exit_Time']), "%Y-%m-%d %H:%M:%S")
                duration_min = (exit_dt - entry_dt).total_seconds() / 60.0
            except Exception:
                duration_min = 0.0

            trade = Trade(
                no=int(row['No']),
                date=str(row['Date']),
                entry_time=str(row['Entry_Time']),
                exit_time=str(row['Exit_Time']),
                holding_time=str(row['Holding_Time']),
                duration_minutes=float(duration_min),
                symbol=str(row['Symbol']),
                side=str(row['Side']),
                price_change_pct=float(row['Price_Change_Pct']),
                entry_amount=float(row['Entry_Amount']),
                entry_price=float(row['Entry_Price']),
                exit_price=float(row['Exit_Price']),
                qty=float(row['Qty']),
                fees=float(row['Fees']),
                pnl_net=float(row['PNL_Net']),
                close_type=str(row['Close_Type']),
                return_rate=str(row['Return_Rate']),
                open_price=float(row['Open_Price']),
                pnl_before_fees=float(row['PNL_Before_Fees']),
                entry_order_id=int(row['Entry_Order_ID']),
                exit_order_id=str(row['Exit_Order_ID'])
            )
            trades.append(trade)

        return trades
