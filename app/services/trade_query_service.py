"""
Services module - 从数据库读取交易数据
"""
from typing import List, Optional

import pandas as pd

from app.database import Database
from app.models import Trade, TradeSummary
from app.repositories import TradeRepository


class TradeQueryService:
    """交易数据查询服务 - 从数据库读取 (Read-Only)"""

    def __init__(self, db: Optional[Database] = None, api_key: str = None, api_secret: str = None):
        # API密钥用于定时任务，这里只负责读取数据库
        self.db = db or Database()
        self.repo = TradeRepository(self.db)

    def get_summary(self) -> TradeSummary:
        """获取交易汇总数据和统计指标"""
        cached = self.repo.get_trade_summary()
        if cached:
            latest_total_trades = self.repo.get_cached_total_trades()
            if latest_total_trades is None:
                stats = self.repo.get_statistics()
                latest_total_trades = stats.get("total_trades", 0)

            if cached.get('total_trades', 0) == latest_total_trades:
                return TradeSummary(**cached)

        summary = self.repo.recompute_trade_summary()
        return TradeSummary(**summary)

    def get_trades_list(self, limit: Optional[int] = None, offset: int = 0) -> List[Trade]:
        """获取交易记录列表"""
        df = self.repo.get_all_trades(limit=limit, offset=offset)

        if df.empty:
            return []

        # Vectorize duration calculation to avoid per-row datetime parsing overhead.
        entry_ts = pd.to_datetime(df["Entry_Time"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
        exit_ts = pd.to_datetime(df["Exit_Time"], format="%Y-%m-%d %H:%M:%S", errors="coerce")
        duration_minutes = (
            (exit_ts - entry_ts).dt.total_seconds().div(60).fillna(0.0).astype(float).tolist()
        )

        trades = []
        for row, duration_min in zip(df.itertuples(index=False), duration_minutes):
            trade = Trade(
                no=int(row.No),
                date=str(row.Date),
                entry_time=str(row.Entry_Time),
                exit_time=str(row.Exit_Time),
                holding_time=str(row.Holding_Time),
                duration_minutes=float(duration_min),
                symbol=str(row.Symbol),
                side=str(row.Side),
                price_change_pct=float(row.Price_Change_Pct),
                entry_amount=float(row.Entry_Amount),
                entry_price=float(row.Entry_Price),
                exit_price=float(row.Exit_Price),
                qty=float(row.Qty),
                fees=float(row.Fees),
                pnl_net=float(row.PNL_Net),
                close_type=str(row.Close_Type),
                return_rate=str(row.Return_Rate),
                open_price=float(row.Open_Price),
                pnl_before_fees=float(row.PNL_Before_Fees),
                entry_order_id=int(row.Entry_Order_ID),
                exit_order_id=str(row.Exit_Order_ID)
            )
            trades.append(trade)

        return trades
