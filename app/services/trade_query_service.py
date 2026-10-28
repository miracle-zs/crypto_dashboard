"""
Services module - 从数据库读取交易数据
"""
from typing import List, Optional

import pandas as pd

from app.database import Database
from app.models import Trade, TradeSummary
from app.repositories import SettingsRepository, TradeRepository


class TradeQueryService:
    """交易数据查询服务 - 从数据库读取 (Read-Only)"""

    def __init__(self, db: Database):
        self.db = db
        self.repo = TradeRepository(self.db)

    def get_summary(self) -> TradeSummary:
        """获取交易汇总数据和统计指标"""
        cached = self.repo.get_trade_summary()
        if cached:
            latest_total_trades = self.repo.get_cached_total_trades()
            if latest_total_trades is None:
                stats = self.repo.get_statistics()
                latest_total_trades = stats.get("total_trades", 0)

            summary_is_complete = (
                cached.get("max_single_loss") is not None
                and cached.get("max_drawdown") is not None
                and len(cached.get("equity_curve") or []) == int(cached.get("total_trades") or 0)
            )
            if cached.get('total_trades', 0) == latest_total_trades and summary_is_complete:
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

    def get_daily_stats(self):
        return self.repo.get_daily_stats()

    def get_trade_aggregates(self, window: str = "all"):
        return self.repo.get_trade_aggregates(window)

    def get_monthly_progress(self):
        target = self.repo.get_monthly_target()
        current_pnl = self.repo.get_monthly_pnl()
        progress = (current_pnl / target * 100) if target > 0 else 0
        return {"target": target, "current": current_pnl, "progress": round(progress, 1)}

    def set_monthly_target(self, target: float):
        self.repo.set_monthly_target(target)
        return {"message": "目标已更新", "target": target}

    def set_position_long_term(self, symbol: str, order_id: int, is_long_term: bool):
        SettingsRepository(self.db).set_position_long_term(symbol, order_id, is_long_term)
        return {"message": "状态已更新", "symbol": symbol, "is_long_term": is_long_term}
