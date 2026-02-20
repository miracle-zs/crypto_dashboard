import asyncio

from app.repositories import SettingsRepository
from app.services.trade_query_service import TradeQueryService


class TradesApiService:
    async def get_summary(self, *, db):
        loop = asyncio.get_event_loop()
        service = TradeQueryService(db=db)
        return await loop.run_in_executor(None, service.get_summary)

    async def get_trades(self, *, db):
        loop = asyncio.get_event_loop()
        service = TradeQueryService(db=db)
        return await loop.run_in_executor(None, service.get_trades_list)

    async def get_daily_stats(self, *, db):
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, db.get_daily_stats)

    async def get_monthly_progress(self, *, db):
        loop = asyncio.get_event_loop()
        target = await loop.run_in_executor(None, db.get_monthly_target)
        current_pnl = await loop.run_in_executor(None, db.get_monthly_pnl)
        progress = (current_pnl / target * 100) if target > 0 else 0
        return {"target": target, "current": current_pnl, "progress": round(progress, 1)}

    async def set_monthly_target(self, *, db, target: float):
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, db.set_monthly_target, target)
        return {"message": "目标已更新", "target": target}

    async def set_position_long_term(self, *, db, symbol: str, order_id: int, is_long_term: bool):
        loop = asyncio.get_event_loop()
        settings_repo = SettingsRepository(db)
        await loop.run_in_executor(None, settings_repo.set_position_long_term, symbol, order_id, is_long_term)
        return {"message": "状态已更新", "symbol": symbol, "is_long_term": is_long_term}
