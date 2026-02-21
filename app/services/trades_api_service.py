from app.core.async_utils import run_in_thread
from app.repositories import SettingsRepository, TradeRepository
from app.services.trade_query_service import TradeQueryService


class TradesApiService:
    async def get_summary(self, *, db):
        service = TradeQueryService(db=db)
        return await run_in_thread(service.get_summary)

    async def get_trades(self, *, db):
        service = TradeQueryService(db=db)
        return await run_in_thread(service.get_trades_list)

    async def get_daily_stats(self, *, db):
        repo = TradeRepository(db)
        return await run_in_thread(repo.get_daily_stats)

    async def get_monthly_progress(self, *, db):
        repo = TradeRepository(db)
        target = await run_in_thread(repo.get_monthly_target)
        current_pnl = await run_in_thread(repo.get_monthly_pnl)
        progress = (current_pnl / target * 100) if target > 0 else 0
        return {"target": target, "current": current_pnl, "progress": round(progress, 1)}

    async def set_monthly_target(self, *, db, target: float):
        repo = TradeRepository(db)
        await run_in_thread(repo.set_monthly_target, target)
        return {"message": "目标已更新", "target": target}

    async def set_position_long_term(self, *, db, symbol: str, order_id: int, is_long_term: bool):
        settings_repo = SettingsRepository(db)
        await run_in_thread(settings_repo.set_position_long_term, symbol, order_id, is_long_term)
        return {"message": "状态已更新", "symbol": symbol, "is_long_term": is_long_term}
