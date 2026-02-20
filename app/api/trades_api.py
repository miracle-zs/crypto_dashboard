import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.deps import get_db
from app.models import DailyStats, Trade, TradeSummary
from app.security import require_admin_token
from app.services import TradeQueryService

router = APIRouter()


def get_trade_service(db=Depends(get_db)):
    return TradeQueryService(db=db)


@router.get("/api/summary", response_model=TradeSummary)
async def get_summary(service: TradeQueryService = Depends(get_trade_service)):
    loop = asyncio.get_event_loop()
    summary = await loop.run_in_executor(None, service.get_summary)
    return summary


@router.get("/api/trades", response_model=list[Trade])
async def get_trades(service: TradeQueryService = Depends(get_trade_service)):
    loop = asyncio.get_event_loop()
    trades = await loop.run_in_executor(None, service.get_trades_list)
    return trades


@router.get("/api/daily-stats", response_model=list[DailyStats])
async def get_daily_stats(db=Depends(get_db)):
    loop = asyncio.get_event_loop()
    daily_stats = await loop.run_in_executor(None, db.get_daily_stats)
    return daily_stats


@router.get("/api/monthly-progress")
async def get_monthly_progress(db=Depends(get_db)):
    loop = asyncio.get_event_loop()
    target = await loop.run_in_executor(None, db.get_monthly_target)
    current_pnl = await loop.run_in_executor(None, db.get_monthly_pnl)
    progress = (current_pnl / target * 100) if target > 0 else 0
    return {
        "target": target,
        "current": current_pnl,
        "progress": round(progress, 1)
    }


@router.post("/api/monthly-target", dependencies=[Depends(require_admin_token)])
async def set_monthly_target(
    target: float = Query(..., description="Monthly target amount"),
    db=Depends(get_db)
):
    if target <= 0:
        raise HTTPException(status_code=400, detail="目标金额必须大于0")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, db.set_monthly_target, target)
    return {"message": "目标已更新", "target": target}


@router.post("/api/positions/set-long-term", dependencies=[Depends(require_admin_token)])
async def set_long_term(
    symbol: str,
    order_id: int,
    is_long_term: bool,
    db=Depends(get_db)
):
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, db.set_position_long_term, symbol, order_id, is_long_term)
    return {"message": "状态已更新", "symbol": symbol, "is_long_term": is_long_term}
