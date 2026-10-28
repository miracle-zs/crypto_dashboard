import asyncio
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.async_utils import run_in_thread
from app.core.deps import get_db
from app.models import DailyStats, FundingSummary, Trade, TradeAggregatesResponse, TradeSummary
from app.security import require_admin_token
from app.services import TradeQueryService
from app.repositories import SyncRepository, TradeRepository

router = APIRouter()


def _trade_queries(db) -> TradeQueryService:
    return TradeQueryService(db=db)


@router.get("/api/database/stats")
async def get_database_stats(db=Depends(get_db)):
    trade_repo = TradeRepository(db)
    sync_repo = SyncRepository(db)
    stats, sync_status = await asyncio.gather(
        run_in_thread(trade_repo.get_statistics),
        run_in_thread(sync_repo.get_sync_status),
    )
    return {"statistics": stats, "sync_status": sync_status}


@router.get("/api/summary", response_model=TradeSummary)
async def get_summary(db=Depends(get_db)):
    return await run_in_thread(_trade_queries(db).get_summary)


@router.get("/api/funding-summary", response_model=FundingSummary)
async def get_funding_summary(db=Depends(get_db)):
    return await run_in_thread(TradeRepository(db).get_funding_summary)


@router.get("/api/trades", response_model=list[Trade])
async def get_trades(
    limit: Optional[int] = Query(None, ge=1, le=5000, description="Maximum trades to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db=Depends(get_db),
):
    return await run_in_thread(_trade_queries(db).get_trades_list, limit, offset)


@router.get("/api/daily-stats", response_model=list[DailyStats])
async def get_daily_stats(db=Depends(get_db)):
    return await run_in_thread(_trade_queries(db).get_daily_stats)


@router.get("/api/trades-aggregates", response_model=TradeAggregatesResponse)
async def get_trades_aggregates(
    window: str = Query("all", pattern="^(all|7d|30d)$", description="Aggregate window: all/7d/30d"),
    db=Depends(get_db),
):
    return await run_in_thread(_trade_queries(db).get_trade_aggregates, window)


@router.get("/api/monthly-progress")
async def get_monthly_progress(db=Depends(get_db)):
    return await run_in_thread(_trade_queries(db).get_monthly_progress)


@router.post("/api/monthly-target", dependencies=[Depends(require_admin_token)])
async def set_monthly_target(
    target: float = Query(..., description="Monthly target amount"),
    db=Depends(get_db)
):
    if target <= 0:
        raise HTTPException(status_code=400, detail="目标金额必须大于0")

    return await run_in_thread(_trade_queries(db).set_monthly_target, target)


@router.post("/api/positions/set-long-term", dependencies=[Depends(require_admin_token)])
async def set_long_term(
    symbol: str,
    order_id: int,
    is_long_term: bool,
    db=Depends(get_db)
):
    return await run_in_thread(
        _trade_queries(db).set_position_long_term,
        symbol,
        order_id,
        is_long_term,
    )
