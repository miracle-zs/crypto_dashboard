from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.deps import get_db
from app.models import DailyStats, Trade, TradeSummary
from app.security import require_admin_token
from app.services import TradesApiService

router = APIRouter()
service = TradesApiService()


@router.get("/api/summary", response_model=TradeSummary)
async def get_summary(db=Depends(get_db)):
    return await service.get_summary(db=db)


@router.get("/api/trades", response_model=list[Trade])
async def get_trades(
    limit: Optional[int] = Query(None, ge=1, le=5000, description="Maximum trades to return"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db=Depends(get_db),
):
    return await service.get_trades(db=db, limit=limit, offset=offset)


@router.get("/api/daily-stats", response_model=list[DailyStats])
async def get_daily_stats(db=Depends(get_db)):
    return await service.get_daily_stats(db=db)


@router.get("/api/monthly-progress")
async def get_monthly_progress(db=Depends(get_db)):
    return await service.get_monthly_progress(db=db)


@router.post("/api/monthly-target", dependencies=[Depends(require_admin_token)])
async def set_monthly_target(
    target: float = Query(..., description="Monthly target amount"),
    db=Depends(get_db)
):
    if target <= 0:
        raise HTTPException(status_code=400, detail="目标金额必须大于0")

    return await service.set_monthly_target(db=db, target=target)


@router.post("/api/positions/set-long-term", dependencies=[Depends(require_admin_token)])
async def set_long_term(
    symbol: str,
    order_id: int,
    is_long_term: bool,
    db=Depends(get_db)
):
    return await service.set_position_long_term(db=db, symbol=symbol, order_id=order_id, is_long_term=is_long_term)
