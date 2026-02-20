import asyncio
from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.deps import get_db
from app.models import BalanceHistoryItem
from app.services.balance_service import BalanceService

router = APIRouter()
service = BalanceService()


@router.get("/api/balance-history", response_model=list[BalanceHistoryItem])
async def get_balance_history(
    time_range: Optional[str] = Query("1d", description="Time range for balance history (e.g., 1h, 1d, 1w, 1m, 1y)"),
    db=Depends(get_db),
):
    loop = asyncio.get_event_loop()
    return await service.build_balance_history_response(db=db, time_range=time_range or "1d", loop=loop)
