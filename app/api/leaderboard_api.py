from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.deps import get_db
from app.services import LeaderboardService

router = APIRouter()
service = LeaderboardService()


@router.get("/api/leaderboard")
async def get_leaderboard_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db=Depends(get_db),
):
    return await service.build_snapshot_response(db=db, date=date)
