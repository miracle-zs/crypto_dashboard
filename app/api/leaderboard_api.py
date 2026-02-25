from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.deps import get_db
from app.models import LeaderboardSnapshotResponse, MetricsHistoryResponse, SnapshotDatesResponse
from app.services import LeaderboardHistoryService, LeaderboardService

router = APIRouter()
service = LeaderboardService()
history_service = LeaderboardHistoryService()


@router.get("/api/leaderboard", response_model=LeaderboardSnapshotResponse)
async def get_leaderboard_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db=Depends(get_db),
):
    return await service.build_snapshot_response(db=db, date=date)


@router.get("/api/leaderboard/dates", response_model=SnapshotDatesResponse)
async def get_leaderboard_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db=Depends(get_db),
):
    return await history_service.list_dates(db=db, limit=limit)


@router.get("/api/leaderboard/metrics-history", response_model=MetricsHistoryResponse)
async def get_leaderboard_metrics_history(
    limit: int = Query(60, ge=1, le=365),
    db=Depends(get_db),
):
    return await history_service.metrics_history(db=db, limit=limit)
