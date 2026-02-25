from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.deps import get_db
from app.models import ReboundSnapshotResponse, SnapshotDatesResponse
from app.services import ReboundService

router = APIRouter()
service = ReboundService()


@router.get("/api/rebound-7d", response_model=ReboundSnapshotResponse)
async def get_rebound_7d_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db=Depends(get_db),
):
    return await service.get_snapshot_response(db=db, date=date, window="7d")


@router.get("/api/rebound-7d/dates", response_model=SnapshotDatesResponse)
async def get_rebound_7d_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db=Depends(get_db),
):
    return await service.list_dates(db=db, window="7d", limit=limit)


@router.get("/api/rebound-30d", response_model=ReboundSnapshotResponse)
async def get_rebound_30d_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db=Depends(get_db),
):
    return await service.get_snapshot_response(db=db, date=date, window="30d")


@router.get("/api/rebound-30d/dates", response_model=SnapshotDatesResponse)
async def get_rebound_30d_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db=Depends(get_db),
):
    return await service.list_dates(db=db, window="30d", limit=limit)


@router.get("/api/rebound-60d", response_model=ReboundSnapshotResponse)
async def get_rebound_60d_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db=Depends(get_db),
):
    return await service.get_snapshot_response(db=db, date=date, window="60d")


@router.get("/api/rebound-60d/dates", response_model=SnapshotDatesResponse)
async def get_rebound_60d_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db=Depends(get_db),
):
    return await service.list_dates(db=db, window="60d", limit=limit)
