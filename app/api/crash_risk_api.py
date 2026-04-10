from fastapi import APIRouter, Depends

from app.core.async_utils import run_in_thread
from app.core.deps import get_db
from app.models import CrashRiskResponse
from app.services import CrashRiskService

router = APIRouter()
service = CrashRiskService()


async def _run_snapshot_job(func, db):
    return await run_in_thread(func, db)


@router.get("/api/crash-risk", response_model=CrashRiskResponse)
async def get_crash_risk(db=Depends(get_db)):
    return await _run_snapshot_job(service.build_from_leaderboard_snapshot, db)


@router.post("/api/crash-risk/refresh", response_model=CrashRiskResponse, status_code=200)
async def refresh_crash_risk(db=Depends(get_db)):
    return await _run_snapshot_job(service.refresh_from_leaderboard_snapshot, db)
