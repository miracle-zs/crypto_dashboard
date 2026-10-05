import asyncio
from functools import partial

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.async_utils import run_in_thread
from app.core.deps import get_db
from app.logger import read_logs
from app.models import LogsResponse, NoonLossReviewHistoryResponse, SyncRunsResponse
from app.repositories import SnapshotRepository, SyncRepository
from app.repositories import TradeRepository
from app.security import require_admin_token

router = APIRouter()


@router.get("/api/status")
async def get_status(request: Request, db=Depends(get_db)):
    trade_repo = TradeRepository(db)
    sync_repo = SyncRepository(db)
    stats, sync_status, quality = await asyncio.gather(
        run_in_thread(trade_repo.get_statistics),
        run_in_thread(sync_repo.get_sync_status),
        run_in_thread(sync_repo.get_data_quality_summary),
    )
    scheduler = getattr(request.app.state, "scheduler", None)
    next_run = scheduler.get_next_run_time() if scheduler else None
    user_stream = getattr(request.app.state, "user_stream", None)
    return {
        "status": "online",
        "readiness": quality.get("data_health", "healthy"),
        "configured": bool(scheduler and scheduler.scheduler.running),
        "database": stats,
        "sync": {**sync_status, "next_run_time": next_run.isoformat() if next_run else None, "data_quality": quality},
        "user_stream": {
            "enabled": user_stream is not None,
            "connected": user_stream.is_connected if user_stream else False,
            "connected_at_ms": getattr(user_stream, "connected_at_ms", 0),
            "last_heartbeat_time_ms": getattr(user_stream, "last_heartbeat_time_ms", 0),
            "last_event_time_ms": getattr(user_stream, "last_event_time_ms", 0),
        },
        "scheduler_running": bool(scheduler and scheduler.scheduler.running),
    }


@router.get("/api/logs", response_model=LogsResponse)
async def get_logs(lines: int = Query(200, ge=1, le=5000, description="Number of log lines to return")):
    return {"logs": await run_in_thread(read_logs, lines)}


@router.get("/api/noon-loss-review-history", response_model=NoonLossReviewHistoryResponse)
async def get_noon_loss_review_history(
    limit: int = Query(7, ge=1, le=90),
    db=Depends(get_db)
):
    snapshot_repo = SnapshotRepository(db)
    rows, summary = await asyncio.gather(
        run_in_thread(partial(snapshot_repo.list_noon_loss_review_history, limit)),
        run_in_thread(snapshot_repo.get_noon_loss_review_history_summary),
    )
    return {"rows": rows, "summary": summary}


@router.get("/api/sync-runs", response_model=SyncRunsResponse)
async def get_sync_runs(
    limit: int = Query(100, ge=1, le=500),
    db=Depends(get_db)
):
    rows = await run_in_thread(partial(SyncRepository(db).list_sync_run_logs, limit))
    return {"rows": rows}


@router.post("/api/sync/manual", dependencies=[Depends(require_admin_token)])
async def manual_sync(request: Request):
    scheduler = getattr(request.app.state, "scheduler", None)
    if not scheduler:
        raise HTTPException(status_code=500, detail="调度器未初始化")

    try:
        scheduler.scheduler.add_job(
            func=scheduler.sync_trades_data,
            id='manual_sync',
            replace_existing=True
        )
        return {"message": "手动同步已触发", "status": "started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"同步失败: {str(e)}")
