import asyncio
from functools import partial

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from app.core.deps import get_db
from app.logger import read_logs
from app.security import require_admin_token

router = APIRouter()


@router.get("/api/logs")
async def get_logs(lines: int = Query(200, description="Number of log lines to return")):
    loop = asyncio.get_event_loop()
    log_lines = await loop.run_in_executor(None, read_logs, lines)
    return {"logs": log_lines}


@router.get("/api/noon-loss-review-history")
async def get_noon_loss_review_history(
    limit: int = Query(7, ge=1, le=90),
    db=Depends(get_db)
):
    loop = asyncio.get_event_loop()
    rows, summary = await asyncio.gather(
        loop.run_in_executor(None, partial(db.list_noon_loss_review_history, limit)),
        loop.run_in_executor(None, db.get_noon_loss_review_history_summary)
    )
    return {"rows": rows, "summary": summary}


@router.get("/api/sync-runs")
async def get_sync_runs(
    limit: int = Query(100, ge=1, le=500),
    db=Depends(get_db)
):
    loop = asyncio.get_event_loop()
    rows = await loop.run_in_executor(None, partial(db.list_sync_run_logs, limit))
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
