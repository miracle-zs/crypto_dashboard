import asyncio

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.database import Database

router = APIRouter()
templates = Jinja2Templates(directory="templates")


def get_db():
    db = Database()
    try:
        yield db
    finally:
        close = getattr(db, "close", None)
        if callable(close):
            close()


@router.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@router.get("/api/status")
async def get_status(request: Request, db: Database = Depends(get_db)):
    loop = asyncio.get_event_loop()
    stats, sync_status = await asyncio.gather(
        loop.run_in_executor(None, db.get_statistics),
        loop.run_in_executor(None, db.get_sync_status),
    )

    scheduler = getattr(request.app.state, "scheduler", None)
    next_run_time = None
    is_configured = scheduler is not None and scheduler.scheduler.running
    if scheduler:
        next_run = scheduler.get_next_run_time()
        next_run_time = next_run.isoformat() if next_run else None

    return {
        "status": "online",
        "configured": is_configured,
        "database": {
            "total_trades": stats.get("total_trades", 0),
            "unique_symbols": stats.get("unique_symbols", 0),
            "earliest_trade": stats.get("earliest_trade"),
            "latest_trade": stats.get("latest_trade"),
        },
        "sync": {
            "last_sync_time": sync_status.get("last_sync_time"),
            "status": sync_status.get("status", "idle"),
            "next_run_time": next_run_time,
            "error_message": sync_status.get("error_message"),
        },
        "scheduler_running": is_configured,
    }
