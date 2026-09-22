import asyncio

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.core.async_utils import run_in_thread
from app.core.deps import get_db
from app.database import Database
from app.repositories import SyncRepository, TradeRepository
from app.static_assets import static_asset_url

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "dark_css_url": static_asset_url("/static/dark-unified.css"),
            "page_js_url": static_asset_url("/static/js/index-dashboard.js"),
        },
    )


@router.get("/api/status")
async def get_status(request: Request, db: Database = Depends(get_db)):
    trade_repo = TradeRepository(db)
    sync_repo = SyncRepository(db)
    stats, sync_status, quality = await asyncio.gather(
        run_in_thread(trade_repo.get_statistics),
        run_in_thread(sync_repo.get_sync_status),
        run_in_thread(sync_repo.get_data_quality_summary),
    )

    scheduler = getattr(request.app.state, "scheduler", None)
    next_run_time = None
    is_configured = scheduler is not None and scheduler.scheduler.running
    if scheduler:
        next_run = scheduler.get_next_run_time()
        next_run_time = next_run.isoformat() if next_run else None

    user_stream = getattr(request.app.state, "user_stream", None)
    user_stream_info = {
        "enabled": user_stream is not None,
        "connected": user_stream.is_connected if user_stream else False,
        "last_event_time_ms": user_stream.last_event_time_ms if user_stream else 0,
    }

    return {
        "status": "online",
        "readiness": quality.get("data_health", "healthy"),
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
            "data_quality": quality,
        },
        "user_stream": user_stream_info,
        "scheduler_running": is_configured,
    }

