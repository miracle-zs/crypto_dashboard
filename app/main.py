import os

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.api.balance_api import router as balance_api_router
from app.api.leaderboard_api import router as leaderboard_api_router
from app.api.positions_api import router as positions_api_router
from app.api.rebound_api import router as rebound_api_router
from app.api.system_api import router as system_api_router
from app.api.trades_api import router as trades_api_router
from app.api.watchnotes_api import router as watchnotes_api_router
from app.core.deps import get_db
from app.logger import logger
from app.routes.leaderboard import router as leaderboard_router
from app.routes.system import router as system_router
from app.routes.trades import router as trades_router
from app.scheduler import get_scheduler, should_start_scheduler
from app.user_stream import BinanceUserDataStream

load_dotenv()

app = FastAPI(title="Zero Gravity Dashboard")
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

scheduler = None
user_stream = None
app.state.scheduler = None

app.include_router(system_router)
app.include_router(trades_router)
app.include_router(leaderboard_router)
app.include_router(system_api_router)
app.include_router(trades_api_router)
app.include_router(leaderboard_api_router)
app.include_router(positions_api_router)
app.include_router(watchnotes_api_router)
app.include_router(rebound_api_router)
app.include_router(balance_api_router)


@app.on_event("startup")
async def startup_event():
    global scheduler, user_stream

    should_start, reason = should_start_scheduler()
    if should_start:
        api_key = os.getenv("BINANCE_API_KEY")
        scheduler = get_scheduler()
        scheduler.start()
        app.state.scheduler = scheduler
        logger.info("定时任务调度器已启动")

        enable_user_stream = os.getenv("ENABLE_USER_STREAM", "0").lower() in ("1", "true", "yes")
        if enable_user_stream:
            user_stream = BinanceUserDataStream(api_key=api_key, db=get_db())
            user_stream.start()
    else:
        app.state.scheduler = None
        if reason == "missing_api_keys":
            logger.warning("未配置API密钥，定时任务未启动")
        elif reason == "multi_worker_unsupported":
            logger.warning(
                "检测到多worker部署(WEB_CONCURRENCY/UVICORN_WORKERS > 1)，"
                "为避免重复调度已禁用内置scheduler。"
                "如需强制启用请设置 SCHEDULER_ALLOW_MULTI_WORKER=1。"
            )


@app.on_event("shutdown")
async def shutdown_event():
    global scheduler, user_stream
    if scheduler:
        scheduler.stop()
        logger.info("定时任务调度器已停止")
    app.state.scheduler = None
    if user_stream:
        user_stream.stop()


@app.get("/live-monitor", response_class=HTMLResponse)
async def read_live_monitor(request: Request):
    return templates.TemplateResponse("live_monitor.html", {"request": request})


@app.get("/metrics", response_class=HTMLResponse)
async def read_metrics(request: Request):
    return templates.TemplateResponse("metrics.html", {"request": request})


@app.get("/logs", response_class=HTMLResponse)
async def read_logs_page(request: Request):
    return templates.TemplateResponse("logs.html", {"request": request})
