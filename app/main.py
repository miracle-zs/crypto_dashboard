import os
from contextlib import asynccontextmanager

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
from app.core.deps import get_db as core_get_db
from app.core.metrics import log_api_metric, measure_ms
from app.database import Database
from app.logger import logger
from app.routes.leaderboard import router as leaderboard_router
from app.routes.system import router as system_router
from app.routes.trades import router as trades_router
from app.scheduler import get_scheduler, should_start_scheduler
from app.user_stream import BinanceUserDataStream

load_dotenv()

scheduler = None
user_stream = None


def get_db():
    yield from core_get_db()


@asynccontextmanager
async def lifespan(app: FastAPI):
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
            user_stream = BinanceUserDataStream(api_key=api_key, db=Database())
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

    try:
        yield
    finally:
        if scheduler:
            scheduler.stop()
            logger.info("定时任务调度器已停止")
        app.state.scheduler = None
        if user_stream:
            user_stream.stop()


app = FastAPI(title="Zero Gravity Dashboard", lifespan=lifespan)
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")
app.state.scheduler = None


@app.middleware("http")
async def request_metrics_middleware(request: Request, call_next):
    response = None
    with measure_ms("api.request", path=request.url.path, method=request.method) as snapshot:
        try:
            response = await call_next(request)
            return response
        finally:
            status_code = response.status_code if response is not None else 500
            log_api_metric(
                path=request.url.path,
                method=request.method,
                status_code=status_code,
                snapshot=snapshot,
            )

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


@app.get("/live-monitor", response_class=HTMLResponse)
async def read_live_monitor(request: Request):
    return templates.TemplateResponse(request, "live_monitor.html")


@app.get("/metrics", response_class=HTMLResponse)
async def read_metrics(request: Request):
    return templates.TemplateResponse(request, "metrics.html")


@app.get("/logs", response_class=HTMLResponse)
async def read_logs_page(request: Request):
    return templates.TemplateResponse(request, "logs.html")
