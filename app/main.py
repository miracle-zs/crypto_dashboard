from fastapi import FastAPI, Request, HTTPException, Query, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from app.models import BalanceHistoryItem
from app.scheduler import get_scheduler, should_start_scheduler
from app.database import Database
from app.user_stream import BinanceUserDataStream
from app.core.deps import get_db
from app.core.time import UTC8
import os
from dotenv import load_dotenv
from typing import List, Optional
from datetime import datetime, timedelta, timezone
from app.logger import logger, read_logs
from app.routes.leaderboard import router as leaderboard_router
from app.routes.system import router as system_router
from app.routes.trades import router as trades_router
from app.api.leaderboard_api import router as leaderboard_api_router
from app.api.positions_api import router as positions_api_router
from app.api.system_api import router as system_api_router
from app.api.trades_api import router as trades_api_router
from app.api.watchnotes_api import router as watchnotes_api_router
import asyncio
from functools import partial

# Load environment variables
load_dotenv()

app = FastAPI(title="Zero Gravity Dashboard")

# Setup templates
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

# Initialize database
# db = Database()  <-- Deprecated global

# Public REST client (market data)
# public_rest = BinanceFuturesRestClient() <-- Deprecated global

# Scheduler instance
scheduler = None
user_stream = None
app.state.scheduler = None


app.include_router(system_router)
app.include_router(trades_router)
app.include_router(leaderboard_router)
app.include_router(leaderboard_api_router)
app.include_router(positions_api_router)
app.include_router(system_api_router)
app.include_router(trades_api_router)
app.include_router(watchnotes_api_router)


def _normalize_symbol(symbol: str) -> str:
    return symbol if symbol.endswith("USDT") else f"{symbol}USDT"


@app.on_event("startup")
async def startup_event():
    """应用启动时执行"""
    global scheduler, user_stream

    should_start, reason = should_start_scheduler()
    if should_start:
        api_key = os.getenv('BINANCE_API_KEY')
        scheduler = get_scheduler()
        # scheduler.start() 通常是非阻塞的，或者已内部处理线程
        scheduler.start()
        app.state.scheduler = scheduler
        logger.info("定时任务调度器已启动")

        enable_user_stream = os.getenv("ENABLE_USER_STREAM", "0").lower() in ("1", "true", "yes")
        if enable_user_stream:
            # UserDataStream 内部可能涉及网络请求，但在 startup 中初始化通常没问题
            # 如果 start() 包含阻塞循环，应该在线程中运行，这里假设它是异步启动或在新线程启动
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
    """应用关闭时执行"""
    global scheduler, user_stream
    if scheduler:
        scheduler.stop()
        logger.info("定时任务调度器已停止")
    app.state.scheduler = None
    if user_stream:
        user_stream.stop()


@app.get("/live-monitor", response_class=HTMLResponse)
async def read_live_monitor(request: Request):
    """Serve the live monitor HTML"""
    return templates.TemplateResponse("live_monitor.html", {"request": request})


@app.get("/metrics", response_class=HTMLResponse)
async def read_metrics(request: Request):
    """Serve the metrics documentation HTML"""
    return templates.TemplateResponse("metrics.html", {"request": request})


@app.get("/logs", response_class=HTMLResponse)
async def read_logs_page(request: Request):
    """Serve the logs viewer HTML"""
    return templates.TemplateResponse("logs.html", {"request": request})




@app.get("/api/leaderboard/dates")
async def get_leaderboard_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db: Database = Depends(get_db)
):
    """获取涨幅榜快照日期列表（倒序）"""
    loop = asyncio.get_event_loop()
    dates = await loop.run_in_executor(None, db.list_leaderboard_snapshot_dates, limit)
    return {"dates": dates}


async def _get_rebound_snapshot_response(
    *,
    date: Optional[str],
    db: Database,
    getter_by_date,
    getter_latest,
    empty_message: str
):
    loop = asyncio.get_event_loop()
    if date:
        try:
            requested_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            return {
                "ok": False,
                "reason": "invalid_date",
                "message": f"日期格式错误: {date}，请使用 YYYY-MM-DD"
            }
        today_utc8 = datetime.now(UTC8).date()
        if requested_date > today_utc8:
            return {
                "ok": False,
                "reason": "future_date",
                "message": f"请求日期 {date} 超过今天 {today_utc8.strftime('%Y-%m-%d')}"
            }
        snapshot = await loop.run_in_executor(None, getter_by_date, date)
    else:
        snapshot = await loop.run_in_executor(None, getter_latest)

    if not snapshot:
        return {
            "ok": False,
            "reason": "no_snapshot",
            "message": empty_message
        }

    open_positions = await loop.run_in_executor(None, db.get_open_positions)
    held_symbols = set()
    for pos in open_positions:
        sym = str(pos.get("symbol", "")).upper().strip()
        if not sym:
            continue
        held_symbols.add(sym)
        held_symbols.add(_normalize_symbol(sym))

    enriched_rows = []
    for idx, row in enumerate(snapshot.get("rows", []), start=1):
        symbol = str(row.get("symbol", "")).upper()
        enriched_rows.append({
            **row,
            "rank": idx,
            "is_held": symbol in held_symbols,
        })

    snapshot["rows"] = enriched_rows
    snapshot["top_count"] = len(enriched_rows)
    snapshot.pop("all_rows", None)
    return {"ok": True, **snapshot}


@app.get("/api/rebound-7d")
async def get_rebound_7d_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db: Database = Depends(get_db)
):
    """获取14D反弹幅度榜历史快照（默认返回最新一条，不进行实时计算）"""
    return await _get_rebound_snapshot_response(
        date=date,
        db=db,
        getter_by_date=db.get_rebound_7d_snapshot_by_date,
        getter_latest=db.get_latest_rebound_7d_snapshot,
        empty_message="暂无快照数据，请等待下一次07:30定时任务生成（14D）"
    )


@app.get("/api/rebound-7d/dates")
async def get_rebound_7d_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db: Database = Depends(get_db)
):
    """获取14D反弹幅度榜快照日期列表（倒序）"""
    loop = asyncio.get_event_loop()
    dates = await loop.run_in_executor(None, db.list_rebound_7d_snapshot_dates, limit)
    return {"dates": dates}


@app.get("/api/rebound-30d")
async def get_rebound_30d_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db: Database = Depends(get_db)
):
    """获取30D反弹幅度榜历史快照（默认返回最新一条，不进行实时计算）"""
    return await _get_rebound_snapshot_response(
        date=date,
        db=db,
        getter_by_date=db.get_rebound_30d_snapshot_by_date,
        getter_latest=db.get_latest_rebound_30d_snapshot,
        empty_message="暂无快照数据，请等待下一次07:30定时任务生成（30D）"
    )


@app.get("/api/rebound-30d/dates")
async def get_rebound_30d_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db: Database = Depends(get_db)
):
    """获取30D反弹幅度榜快照日期列表（倒序）"""
    loop = asyncio.get_event_loop()
    dates = await loop.run_in_executor(None, db.list_rebound_30d_snapshot_dates, limit)
    return {"dates": dates}


@app.get("/api/rebound-60d")
async def get_rebound_60d_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db: Database = Depends(get_db)
):
    """获取60D反弹幅度榜历史快照（默认返回最新一条，不进行实时计算）"""
    return await _get_rebound_snapshot_response(
        date=date,
        db=db,
        getter_by_date=db.get_rebound_60d_snapshot_by_date,
        getter_latest=db.get_latest_rebound_60d_snapshot,
        empty_message="暂无快照数据，请等待下一次07:30定时任务生成（60D）"
    )


@app.get("/api/rebound-60d/dates")
async def get_rebound_60d_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db: Database = Depends(get_db)
):
    """获取60D反弹幅度榜快照日期列表（倒序）"""
    loop = asyncio.get_event_loop()
    dates = await loop.run_in_executor(None, db.list_rebound_60d_snapshot_dates, limit)
    return {"dates": dates}


@app.get("/api/leaderboard/metrics-history")
async def get_leaderboard_metrics_history(
    limit: int = Query(60, ge=1, le=365),
    db: Database = Depends(get_db)
):
    """按日期返回三指标历史（只回填缺失日期）。"""
    loop = asyncio.get_event_loop()
    dates = await loop.run_in_executor(None, db.list_leaderboard_snapshot_dates, limit)
    if not dates:
        return {"rows": []}

    metrics_map = await loop.run_in_executor(
        None, db.get_leaderboard_daily_metrics_by_dates, dates
    )
    missing_dates = [d for d in dates if d not in metrics_map]
    for d in missing_dates:
        payload = await loop.run_in_executor(None, db.upsert_leaderboard_daily_metrics_for_date, d)
        if payload:
            metrics_map[d] = payload

    rows = []
    for snapshot_date in dates:
        row = metrics_map.get(snapshot_date)
        if not row:
            continue
        metric1 = row.get("metric1", {}) or {}
        metric2 = row.get("metric2", {}) or {}
        metric3 = row.get("metric3", {}) or {}
        eval3 = int(metric3.get("evaluated_count") or 0)
        dist3 = metric3.get("distribution", {}) or {}
        lt_neg10 = int(dist3.get("lt_neg10") or 0)
        gt_pos10 = int(dist3.get("gt_pos10") or 0)
        lt_neg10_pct = round(lt_neg10 * 100.0 / eval3, 2) if eval3 > 0 else None
        gt_pos10_pct = round(gt_pos10 * 100.0 / eval3, 2) if eval3 > 0 else None

        rows.append({
            "snapshot_date": snapshot_date,
            "metric1": metric1,
            "metric2": metric2,
            "metric3": metric3,
            "m1_prob_pct": metric1.get("probability_pct"),
            "m1_hits": metric1.get("hits"),
            "m2_prob_pct": metric2.get("probability_pct"),
            "m2_hits": metric2.get("hits"),
            "m3_eval_count": eval3,
            "m3_lt_neg10_pct": lt_neg10_pct,
            "m3_gt_pos10_pct": gt_pos10_pct,
        })
    return {"rows": rows}


@app.get("/api/balance-history", response_model=List[BalanceHistoryItem])
async def get_balance_history(
    time_range: Optional[str] = Query("1d", description="Time range for balance history (e.g., 1h, 1d, 1w, 1m, 1y)"),
    db: Database = Depends(get_db)
):
    """Get account balance history with optional time range filtering"""
    end_time = datetime.utcnow()
    start_time = None

    if time_range == "1h":
        start_time = end_time - timedelta(hours=1)
    elif time_range == "1d":
        start_time = end_time - timedelta(days=1)
    elif time_range == "1w":
        start_time = end_time - timedelta(weeks=1)
    elif time_range == "1m":
        start_time = end_time - timedelta(days=30)
    elif time_range == "1y":
        start_time = end_time - timedelta(days=365)
    else:
        start_time = end_time - timedelta(hours=2)

    loop = asyncio.get_event_loop()

    # 在 executor 中运行数据库查询
    history_data = await loop.run_in_executor(
        None,
        partial(db.get_balance_history, start_time=start_time, end_time=end_time)
    )
    if not history_data:
        return []

    transfers = await loop.run_in_executor(None, db.get_transfers)

    # 预处理 transfers: 解析时间并排序
    sorted_transfers = []
    for t in transfers:
        try:
            t_dt = datetime.fromisoformat(t['timestamp']).replace(tzinfo=timezone.utc)
            sorted_transfers.append((t_dt, float(t['amount'])))
        except (TypeError, ValueError):
            continue
    sorted_transfers.sort(key=lambda x: x[0])

    # 转换数据：从数据库的UTC时间转换为UTC+8时区，再生成时间戳
    transformed_data = []

    # 双指针优化变量
    transfer_idx = 0
    current_net_deposits = 0.0
    total_transfers = len(sorted_transfers)

    # 以当前时间区间首点为基准，仅剔除区间内出入金影响，避免历史入金导致净值整体偏移
    first_utc_dt = datetime.fromisoformat(history_data[0]['timestamp']).replace(tzinfo=timezone.utc)
    while transfer_idx < total_transfers and sorted_transfers[transfer_idx][0] <= first_utc_dt:
        current_net_deposits += sorted_transfers[transfer_idx][1]
        transfer_idx += 1
    baseline_net_deposits = current_net_deposits

    # 降采样准备 (简单的间隔采样)
    total_points = len(history_data)
    target_points = 1000  # 限制最大返回点数，避免前端卡顿
    step = 1
    if total_points > target_points:
        step = total_points // target_points

    for i, item in enumerate(history_data):
        # 降采样：跳过非关键点 (保留第一个和最后一个点)
        if i % step != 0 and i != total_points - 1:
            continue

        utc_dt_naive = datetime.fromisoformat(item['timestamp'])
        utc_dt_aware = utc_dt_naive.replace(tzinfo=timezone.utc)
        utc8_dt = utc_dt_aware.astimezone(UTC8)
        current_ts = int(utc8_dt.timestamp() * 1000)
        point_transfer_amount = 0.0
        point_transfer_count = 0

        # 计算区间累计净值 (Cumulative Equity) - 优化后的 O(N) 算法
        # 推进 transfer 指针，直到超过当前余额记录的时间
        while transfer_idx < total_transfers and sorted_transfers[transfer_idx][0] <= utc_dt_aware:
            transfer_amount = sorted_transfers[transfer_idx][1]
            current_net_deposits += transfer_amount
            point_transfer_amount += transfer_amount
            point_transfer_count += 1
            transfer_idx += 1

        net_transfer_in_range = current_net_deposits - baseline_net_deposits
        cumulative_val = item['balance'] - net_transfer_in_range

        # 5. 生成前端需要的时间戳（毫秒）
        transformed_data.append({
            "time": current_ts,
            "value": item['balance'],
            "cumulative_equity": cumulative_val,
            "transfer_amount": point_transfer_amount if point_transfer_count > 0 else None,
            "transfer_count": point_transfer_count if point_transfer_count > 0 else None
        })

    return transformed_data
