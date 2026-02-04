from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from app.services import BinanceOrderAnalyzer
from app.models import Trade, TradeSummary, BalanceHistoryItem
from app.scheduler import get_scheduler
from app.database import Database
import os
from dotenv import load_dotenv
from typing import List, Optional
from datetime import datetime, timedelta, timezone

# Load environment variables
load_dotenv()

# 定义UTC+8时区
UTC8 = timezone(timedelta(hours=8))

app = FastAPI(title="Zero Gravity Dashboard")

# Setup templates
templates = Jinja2Templates(directory="templates")

# Initialize Analyzer (now reads from database)
analyzer = BinanceOrderAnalyzer()

# Initialize database
db = Database()

# Scheduler instance
scheduler = None


@app.on_event("startup")
async def startup_event():
    """应用启动时执行"""
    global scheduler

    # 启动定时任务调度器
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')

    if api_key and api_secret:
        scheduler = get_scheduler()
        scheduler.start()
        print("✓ 定时任务调度器已启动")
    else:
        print("⚠ 警告: 未配置API密钥，定时任务未启动")


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时执行"""
    global scheduler
    if scheduler:
        scheduler.stop()
        print("✓ 定时任务调度器已停止")


@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """Serve the dashboard HTML"""
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/live-monitor", response_class=HTMLResponse)
async def read_live_monitor(request: Request):
    """Serve the live monitor HTML"""
    return templates.TemplateResponse("live_monitor.html", {"request": request})


@app.get("/metrics", response_class=HTMLResponse)
async def read_metrics(request: Request):
    """Serve the metrics documentation HTML"""
    return templates.TemplateResponse("metrics.html", {"request": request})


@app.get("/api/balance-history", response_model=List[BalanceHistoryItem])
async def get_balance_history(
    time_range: Optional[str] = Query("1d", description="Time range for balance history (e.g., 1h, 1d, 1w, 1m, 1y)")
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
        start_time = end_time - timedelta(days=30)  # Approximately 1 month
    elif time_range == "1y":
        start_time = end_time - timedelta(days=365)  # Approximately 1 year
    else:
        # Default or invalid time_range, fetch last 2 hours
        start_time = end_time - timedelta(hours=2)

    history_data = db.get_balance_history(start_time=start_time, end_time=end_time)

    # 获取出入金记录
    transfers = db.get_transfers()

    # 转换数据：从数据库的UTC时间转换为UTC+8时区，再生成时间戳
    transformed_data = []
    for item in history_data:
        # 1. 解析来自数据库的ISO格式时间字符串（我们知道它是UTC）
        utc_dt_naive = datetime.fromisoformat(item['timestamp'])

        # 2. 将其设置为UTC时区（使其成为一个"aware"的datetime对象）
        utc_dt_aware = utc_dt_naive.replace(tzinfo=timezone.utc)

        # 3. 转换为UTC+8时区
        utc8_dt = utc_dt_aware.astimezone(UTC8)
        current_ts = int(utc8_dt.timestamp() * 1000)

        # 4. 计算累计净值 (Cumulative Equity)
        # 逻辑：Cumulative = Actual Balance - Net Deposits
        # 比如：余额 14000，之前提了 10000 (Net Deposit = -10000)
        # Cumulative = 14000 - (-10000) = 24000

        net_deposits = 0.0
        for t in transfers:
            # 只统计在这个时间点之前的转账
            # 解析transfer的时间
            t_dt = datetime.fromisoformat(t['timestamp']).replace(tzinfo=timezone.utc)
            if t_dt <= utc_dt_aware:
                net_deposits += t['amount']

        cumulative_val = item['balance'] - net_deposits

        # 5. 生成前端需要的时间戳（毫秒）
        transformed_data.append({
            "time": current_ts,
            "value": item['balance'],
            "cumulative_equity": cumulative_val
        })

    return transformed_data


@app.get("/api/summary", response_model=TradeSummary)
async def get_summary():
    """Get calculated trading metrics and equity curve"""
    summary = analyzer.get_summary()
    return summary


@app.get("/api/trades", response_model=List[Trade])
async def get_trades():
    """Get list of individual trades"""
    trades = analyzer.get_trades_list()
    return trades


@app.get("/api/status")
async def get_status():
    """Check system status"""
    global scheduler

    # 获取数据库统计
    stats = db.get_statistics()
    sync_status = db.get_sync_status()

    next_run_time = None
    is_configured = scheduler is not None and scheduler.scheduler.running
    if scheduler:
        next_run = scheduler.get_next_run_time()
        next_run_time = next_run.isoformat() if next_run else None

    return {
        "status": "online",
        "configured": is_configured,
        "database": {
            "total_trades": stats.get('total_trades', 0),
            "unique_symbols": stats.get('unique_symbols', 0),
            "earliest_trade": stats.get('earliest_trade'),
            "latest_trade": stats.get('latest_trade')
        },
        "sync": {
            "last_sync_time": sync_status.get('last_sync_time'),
            "status": sync_status.get('status', 'idle'),
            "next_run_time": next_run_time,
            "error_message": sync_status.get('error_message')
        },
        "scheduler_running": is_configured
    }


@app.post("/api/sync/manual")
async def manual_sync():
    """手动触发数据同步"""
    global scheduler

    if not scheduler:
        raise HTTPException(status_code=500, detail="调度器未初始化")

    try:
        # 在后台执行同步
        scheduler.scheduler.add_job(
            func=scheduler.sync_trades_data,
            id='manual_sync',
            replace_existing=True
        )
        return {"message": "手动同步已触发", "status": "started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"同步失败: {str(e)}")


@app.get("/api/database/stats")
async def get_database_stats():
    """获取数据库详细统计信息"""
    stats = db.get_statistics()
    sync_status = db.get_sync_status()

    return {
        "statistics": stats,
        "sync_status": sync_status
    }
