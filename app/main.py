from fastapi import FastAPI, Request, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from app.services import BinanceOrderAnalyzer
from app.models import Trade, TradeSummary, BalanceHistoryItem, DailyStats, OpenPositionsResponse
from app.scheduler import get_scheduler
from app.database import Database
from app.binance_client import BinanceFuturesRestClient
from app.user_stream import BinanceUserDataStream
import os
from dotenv import load_dotenv
from typing import List, Optional, Dict
from datetime import datetime, timedelta, timezone
from app.logger import logger, read_logs
from collections import defaultdict

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

# Public REST client (market data)
public_rest = BinanceFuturesRestClient()

# Scheduler instance
scheduler = None
user_stream = None


def _format_holding_time(total_minutes: int) -> str:
    if total_minutes <= 0:
        return "0m"
    if total_minutes < 60:
        return f"{total_minutes}m"
    hours = total_minutes // 60
    minutes = total_minutes % 60
    if hours < 24:
        return f"{hours}h {minutes}m"
    days = hours // 24
    hours = hours % 24
    return f"{days}d {hours}h"


def _normalize_symbol(symbol: str) -> str:
    return symbol if symbol.endswith("USDT") else f"{symbol}USDT"


def _fetch_mark_price_map(symbols: List[str]) -> Dict[str, float]:
    if not symbols:
        return {}

    unique_symbols = sorted(set(symbols))

    try:
        data = public_rest.public_get("/fapi/v1/premiumIndex")
        if isinstance(data, dict):
            data = [data]
        price_map = {
            item["symbol"]: float(item["markPrice"])
            for item in data
            if isinstance(item, dict) and "symbol" in item and "markPrice" in item
        }
        return {symbol: price_map.get(symbol) for symbol in unique_symbols if symbol in price_map}
    except Exception as exc:
        logger.warning(f"Failed to fetch mark prices via premiumIndex: {exc}")

    try:
        data = public_rest.public_get("/fapi/v1/ticker/price")
        if isinstance(data, dict):
            data = [data]
        price_map = {
            item["symbol"]: float(item["price"])
            for item in data
            if isinstance(item, dict) and "symbol" in item and "price" in item
        }
        return {symbol: price_map.get(symbol) for symbol in unique_symbols if symbol in price_map}
    except Exception as exc:
        logger.warning(f"Failed to fetch mark prices via ticker/price: {exc}")

    return {}


@app.on_event("startup")
async def startup_event():
    """应用启动时执行"""
    global scheduler, user_stream

    # 启动定时任务调度器
    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')

    if api_key and api_secret:
        scheduler = get_scheduler()
        scheduler.start()
        logger.info("定时任务调度器已启动")

        enable_user_stream = os.getenv("ENABLE_USER_STREAM", "0").lower() in ("1", "true", "yes")
        if enable_user_stream:
            user_stream = BinanceUserDataStream(api_key=api_key, db=db)
            user_stream.start()
    else:
        logger.warning("未配置API密钥，定时任务未启动")


@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时执行"""
    global scheduler, user_stream
    if scheduler:
        scheduler.stop()
        logger.info("定时任务调度器已停止")
    if user_stream:
        user_stream.stop()


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


@app.get("/logs", response_class=HTMLResponse)
async def read_logs_page(request: Request):
    """Serve the logs viewer HTML"""
    return templates.TemplateResponse("logs.html", {"request": request})


@app.get("/api/logs")
async def get_logs(lines: int = Query(200, description="Number of log lines to return")):
    """获取最近的日志"""
    log_lines = read_logs(lines)
    return {"logs": log_lines}


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


@app.get("/api/open-positions", response_model=OpenPositionsResponse)
async def get_open_positions():
    """Get open positions with unrealized PnL and concentration metrics"""
    raw_positions = db.get_open_positions()
    now = datetime.now(UTC8)

    if not raw_positions:
        return {
            "as_of": now.isoformat(),
            "positions": [],
            "summary": {
                "total_positions": 0,
                "long_count": 0,
                "short_count": 0,
                "total_notional": 0.0,
                "long_notional": 0.0,
                "short_notional": 0.0,
                "net_exposure": 0.0,
                "total_unrealized_pnl": 0.0,
                "avg_holding_minutes": 0.0,
                "avg_holding_time": "0m",
                "concentration_top1": 0.0,
                "concentration_top3": 0.0,
                "concentration_hhi": 0.0
            }
        }

    symbols_full = [_normalize_symbol(pos["symbol"]) for pos in raw_positions]
    mark_prices = _fetch_mark_price_map(symbols_full)

    positions = []
    per_symbol_notional = defaultdict(float)
    total_notional = 0.0
    long_notional = 0.0
    short_notional = 0.0
    total_unrealized_pnl = 0.0
    total_holding_minutes = 0
    recent_loss_count = 0

    for pos in raw_positions:
        symbol = str(pos.get("symbol", "")).upper()
        side = str(pos.get("side", "")).upper()
        qty = float(pos.get("qty", 0.0))
        entry_price = float(pos.get("entry_price", 0.0))
        entry_amount = float(pos.get("entry_amount") or (entry_price * qty))
        entry_time_str = str(pos.get("entry_time"))
        is_long_term = pos.get("is_long_term", 0) == 1

        symbol_full = _normalize_symbol(symbol)
        mark_price = mark_prices.get(symbol_full)
        price_for_notional = mark_price if mark_price is not None else entry_price
        notional = float(price_for_notional * qty)

        unrealized_pnl = None
        unrealized_pnl_pct = None
        if mark_price is not None and qty > 0:
            if side == "SHORT":
                unrealized_pnl = (entry_price - mark_price) * qty
            else:
                unrealized_pnl = (mark_price - entry_price) * qty
            if entry_amount > 0:
                unrealized_pnl_pct = (unrealized_pnl / entry_amount) * 100

        try:
            entry_dt = datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC8)
        except ValueError:
            entry_dt = now

        holding_minutes = max(0, int((now - entry_dt).total_seconds() // 60))
        holding_time = _format_holding_time(holding_minutes)

        # 检查是否为24h内浮亏
        if not is_long_term and holding_minutes <= 24 * 60:
            if unrealized_pnl is not None and unrealized_pnl < 0:
                recent_loss_count += 1

        total_notional += notional
        total_holding_minutes += holding_minutes
        if side == "SHORT":
            short_notional += notional
        else:
            long_notional += notional

        if unrealized_pnl is not None:
            total_unrealized_pnl += unrealized_pnl

        per_symbol_notional[symbol] += notional

        positions.append({
            "symbol": symbol,
            "order_id": pos.get("order_id"),
            "side": side,
            "qty": qty,
            "entry_price": entry_price,
            "mark_price": mark_price,
            "entry_time": entry_time_str,
            "holding_minutes": holding_minutes,
            "holding_time": holding_time,
            "entry_amount": entry_amount,
            "notional": notional,
            "unrealized_pnl": unrealized_pnl,
            "unrealized_pnl_pct": unrealized_pnl_pct,
            "is_long_term": pos.get("is_long_term", 0) == 1,
            "weight": 0.0
        })

    if total_notional > 0:
        for pos in positions:
            pos["weight"] = pos["notional"] / total_notional

    positions.sort(key=lambda item: item["notional"], reverse=True)

    shares = []
    if total_notional > 0:
        shares = sorted(
            (value / total_notional for value in per_symbol_notional.values()),
            reverse=True
        )

    concentration_top1 = shares[0] if shares else 0.0
    concentration_top3 = sum(shares[:3]) if shares else 0.0
    concentration_hhi = sum(share ** 2 for share in shares) if shares else 0.0

    avg_holding_minutes = (total_holding_minutes / len(positions)) if positions else 0.0
    avg_holding_time = _format_holding_time(int(avg_holding_minutes))

    summary = {
        "total_positions": len(positions),
        "long_count": sum(1 for p in positions if p["side"] == "LONG"),
        "short_count": sum(1 for p in positions if p["side"] == "SHORT"),
        "total_notional": total_notional,
        "long_notional": long_notional,
        "short_notional": short_notional,
        "net_exposure": long_notional - short_notional,
        "total_unrealized_pnl": total_unrealized_pnl,
        "avg_holding_minutes": avg_holding_minutes,
        "avg_holding_time": avg_holding_time,
        "concentration_top1": concentration_top1,
        "concentration_top3": concentration_top3,
        "concentration_hhi": concentration_hhi,
        "recent_loss_count": recent_loss_count
    }

    return {
        "as_of": now.isoformat(),
        "positions": positions,
        "summary": summary
    }


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


@app.get("/api/daily-stats", response_model=List[DailyStats])
async def get_daily_stats():
    """获取每日交易统计（开单数量、开单金额）"""
    daily_stats = db.get_daily_stats()
    return daily_stats




@app.get("/api/monthly-progress")
async def get_monthly_progress():
    """获取本月目标进度"""
    target = db.get_monthly_target()
    current_pnl = db.get_monthly_pnl()
    progress = (current_pnl / target * 100) if target > 0 else 0

    return {
        "target": target,
        "current": current_pnl,
        "progress": round(progress, 1)
    }


@app.post("/api/monthly-target")
async def set_monthly_target(target: float = Query(..., description="Monthly target amount")):
    """设置月度目标"""
    if target <= 0:
        raise HTTPException(status_code=400, detail="目标金额必须大于0")

    db.set_monthly_target(target)
    return {"message": "目标已更新", "target": target}


@app.post("/api/positions/set-long-term")
async def set_long_term(symbol: str, order_id: int, is_long_term: bool):
    """设置持仓是否为长期持仓"""
    db.set_position_long_term(symbol, order_id, is_long_term)
    return {"message": "状态已更新", "symbol": symbol, "is_long_term": is_long_term}
