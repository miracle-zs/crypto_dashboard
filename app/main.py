from fastapi import FastAPI, Request, HTTPException, Query, Depends
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from app.services import TradeQueryService
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
import asyncio
from functools import partial

# Load environment variables
load_dotenv()

# 定义UTC+8时区
UTC8 = timezone(timedelta(hours=8))

app = FastAPI(title="Zero Gravity Dashboard")

# Setup templates
templates = Jinja2Templates(directory="templates")

# Initialize database
# db = Database()  <-- Deprecated global

# Public REST client (market data)
# public_rest = BinanceFuturesRestClient() <-- Deprecated global

# Scheduler instance
scheduler = None
user_stream = None


def get_db():
    return Database()


def get_trade_service():
    return TradeQueryService()


def get_public_rest():
    return BinanceFuturesRestClient()


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


def _parse_local_snapshot_time(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC8)
    except Exception:
        return None


def _is_noon_review_ready(
    noon_snapshot: Optional[Dict],
    noon_review_snapshot: Optional[Dict]
) -> bool:
    if not noon_review_snapshot:
        return False
    review_dt = _parse_local_snapshot_time(str(noon_review_snapshot.get("review_time") or ""))
    if review_dt is None:
        return False
    noon_dt = _parse_local_snapshot_time(str((noon_snapshot or {}).get("snapshot_time") or ""))
    if noon_dt is None:
        # 没有午间快照时，视作未完成有效复盘，避免凌晨空复盘误导前端状态。
        return False
    return review_dt >= noon_dt


def _fetch_mark_price_map(symbols: List[str], client: BinanceFuturesRestClient) -> Dict[str, float]:
    """
    同步辅助函数，包含阻塞网络IO，需在 executor 中运行
    """
    if not symbols:
        return {}

    unique_symbols = sorted(set(symbols))

    try:
        data = client.public_get("/fapi/v1/premiumIndex")
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
        data = client.public_get("/fapi/v1/ticker/price")
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
        # scheduler.start() 通常是非阻塞的，或者已内部处理线程
        scheduler.start()
        logger.info("定时任务调度器已启动")

        enable_user_stream = os.getenv("ENABLE_USER_STREAM", "0").lower() in ("1", "true", "yes")
        if enable_user_stream:
            # UserDataStream 内部可能涉及网络请求，但在 startup 中初始化通常没问题
            # 如果 start() 包含阻塞循环，应该在线程中运行，这里假设它是异步启动或在新线程启动
            user_stream = BinanceUserDataStream(api_key=api_key, db=get_db())
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


@app.get("/leaderboard", response_class=HTMLResponse)
async def read_leaderboard_page(request: Request):
    """Serve the leaderboard HTML"""
    return templates.TemplateResponse("leaderboard.html", {"request": request})


@app.get("/api/logs")
async def get_logs(lines: int = Query(200, description="Number of log lines to return")):
    """获取最近的日志"""
    # read_logs 是文件IO，量大时可能阻塞，放入 executor
    loop = asyncio.get_event_loop()
    log_lines = await loop.run_in_executor(None, read_logs, lines)
    return {"logs": log_lines}


@app.get("/api/leaderboard")
async def get_leaderboard_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db: Database = Depends(get_db)
):
    """获取涨幅榜历史快照（默认返回最新一条，不进行实时计算）"""
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
        snapshot = await loop.run_in_executor(None, db.get_leaderboard_snapshot_by_date, date)
    else:
        snapshot = await loop.run_in_executor(None, db.get_latest_leaderboard_snapshot)

    if not snapshot:
        return {
            "ok": False,
            "reason": "no_snapshot",
            "message": "暂无快照数据，请等待下一次07:40定时任务生成"
        }

    # 1) 已持仓标记
    open_positions = await loop.run_in_executor(None, db.get_open_positions)
    held_symbols = set()
    for pos in open_positions:
        sym = str(pos.get("symbol", "")).upper().strip()
        if not sym:
            continue
        held_symbols.add(sym)
        held_symbols.add(_normalize_symbol(sym))

    # 2) 与昨日排名对比
    try:
        snap_date = datetime.strptime(snapshot["snapshot_date"], "%Y-%m-%d").date()
    except Exception:
        snap_date = None

    yesterday_snapshot = None
    if snap_date is not None:
        yesterday = (snap_date - timedelta(days=1)).strftime("%Y-%m-%d")
        yesterday_snapshot = await loop.run_in_executor(
            None, db.get_leaderboard_snapshot_by_date, yesterday
        )
    yesterday_rank = {}
    yesterday_losers_rank = {}
    if yesterday_snapshot:
        for idx, row in enumerate(yesterday_snapshot.get("rows", []), start=1):
            symbol = str(row.get("symbol", "")).upper()
            if symbol:
                yesterday_rank[symbol] = idx
        for idx, row in enumerate(yesterday_snapshot.get("losers_rows", []), start=1):
            symbol = str(row.get("symbol", "")).upper()
            if symbol:
                yesterday_losers_rank[symbol] = idx

    # 3) 近7天出现次数（以当前快照日期为截止）
    appearances_7d = {}
    losers_appearances_7d = {}
    if snap_date is not None:
        start_date = (snap_date - timedelta(days=6)).strftime("%Y-%m-%d")
        end_date = snap_date.strftime("%Y-%m-%d")
        snapshots_7d = await loop.run_in_executor(
            None, db.get_leaderboard_snapshots_between, start_date, end_date
        )
        for snap in snapshots_7d:
            seen = set()
            for row in snap.get("rows", []):
                symbol = str(row.get("symbol", "")).upper()
                if not symbol or symbol in seen:
                    continue
                seen.add(symbol)
                appearances_7d[symbol] = appearances_7d.get(symbol, 0) + 1
            seen_losers = set()
            for row in snap.get("losers_rows", []):
                symbol = str(row.get("symbol", "")).upper()
                if not symbol or symbol in seen_losers:
                    continue
                seen_losers.add(symbol)
                losers_appearances_7d[symbol] = losers_appearances_7d.get(symbol, 0) + 1

    enriched_rows = []
    for idx, row in enumerate(snapshot.get("rows", []), start=1):
        symbol = str(row.get("symbol", "")).upper()
        prev_rank = yesterday_rank.get(symbol)
        rank_delta = None if prev_rank is None else (prev_rank - idx)
        enriched_rows.append({
            **row,
            "is_held": symbol in held_symbols,
            "rank_delta_vs_yesterday": rank_delta,
            "appearances_7d": appearances_7d.get(symbol, 0),
        })

    enriched_losers_rows = []
    for idx, row in enumerate(snapshot.get("losers_rows", []), start=1):
        symbol = str(row.get("symbol", "")).upper()
        prev_rank = yesterday_losers_rank.get(symbol)
        rank_delta = None if prev_rank is None else (prev_rank - idx)
        prev_gainer_rank = yesterday_rank.get(symbol)
        was_prev_gainer_top = prev_gainer_rank is not None
        enriched_losers_rows.append({
            **row,
            "is_held": symbol in held_symbols,
            "rank_delta_vs_yesterday": rank_delta,
            "appearances_7d": losers_appearances_7d.get(symbol, 0),
            "was_prev_gainer_top": was_prev_gainer_top,
            "prev_gainer_rank": prev_gainer_rank,
        })

    snapshot["rows"] = enriched_rows
    snapshot["losers_rows"] = enriched_losers_rows
    # 语义更清晰的别名字段（兼容保留旧字段）
    snapshot["gainers_top_rows"] = enriched_rows
    snapshot["gainers_top_count"] = len(enriched_rows)
    snapshot["losers_top_count"] = len(enriched_losers_rows)
    metric_payload = await loop.run_in_executor(
        None, db.get_leaderboard_daily_metrics, str(snapshot.get("snapshot_date"))
    )
    if not metric_payload:
        metric_payload = await loop.run_in_executor(
            None, db.upsert_leaderboard_daily_metrics_for_date, str(snapshot.get("snapshot_date"))
        )

    metric1 = metric_payload.get("metric1", {}) if metric_payload else {}
    metric2 = metric_payload.get("metric2", {}) if metric_payload else {}
    metric3 = metric_payload.get("metric3", {}) if metric_payload else {}
    snapshot["losers_reversal"] = metric1
    snapshot["next_day_drop_metric"] = metric2
    snapshot["change_48h_metric"] = metric3
    # Backward-compatible keys retained for existing frontend readers.
    snapshot["short_48h_metric"] = metric3
    snapshot["hold_48h_metric"] = metric3
    snapshot.pop("all_rows", None)
    return {"ok": True, **snapshot}


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


@app.get("/api/noon-loss-review-history")
async def get_noon_loss_review_history(
    limit: int = Query(7, ge=1, le=90),
    db: Database = Depends(get_db)
):
    """返回午间止损与夜间复盘历史对比（按日期倒序）。"""
    loop = asyncio.get_event_loop()
    rows = await loop.run_in_executor(None, partial(db.list_noon_loss_review_history, limit))
    return {"rows": rows}


@app.get("/api/summary", response_model=TradeSummary)
async def get_summary(service: TradeQueryService = Depends(get_trade_service)):
    """Get calculated trading metrics and equity curve"""
    # Service 层通常调用 DB，视为阻塞
    loop = asyncio.get_event_loop()
    summary = await loop.run_in_executor(None, service.get_summary)
    return summary


@app.get("/api/trades", response_model=List[Trade])
async def get_trades(service: TradeQueryService = Depends(get_trade_service)):
    """Get list of individual trades"""
    # Service 层通常调用 DB，视为阻塞
    loop = asyncio.get_event_loop()
    trades = await loop.run_in_executor(None, service.get_trades_list)
    return trades


@app.get("/api/open-positions", response_model=OpenPositionsResponse)
async def get_open_positions(
    db: Database = Depends(get_db),
    client: BinanceFuturesRestClient = Depends(get_public_rest)
):
    """Get open positions with unrealized PnL and concentration metrics"""
    loop = asyncio.get_event_loop()
    profit_alert_threshold_pct = float(os.getenv("PROFIT_ALERT_THRESHOLD_PCT", "20") or 20)
    now = datetime.now(UTC8)
    today_snapshot_date = now.strftime("%Y-%m-%d")

    # 数据库查询放入 executor
    raw_positions, noon_loss_snapshot, noon_review_snapshot = await asyncio.gather(
        loop.run_in_executor(None, db.get_open_positions),
        loop.run_in_executor(None, partial(db.get_noon_loss_snapshot_by_date, today_snapshot_date)),
        loop.run_in_executor(None, partial(db.get_noon_loss_review_snapshot_by_date, today_snapshot_date))
    )
    noon_loss_count = int(noon_loss_snapshot.get("loss_count", 0)) if noon_loss_snapshot else 0
    noon_stop_loss_total = float(noon_loss_snapshot.get("total_stop_loss", 0.0)) if noon_loss_snapshot else 0.0
    noon_stop_loss_pct = float(noon_loss_snapshot.get("pct_of_balance", 0.0)) if noon_loss_snapshot else 0.0
    noon_snapshot_time = str(noon_loss_snapshot.get("snapshot_time")) if noon_loss_snapshot else None
    noon_review_ready = _is_noon_review_ready(noon_loss_snapshot, noon_review_snapshot)
    noon_review_time = (
        str(noon_review_snapshot.get("review_time")) if (noon_review_snapshot and noon_review_ready) else None
    )
    noon_review_not_cut_count = (
        int(noon_review_snapshot.get("not_cut_count", 0)) if (noon_review_snapshot and noon_review_ready) else 0
    )
    noon_review_noon_cut_loss_total = (
        float(noon_review_snapshot.get("noon_cut_loss_total", 0.0))
        if (noon_review_snapshot and noon_review_ready) else 0.0
    )
    noon_review_hold_loss_total = (
        float(noon_review_snapshot.get("hold_loss_total", 0.0))
        if (noon_review_snapshot and noon_review_ready) else 0.0
    )
    noon_review_delta_loss_total = (
        float(noon_review_snapshot.get("delta_loss_total", 0.0))
        if (noon_review_snapshot and noon_review_ready) else 0.0
    )
    noon_review_pct_of_balance = (
        float(noon_review_snapshot.get("pct_of_balance", 0.0))
        if (noon_review_snapshot and noon_review_ready) else 0.0
    )

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
                "concentration_hhi": 0.0,
                "recent_loss_count": noon_loss_count,
                "recent_loss_total_if_stop_now": noon_stop_loss_total,
                "recent_loss_pct_of_balance": noon_stop_loss_pct,
                "recent_loss_snapshot_date": today_snapshot_date if noon_loss_snapshot else None,
                "recent_loss_snapshot_time": noon_snapshot_time,
                "recent_loss_snapshot_ready": noon_loss_snapshot is not None,
                "noon_review_snapshot_date": today_snapshot_date if noon_review_ready else None,
                "noon_review_snapshot_time": noon_review_time,
                "noon_review_snapshot_ready": noon_review_ready,
                "noon_review_not_cut_count": noon_review_not_cut_count,
                "noon_review_noon_cut_loss_total": noon_review_noon_cut_loss_total,
                "noon_review_hold_loss_total": noon_review_hold_loss_total,
                "noon_review_delta_loss_total": noon_review_delta_loss_total,
                "noon_review_pct_of_balance": noon_review_pct_of_balance,
                "profit_alert_threshold_pct": profit_alert_threshold_pct
            }
        }

    symbols_full = [_normalize_symbol(pos["symbol"]) for pos in raw_positions]

    # 网络请求放入 executor
    mark_prices = await loop.run_in_executor(
        None,
        _fetch_mark_price_map,
        symbols_full,
        client
    )

    positions = []
    per_symbol_notional = defaultdict(float)
    total_notional = 0.0
    long_notional = 0.0
    short_notional = 0.0
    total_unrealized_pnl = 0.0
    total_holding_minutes = 0

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
            "profit_alerted": pos.get("profit_alerted", 0) == 1,
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
        "recent_loss_count": noon_loss_count,
        "recent_loss_total_if_stop_now": noon_stop_loss_total,
        "recent_loss_pct_of_balance": noon_stop_loss_pct,
        "recent_loss_snapshot_date": today_snapshot_date if noon_loss_snapshot else None,
        "recent_loss_snapshot_time": noon_snapshot_time,
        "recent_loss_snapshot_ready": noon_loss_snapshot is not None,
        "noon_review_snapshot_date": today_snapshot_date if noon_review_ready else None,
        "noon_review_snapshot_time": noon_review_time,
        "noon_review_snapshot_ready": noon_review_ready,
        "noon_review_not_cut_count": noon_review_not_cut_count,
        "noon_review_noon_cut_loss_total": noon_review_noon_cut_loss_total,
        "noon_review_hold_loss_total": noon_review_hold_loss_total,
        "noon_review_delta_loss_total": noon_review_delta_loss_total,
        "noon_review_pct_of_balance": noon_review_pct_of_balance,
        "profit_alert_threshold_pct": profit_alert_threshold_pct
    }

    return {
        "as_of": now.isoformat(),
        "positions": positions,
        "summary": summary
    }


@app.get("/api/status")
async def get_status(db: Database = Depends(get_db)):
    """Check system status"""
    global scheduler

    loop = asyncio.get_event_loop()

    # 并行执行独立的数据库查询
    stats, sync_status = await asyncio.gather(
        loop.run_in_executor(None, db.get_statistics),
        loop.run_in_executor(None, db.get_sync_status)
    )

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
        # 在后台执行同步 - 这本身就是提交到 scheduler 的线程池，不阻塞主线程
        scheduler.scheduler.add_job(
            func=scheduler.sync_trades_data,
            id='manual_sync',
            replace_existing=True
        )
        return {"message": "手动同步已触发", "status": "started"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"同步失败: {str(e)}")


@app.get("/api/database/stats")
async def get_database_stats(db: Database = Depends(get_db)):
    """获取数据库详细统计信息"""
    loop = asyncio.get_event_loop()

    # 并行执行
    stats, sync_status = await asyncio.gather(
        loop.run_in_executor(None, db.get_statistics),
        loop.run_in_executor(None, db.get_sync_status)
    )

    return {
        "statistics": stats,
        "sync_status": sync_status
    }


@app.get("/api/daily-stats", response_model=List[DailyStats])
async def get_daily_stats(db: Database = Depends(get_db)):
    """获取每日交易统计（开单数量、开单金额）"""
    loop = asyncio.get_event_loop()
    daily_stats = await loop.run_in_executor(None, db.get_daily_stats)
    return daily_stats


@app.get("/api/monthly-progress")
async def get_monthly_progress(db: Database = Depends(get_db)):
    """获取本月目标进度"""
    loop = asyncio.get_event_loop()

    # 串行执行（或 gather 并行）
    target = await loop.run_in_executor(None, db.get_monthly_target)
    current_pnl = await loop.run_in_executor(None, db.get_monthly_pnl)

    progress = (current_pnl / target * 100) if target > 0 else 0

    return {
        "target": target,
        "current": current_pnl,
        "progress": round(progress, 1)
    }


@app.post("/api/monthly-target")
async def set_monthly_target(
    target: float = Query(..., description="Monthly target amount"),
    db: Database = Depends(get_db)
):
    """设置月度目标"""
    if target <= 0:
        raise HTTPException(status_code=400, detail="目标金额必须大于0")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, db.set_monthly_target, target)
    return {"message": "目标已更新", "target": target}


@app.post("/api/positions/set-long-term")
async def set_long_term(
    symbol: str,
    order_id: int,
    is_long_term: bool,
    db: Database = Depends(get_db)
):
    """设置持仓是否为长期持仓"""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, db.set_position_long_term, symbol, order_id, is_long_term)
    return {"message": "状态已更新", "symbol": symbol, "is_long_term": is_long_term}


@app.get("/api/watch-notes")
async def get_watch_notes(
    limit: int = Query(200, description="最大返回记录数"),
    db: Database = Depends(get_db)
):
    """获取明日观察列表"""
    loop = asyncio.get_event_loop()
    items = await loop.run_in_executor(None, db.get_watch_notes, limit)
    return {"items": items}


@app.post("/api/watch-notes")
async def create_watch_note(
    symbol: str = Query(..., description="观察币种"),
    db: Database = Depends(get_db)
):
    """新增明日观察记录（时间自动写入）"""
    normalized_symbol = (symbol or "").strip().upper()
    if not normalized_symbol:
        raise HTTPException(status_code=400, detail="symbol 不能为空")

    loop = asyncio.get_event_loop()
    try:
        item = await loop.run_in_executor(None, db.add_watch_note, normalized_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    exists_today = bool(item.get("exists_today"))
    return {
        "message": "已存在" if exists_today else "已记录",
        "item": item,
        "exists_today": exists_today
    }


@app.delete("/api/watch-notes/{note_id}")
async def remove_watch_note(
    note_id: int,
    db: Database = Depends(get_db)
):
    """删除明日观察记录"""
    loop = asyncio.get_event_loop()
    deleted = await loop.run_in_executor(None, db.delete_watch_note, note_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"message": "已删除", "id": note_id}
