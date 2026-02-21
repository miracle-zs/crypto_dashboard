import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

from app.logger import logger


def build_top_gainers_snapshot(scheduler, utc8):
    """构建涨跌幅榜快照（不处理锁与冷却）。"""
    stage_started_at = time.perf_counter()
    now_utc = datetime.now(timezone.utc)
    midnight_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_utc_ms = int(midnight_utc.timestamp() * 1000)

    exchange_info = scheduler.processor.get_exchange_info(client=scheduler.processor.client)
    if not exchange_info or "symbols" not in exchange_info:
        raise RuntimeError("无法获取 exchangeInfo")

    usdt_perpetual_symbols = {
        item.get("symbol")
        for item in exchange_info.get("symbols", [])
        if (
            item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
            and str(item.get("status", "")).upper() == "TRADING"
        )
    }
    if not usdt_perpetual_symbols:
        raise RuntimeError("无可用USDT永续交易对")

    ticker_data = scheduler.processor.client.public_get("/fapi/v1/ticker/24hr")
    if not ticker_data or not isinstance(ticker_data, list):
        raise RuntimeError("无法获取 24hr ticker")

    candidates = []
    for item in ticker_data:
        symbol = item.get("symbol")
        if not symbol or symbol not in usdt_perpetual_symbols:
            continue
        try:
            last_price = float(item.get("lastPrice", 0.0))
            quote_volume = float(item.get("quoteVolume", 0.0))
        except (TypeError, ValueError):
            continue

        if last_price <= 0:
            continue
        if quote_volume < scheduler.leaderboard_min_quote_volume:
            continue

        candidates.append({
            "symbol": symbol,
            "last_price": last_price,
            "quote_volume": quote_volume,
        })

    candidates.sort(key=lambda x: x["quote_volume"], reverse=True)
    if scheduler.leaderboard_max_symbols > 0:
        candidates = candidates[: scheduler.leaderboard_max_symbols]
    logger.info(
        "晨间涨幅榜候选统计: "
        f"candidates={len(candidates)}, "
        f"min_quote_volume={scheduler.leaderboard_min_quote_volume:.0f}, "
        f"max_symbols={scheduler.leaderboard_max_symbols}"
    )

    leaderboard = []
    progress_step = 20
    total_candidates = len(candidates)
    if total_candidates > 0:
        min_interval = max(0.05, float(os.getenv("BINANCE_MIN_REQUEST_INTERVAL", "0.3")))
        per_worker_rpm = max(1.0, 60.0 / min_interval)
        workers_by_budget = max(1, int(scheduler.leaderboard_weight_budget_per_minute // per_worker_rpm))
        worker_count = min(total_candidates, scheduler.leaderboard_kline_workers, workers_by_budget)
        estimated_peak_weight_per_min = int(worker_count * per_worker_rpm)
        estimated_total_weight = 1 + 40 + total_candidates
        logger.info(
            "晨间涨幅榜并发计划: "
            f"workers={worker_count}, "
            f"min_interval={min_interval:.2f}s, "
            f"budget={scheduler.leaderboard_weight_budget_per_minute}/min, "
            f"est_peak={estimated_peak_weight_per_min}/min, "
            f"est_total_weight={estimated_total_weight}"
        )

        thread_local = threading.local()

        def _kline_task(item: dict):
            if scheduler._is_api_cooldown_active(source="涨幅榜-逐币种计算"):
                return item, None
            worker_client = getattr(thread_local, "client", None)
            if worker_client is None:
                worker_client = scheduler.processor._create_worker_client()
                thread_local.client = worker_client
            open_price = scheduler.processor.get_price_change_from_utc_start(
                symbol=item["symbol"],
                timestamp=midnight_utc_ms,
                client=worker_client,
            )
            return item, open_price

        processed = 0
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(_kline_task, item) for item in candidates]
            for future in as_completed(futures):
                processed += 1
                try:
                    item, open_price = future.result()
                except Exception as exc:
                    logger.warning(f"涨幅榜逐币种计算异常: {exc}")
                    if processed % progress_step == 0 or processed == total_candidates:
                        logger.info(
                            "晨间涨幅榜进度: "
                            f"{processed}/{total_candidates}, "
                            f"effective={len(leaderboard)}, "
                            f"elapsed={time.perf_counter() - stage_started_at:.1f}s"
                        )
                    continue

                if open_price is not None and open_price > 0:
                    pct_change = (item["last_price"] / open_price - 1) * 100
                    leaderboard.append(
                        {
                            "symbol": item["symbol"],
                            "change": pct_change,
                            "volume": item["quote_volume"],
                            "last_price": item["last_price"],
                        }
                    )

                if processed % progress_step == 0 or processed == total_candidates:
                    logger.info(
                        "晨间涨幅榜进度: "
                        f"{processed}/{total_candidates}, "
                        f"effective={len(leaderboard)}, "
                        f"elapsed={time.perf_counter() - stage_started_at:.1f}s"
                    )

    leaderboard.sort(key=lambda x: x["change"], reverse=True)
    top_list = leaderboard[: scheduler.leaderboard_top_n]
    losers_list = sorted(leaderboard, key=lambda x: x["change"])[: scheduler.leaderboard_top_n]

    snapshot = {
        "snapshot_date": datetime.now(utc8).strftime("%Y-%m-%d"),
        "snapshot_time": datetime.now(utc8).strftime("%Y-%m-%d %H:%M:%S"),
        "window_start_utc": midnight_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "candidates": len(candidates),
        "effective": len(leaderboard),
        "top": len(top_list),
        "rows": top_list,
        "losers_rows": losers_list,
        "all_rows": leaderboard,
    }
    logger.info(
        "晨间涨幅榜快照构建完成: "
        f"candidates={snapshot['candidates']}, "
        f"effective={snapshot['effective']}, "
        f"top={snapshot['top']}, "
        f"elapsed={time.perf_counter() - stage_started_at:.1f}s"
    )
    return snapshot


def build_rebound_snapshot(scheduler, *, utc8, window_days: int, top_n: int, kline_workers: int, weight_budget_per_minute: int, label: str):
    """构建反弹幅度榜快照（不处理锁与冷却）。"""
    stage_started_at = time.perf_counter()
    now_utc = datetime.now(timezone.utc)
    window_start_utc = now_utc - timedelta(days=window_days)

    exchange_info = scheduler.processor.get_exchange_info(client=scheduler.processor.client)
    if not exchange_info or "symbols" not in exchange_info:
        raise RuntimeError("无法获取 exchangeInfo")

    usdt_perpetual_symbols = {
        item.get("symbol")
        for item in exchange_info.get("symbols", [])
        if (
            item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
            and str(item.get("status", "")).upper() == "TRADING"
        )
    }
    if not usdt_perpetual_symbols:
        raise RuntimeError("无可用USDT永续交易对")

    ticker_data = scheduler.processor.client.public_get("/fapi/v1/ticker/price")
    if not ticker_data:
        raise RuntimeError("无法获取 ticker/price")
    if isinstance(ticker_data, dict):
        ticker_data = [ticker_data]

    candidates = []
    for item in ticker_data:
        symbol = item.get("symbol")
        if not symbol or symbol not in usdt_perpetual_symbols:
            continue
        try:
            current_price = float(item.get("price", 0.0))
        except (TypeError, ValueError):
            continue
        if current_price <= 0:
            continue
        candidates.append({"symbol": symbol, "current_price": current_price})

    candidates.sort(key=lambda x: x["symbol"])
    logger.info(f"{label}候选统计: candidates={len(candidates)}, top_n={top_n}")

    rebound_rows = []
    progress_step = 20
    total_candidates = len(candidates)
    if total_candidates > 0:
        min_interval = max(0.05, float(os.getenv("BINANCE_MIN_REQUEST_INTERVAL", "0.3")))
        per_worker_rpm = max(1.0, 60.0 / min_interval)
        workers_by_budget = max(1, int(weight_budget_per_minute // per_worker_rpm))
        worker_count = min(total_candidates, kline_workers, workers_by_budget)
        estimated_peak_weight_per_min = int(worker_count * per_worker_rpm)
        estimated_total_weight = 1 + 1 + total_candidates
        logger.info(
            f"{label}并发计划: "
            f"workers={worker_count}, "
            f"min_interval={min_interval:.2f}s, "
            f"budget={weight_budget_per_minute}/min, "
            f"est_peak={estimated_peak_weight_per_min}/min, "
            f"est_total_weight={estimated_total_weight}"
        )

        thread_local = threading.local()
        metric_field = f"rebound_{window_days}d_pct"
        low_field = f"low_{window_days}d"
        low_time_field = f"low_{window_days}d_at_utc"
        kline_limit = max(14, int(window_days))

        def _kline_task(item: dict):
            if scheduler._is_api_cooldown_active(source=f"{label}-逐币种计算"):
                return item, None

            worker_client = getattr(thread_local, "client", None)
            if worker_client is None:
                worker_client = scheduler.processor._create_worker_client()
                thread_local.client = worker_client

            try:
                klines = worker_client.public_get(
                    "/fapi/v1/klines",
                    {"symbol": item["symbol"], "interval": "1d", "limit": kline_limit},
                ) or []
            except Exception:
                return item, None

            lows = []
            for kline in klines:
                if not isinstance(kline, list) or len(kline) < 4:
                    continue
                try:
                    low_price = float(kline[3])
                    open_time = int(kline[0])
                except (TypeError, ValueError):
                    continue
                if low_price <= 0:
                    continue
                lows.append((low_price, open_time))

            if not lows:
                return item, None

            low_price, low_ts = min(lows, key=lambda entry: entry[0])
            rebound_pct = (item["current_price"] / low_price - 1.0) * 100.0
            low_at_utc = datetime.fromtimestamp(low_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            return item, {low_field: low_price, low_time_field: low_at_utc, metric_field: rebound_pct}

        processed = 0
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(_kline_task, item) for item in candidates]
            for future in as_completed(futures):
                processed += 1
                try:
                    item, payload = future.result()
                except Exception as exc:
                    logger.warning(f"{label}逐币种计算异常: {exc}")
                    payload = None
                    item = None

                if item and payload:
                    rebound_rows.append(
                        {
                            "symbol": item["symbol"],
                            "current_price": item["current_price"],
                            low_field: payload[low_field],
                            low_time_field: payload[low_time_field],
                            metric_field: payload[metric_field],
                        }
                    )

                if processed % progress_step == 0 or processed == total_candidates:
                    logger.info(
                        f"{label}进度: "
                        f"{processed}/{total_candidates}, "
                        f"effective={len(rebound_rows)}, "
                        f"elapsed={time.perf_counter() - stage_started_at:.1f}s"
                    )

    metric_field = f"rebound_{window_days}d_pct"
    rebound_rows.sort(key=lambda x: x[metric_field], reverse=True)
    top_list = rebound_rows[:top_n]

    snapshot = {
        "snapshot_date": datetime.now(utc8).strftime("%Y-%m-%d"),
        "snapshot_time": datetime.now(utc8).strftime("%Y-%m-%d %H:%M:%S"),
        "window_start_utc": window_start_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "candidates": len(candidates),
        "effective": len(rebound_rows),
        "top": len(top_list),
        "rows": top_list,
        "all_rows": rebound_rows,
    }
    logger.info(
        f"{label}快照构建完成: "
        f"candidates={snapshot['candidates']}, "
        f"effective={snapshot['effective']}, "
        f"top={snapshot['top']}, "
        f"elapsed={time.perf_counter() - stage_started_at:.1f}s"
    )
    return snapshot
