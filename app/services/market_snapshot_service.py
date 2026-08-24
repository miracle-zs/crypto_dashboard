import os
import threading
import time
from datetime import datetime, timedelta, timezone

from app.core.binance_request_budget import (
    BulkRequestAborted,
    RollingWeightLimiter,
    WeightedRequestSession,
    binance_request_weight,
    calculate_request_weight,
)
from app.logger import logger


DAY_MS = 86_400_000
DEFAULT_REBOUND_WINDOWS = (14, 30, 60, 365)

_EXCHANGE_SYMBOLS_CACHE = {"symbols": None, "expires_at": 0.0}
_EXCHANGE_SYMBOLS_LOCK = threading.Lock()


def _resolve_exchange_symbols_cache_ttl() -> float:
    raw = os.getenv("EXCHANGE_INFO_CACHE_TTL_SECONDS", "300")
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return 300.0
    return max(0.0, value)


def _get_usdt_perpetual_symbol_meta(scheduler, *, client=None):
    now = time.time()
    with _EXCHANGE_SYMBOLS_LOCK:
        cached_symbols = _EXCHANGE_SYMBOLS_CACHE.get("symbols")
        expires_at = float(_EXCHANGE_SYMBOLS_CACHE.get("expires_at", 0.0) or 0.0)
        if cached_symbols and now < expires_at:
            return dict(cached_symbols)

    exchange_client = client or scheduler.processor.client
    exchange_info = scheduler.processor.get_exchange_info(client=exchange_client)
    if not exchange_info or "symbols" not in exchange_info:
        raise RuntimeError("无法获取 exchangeInfo")

    symbols = {}
    for item in exchange_info.get("symbols", []):
        symbol = item.get("symbol")
        if (
            not symbol
            or item.get("contractType") != "PERPETUAL"
            or item.get("quoteAsset") != "USDT"
            or str(item.get("status", "")).upper() != "TRADING"
        ):
            continue
        symbols[str(symbol)] = {"onboard_date": item.get("onboardDate")}
    if not symbols:
        raise RuntimeError("无可用USDT永续交易对")

    ttl_seconds = _resolve_exchange_symbols_cache_ttl()
    with _EXCHANGE_SYMBOLS_LOCK:
        _EXCHANGE_SYMBOLS_CACHE["symbols"] = dict(symbols)
        _EXCHANGE_SYMBOLS_CACHE["expires_at"] = now + ttl_seconds
    return symbols


def _get_usdt_perpetual_symbols(scheduler):
    return set(_get_usdt_perpetual_symbol_meta(scheduler).keys())


def _is_listing_daily_candle(open_time_ms: int, onboard_date_ms) -> bool:
    try:
        onboard_ts = int(onboard_date_ms)
    except (TypeError, ValueError):
        return False
    return open_time_ms <= onboard_ts < open_time_ms + DAY_MS


def _filter_listing_daily_candle(klines, onboard_date_ms):
    valid_rows = [kline for kline in klines if isinstance(kline, list) and len(kline) >= 4]
    if not valid_rows:
        return []
    try:
        first_open_time_ms = int(valid_rows[0][0])
    except (TypeError, ValueError):
        return valid_rows
    if _is_listing_daily_candle(first_open_time_ms, onboard_date_ms):
        return valid_rows[1:]
    return valid_rows


def _extract_highs_from_klines(klines):
    highs = []
    for kline in klines:
        value = kline.get("high") if isinstance(kline, dict) else kline[2] if len(kline) >= 3 else None
        try:
            high_price = float(value)
        except (TypeError, ValueError):
            continue
        if high_price > 0:
            highs.append(high_price)
    return highs


def _calc_drawdown_from_high(current_price: float, high_price: float):
    if current_price is None or current_price <= 0:
        return None
    if high_price is None or high_price <= 0:
        return None
    return min(0.0, (current_price / high_price - 1.0) * 100.0)


def _build_drawdown_fields(*, current_price: float, highs_7d, highs_window):
    recent_7d_high = max(highs_7d) if highs_7d else None
    window_high = max(highs_window) if highs_window else None
    return {
        "drawdown_from_7d_high_pct": _calc_drawdown_from_high(current_price, recent_7d_high),
        "drawdown_from_window_high_pct": _calc_drawdown_from_high(current_price, window_high),
    }


def _normalize_daily_rows(symbol: str, raw_klines, *, now_ms: int) -> list[dict]:
    rows = []
    for kline in raw_klines or []:
        if not isinstance(kline, list) or len(kline) < 5:
            continue
        try:
            open_time = int(kline[0])
            close_time = int(kline[6]) if len(kline) > 6 else open_time + DAY_MS - 1
            rows.append(
                {
                    "symbol": str(symbol).upper(),
                    "open_time": open_time,
                    "open": float(kline[1]),
                    "high": float(kline[2]),
                    "low": float(kline[3]),
                    "close": float(kline[4]),
                    "is_closed": close_time < int(now_ms),
                }
            )
        except (TypeError, ValueError):
            continue
    return rows


def update_daily_kline_cache(
    scheduler,
    *,
    history_days: int = 365,
    max_weight_per_60s: int = 200,
    now_ms: int | None = None,
    limiter: RollingWeightLimiter | None = None,
):
    """Backfill missing symbols once, then upsert only the latest two daily bars."""
    if not getattr(scheduler, "daily_kline_repo", None):
        raise RuntimeError("daily_kline_repo 未配置")
    if not scheduler.processor:
        raise RuntimeError("API密钥未配置")

    started_at = time.perf_counter()
    resolved_now_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    job_limiter = limiter or RollingWeightLimiter(max_weight_per_60s)
    session = WeightedRequestSession(scheduler.processor.client, limiter=job_limiter)
    symbol_meta = _get_usdt_perpetual_symbol_meta(scheduler, client=session)
    symbols = sorted(symbol_meta)
    latest_by_symbol = scheduler.daily_kline_repo.latest_open_times(symbols)

    updated_rows = 0
    backfill_symbols = 0
    incremental_symbols = 0
    failed_symbols = []
    try:
        for index, symbol in enumerate(symbols, start=1):
            latest_open_time = latest_by_symbol.get(symbol)
            if latest_open_time is None:
                params = {
                    "symbol": symbol,
                    "interval": "1d",
                    "limit": max(1, int(history_days)),
                }
                backfill_symbols += 1
            else:
                params = {
                    "symbol": symbol,
                    "interval": "1d",
                    "startTime": int(latest_open_time),
                    "limit": 2,
                }
                incremental_symbols += 1

            raw_klines = session.public_get("/fapi/v1/klines", params)
            if raw_klines is None:
                failed_symbols.append(symbol)
                continue
            filtered = _filter_listing_daily_candle(
                raw_klines,
                symbol_meta.get(symbol, {}).get("onboard_date"),
            )
            normalized = _normalize_daily_rows(symbol, filtered, now_ms=resolved_now_ms)
            updated_rows += scheduler.daily_kline_repo.upsert(normalized)

            if index % 20 == 0 or index == len(symbols):
                logger.info(
                    "日K线缓存进度: "
                    f"{index}/{len(symbols)}, rows={updated_rows}, "
                    f"actual_weight={session.total_weight}"
                )
    except BulkRequestAborted:
        logger.error(
            "日K线缓存任务因 Binance 429/418 立即终止: "
            f"processed={backfill_symbols + incremental_symbols}, actual_weight={session.total_weight}"
        )
        raise

    result = {
        "symbols": len(symbols),
        "backfill_symbols": backfill_symbols,
        "incremental_symbols": incremental_symbols,
        "failed_symbols": failed_symbols,
        "updated_rows": updated_rows,
        "request_count": len(session.requests),
        "request_weight": session.total_weight,
        "peak_limit_per_60s": int(max_weight_per_60s),
        "elapsed_seconds": time.perf_counter() - started_at,
    }
    logger.info(
        "日K线缓存更新完成: "
        f"symbols={result['symbols']}, backfill={backfill_symbols}, "
        f"incremental={incremental_symbols}, rows={updated_rows}, "
        f"requests={result['request_count']}, actual_weight={result['request_weight']}, "
        f"rolling_limit={max_weight_per_60s}/60s"
    )
    return result


def _ticker_last_price(item) -> float | None:
    try:
        price = float(item.get("lastPrice", item.get("price", 0.0)))
    except (TypeError, ValueError):
        return None
    return price if price > 0 else None


def calculate_top_gainers_snapshot(
    *,
    ticker_data,
    daily_klines_by_symbol,
    utc8,
    now_utc: datetime,
    min_quote_volume: float,
    max_symbols: int,
    top_n: int,
):
    """Pure leaderboard calculation over one local market view."""
    midnight_utc = now_utc.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    midnight_utc_ms = int(midnight_utc.timestamp() * 1000)
    candidates = []
    for item in ticker_data or []:
        symbol = str(item.get("symbol") or "")
        rows = daily_klines_by_symbol.get(symbol) or []
        if not symbol or not rows:
            continue
        current_price = _ticker_last_price(item)
        try:
            quote_volume = float(item.get("quoteVolume", 0.0))
        except (TypeError, ValueError):
            continue
        if current_price is None or quote_volume < float(min_quote_volume):
            continue
        candidates.append(
            {
                "symbol": symbol,
                "last_price": current_price,
                "quote_volume": quote_volume,
            }
        )

    candidates.sort(key=lambda item: item["quote_volume"], reverse=True)
    if int(max_symbols) > 0:
        candidates = candidates[: int(max_symbols)]

    leaderboard = []
    for item in candidates:
        rows = daily_klines_by_symbol[item["symbol"]]
        current_candle = next(
            (row for row in reversed(rows) if int(row["open_time"]) == midnight_utc_ms),
            None,
        )
        if current_candle is None:
            continue
        open_price = float(current_candle["open"])
        if open_price <= 0:
            continue
        drawdown_fields = _build_drawdown_fields(
            current_price=item["last_price"],
            highs_7d=_extract_highs_from_klines(rows[-7:]),
            highs_window=_extract_highs_from_klines([current_candle]),
        )
        leaderboard.append(
            {
                "symbol": item["symbol"],
                "change": (item["last_price"] / open_price - 1.0) * 100.0,
                "volume": item["quote_volume"],
                "last_price": item["last_price"],
                **drawdown_fields,
            }
        )

    leaderboard.sort(key=lambda row: row["change"], reverse=True)
    top_list = leaderboard[: int(top_n)]
    losers_list = sorted(leaderboard, key=lambda row: row["change"])[: int(top_n)]
    return {
        "snapshot_date": datetime.now(utc8).strftime("%Y-%m-%d") if now_utc is None else now_utc.astimezone(utc8).strftime("%Y-%m-%d"),
        "snapshot_time": now_utc.astimezone(utc8).strftime("%Y-%m-%d %H:%M:%S"),
        "window_start_utc": midnight_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "candidates": len(candidates),
        "effective": len(leaderboard),
        "top": len(top_list),
        "rows": top_list,
        "losers_rows": losers_list,
        "all_rows": leaderboard,
    }


def calculate_rebound_snapshots(
    *,
    ticker_data,
    daily_klines_by_symbol,
    utc8,
    now_utc: datetime,
    top_n_by_window: dict[int, int],
    windows=DEFAULT_REBOUND_WINDOWS,
):
    """Purely calculate all rebound windows from the same local rows and prices."""
    prices = {}
    for item in ticker_data or []:
        symbol = str(item.get("symbol") or "")
        price = _ticker_last_price(item)
        if symbol and price is not None and symbol in daily_klines_by_symbol:
            prices[symbol] = price

    midnight_utc = now_utc.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    snapshots = {}
    for window_days in windows:
        days = int(window_days)
        cutoff_ms = int((midnight_utc - timedelta(days=max(0, days - 1))).timestamp() * 1000)
        metric_field = f"rebound_{days}d_pct"
        low_field = f"low_{days}d"
        low_time_field = f"low_{days}d_at_utc"
        rebound_rows = []

        for symbol in sorted(prices):
            window_rows = [
                row
                for row in daily_klines_by_symbol.get(symbol, [])
                if int(row["open_time"]) >= cutoff_ms
            ]
            valid_rows = [row for row in window_rows if float(row.get("low", 0.0)) > 0]
            if not valid_rows:
                continue
            low_row = min(valid_rows, key=lambda row: float(row["low"]))
            low_price = float(low_row["low"])
            current_price = prices[symbol]
            rebound_rows.append(
                {
                    "symbol": symbol,
                    "current_price": current_price,
                    low_field: low_price,
                    low_time_field: datetime.fromtimestamp(
                        int(low_row["open_time"]) / 1000,
                        tz=timezone.utc,
                    ).strftime("%Y-%m-%d %H:%M:%S"),
                    metric_field: (current_price / low_price - 1.0) * 100.0,
                    **_build_drawdown_fields(
                        current_price=current_price,
                        highs_7d=_extract_highs_from_klines(valid_rows[-7:]),
                        highs_window=_extract_highs_from_klines(valid_rows),
                    ),
                }
            )

        rebound_rows.sort(key=lambda row: row[metric_field], reverse=True)
        top_list = rebound_rows[: int(top_n_by_window.get(days, 10))]
        snapshots[days] = {
            "snapshot_date": now_utc.astimezone(utc8).strftime("%Y-%m-%d"),
            "snapshot_time": now_utc.astimezone(utc8).strftime("%Y-%m-%d %H:%M:%S"),
            "window_start_utc": (midnight_utc - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S"),
            "candidates": len(prices),
            "effective": len(rebound_rows),
            "top": len(top_list),
            "rows": top_list,
            "all_rows": rebound_rows,
        }
    return snapshots


def _load_daily_market_view(scheduler, *, now_utc: datetime):
    repo = getattr(scheduler, "daily_kline_repo", None)
    if repo is None:
        raise RuntimeError("daily_kline_repo 未配置")
    since_ms = int((now_utc.astimezone(timezone.utc) - timedelta(days=366)).timestamp() * 1000)
    market_view = repo.load(since_open_time=since_ms)
    if not market_view:
        raise RuntimeError("本地日K线缓存为空，请先执行日K线更新任务")
    return market_view


def build_all_market_snapshots(scheduler, utc8, *, now_utc: datetime | None = None):
    """Fetch one all-market ticker, then calculate all five snapshots locally."""
    resolved_now_utc = now_utc or datetime.now(timezone.utc)
    market_view = _load_daily_market_view(scheduler, now_utc=resolved_now_utc)
    session = WeightedRequestSession(scheduler.processor.client)
    ticker_data = session.public_get("/fapi/v1/ticker/24hr")
    if not ticker_data or not isinstance(ticker_data, list):
        raise RuntimeError("无法获取全市场 24hr ticker")

    leaderboard = calculate_top_gainers_snapshot(
        ticker_data=ticker_data,
        daily_klines_by_symbol=market_view,
        utc8=utc8,
        now_utc=resolved_now_utc,
        min_quote_volume=scheduler.leaderboard_min_quote_volume,
        max_symbols=scheduler.leaderboard_max_symbols,
        top_n=scheduler.leaderboard_top_n,
    )
    top_n_by_window = {
        14: scheduler.rebound_7d_top_n,
        30: scheduler.rebound_30d_top_n,
        60: scheduler.rebound_60d_top_n,
        365: scheduler.rebound_365d_top_n,
    }
    rebounds = calculate_rebound_snapshots(
        ticker_data=ticker_data,
        daily_klines_by_symbol=market_view,
        utc8=utc8,
        now_utc=resolved_now_utc,
        top_n_by_window=top_n_by_window,
    )
    logger.info(
        "市场快照数据任务完成: "
        f"ticker_requests={len(session.requests)}, actual_weight={session.total_weight}, "
        "rebound_calculation_rest_requests=0"
    )
    return {
        "leaderboard": leaderboard,
        "rebounds": rebounds,
        "request_count": len(session.requests),
        "request_weight": session.total_weight,
    }


def build_top_gainers_snapshot(scheduler, utc8):
    """Compatibility entry point backed only by the local daily-kline cache."""
    now_utc = datetime.now(timezone.utc)
    market_view = _load_daily_market_view(scheduler, now_utc=now_utc)
    session = WeightedRequestSession(scheduler.processor.client)
    ticker_data = session.public_get("/fapi/v1/ticker/24hr")
    if not ticker_data or not isinstance(ticker_data, list):
        raise RuntimeError("无法获取 24hr ticker")
    snapshot = calculate_top_gainers_snapshot(
        ticker_data=ticker_data,
        daily_klines_by_symbol=market_view,
        utc8=utc8,
        now_utc=now_utc,
        min_quote_volume=scheduler.leaderboard_min_quote_volume,
        max_symbols=scheduler.leaderboard_max_symbols,
        top_n=scheduler.leaderboard_top_n,
    )
    snapshot["request_weight"] = session.total_weight
    return snapshot


def build_rebound_snapshot(
    scheduler,
    *,
    utc8,
    window_days: int,
    top_n: int,
    kline_workers: int,
    weight_budget_per_minute: int,
    label: str,
):
    """Compatibility entry point; no per-symbol K-line requests are emitted."""
    del kline_workers, weight_budget_per_minute, label
    now_utc = datetime.now(timezone.utc)
    market_view = _load_daily_market_view(scheduler, now_utc=now_utc)
    session = WeightedRequestSession(scheduler.processor.client)
    ticker_data = session.public_get("/fapi/v1/ticker/24hr")
    if not ticker_data or not isinstance(ticker_data, list):
        raise RuntimeError("无法获取全市场 ticker")
    snapshot = calculate_rebound_snapshots(
        ticker_data=ticker_data,
        daily_klines_by_symbol=market_view,
        utc8=utc8,
        now_utc=now_utc,
        top_n_by_window={int(window_days): int(top_n)},
        windows=(int(window_days),),
    )[int(window_days)]
    snapshot["request_weight"] = session.total_weight
    return snapshot
