from datetime import datetime
import time
from zoneinfo import ZoneInfo

from app.logger import logger

UTC8 = ZoneInfo("Asia/Shanghai")


def request_trades_compensation_job(
    scheduler,
    symbols: list[str],
    *,
    reason: str = "open_positions_change",
    symbol_since_ms: dict[str, int] | None = None,
):
    if not scheduler.enable_triggered_trades_compensation:
        return
    normalized = sorted({scheduler._normalize_futures_symbol(symbol) for symbol in symbols if symbol})
    if not normalized:
        return

    normalized_since_map = {}
    if symbol_since_ms:
        for raw_symbol, raw_since in symbol_since_ms.items():
            if not raw_symbol or raw_since is None:
                continue
            symbol = scheduler._normalize_futures_symbol(raw_symbol)
            previous = normalized_since_map.get(symbol)
            normalized_since_map[symbol] = int(raw_since) if previous is None else min(int(raw_since), previous)

    for symbol in normalized:
        fallback_since = int(datetime.now(UTC8).timestamp() * 1000) - int(
            scheduler.trades_compensation_lookback_minutes
        ) * 60 * 1000
        requested_since = int(normalized_since_map[symbol]) if symbol in normalized_since_map else fallback_since
        previous = scheduler._pending_compensation_since_ms.get(symbol)
        scheduler._pending_compensation_since_ms[symbol] = (
            requested_since if previous is None else min(previous, requested_since)
        )

    scheduler.scheduler.add_job(
        func=scheduler._run_pending_trades_compensation,
        trigger="date",
        id="sync_trades_compensation_pending",
        replace_existing=True,
    )
    logger.info(
        "已安排触发式补偿同步: "
        f"symbols={len(normalized)}, reason={reason}, pending_total={len(scheduler._pending_compensation_since_ms)}"
    )


def run_pending_trades_compensation_job(scheduler):
    symbols = sorted(scheduler._pending_compensation_since_ms.keys())
    if not symbols:
        return True
    symbol_since_ms = dict(scheduler._pending_compensation_since_ms)
    scheduler._pending_compensation_since_ms.clear()
    return scheduler.sync_trades_compensation(symbols=symbols, reason="triggered", symbol_since_ms=symbol_since_ms)


def sync_trades_compensation_job(
    scheduler,
    *,
    symbols: list[str],
    reason: str = "triggered",
    symbol_since_ms: dict[str, int] | None = None,
):
    if not scheduler.processor:
        logger.warning("无法执行交易补偿同步: API密钥未配置")
        return True
    if scheduler._is_api_cooldown_active(source="交易补偿同步"):
        return True
    if not scheduler._try_enter_api_job_slot(source="交易补偿同步"):
        return True

    started_at = time.perf_counter()
    since = 0
    until = 0
    success_symbols = []
    failure_symbols = {}
    try:
        until = int(datetime.now(UTC8).timestamp() * 1000)
        fallback_since = max(0, until - int(scheduler.trades_compensation_lookback_minutes) * 60 * 1000)
        logger.info(
            "开始触发式补偿同步... "
            f"reason={reason}, symbols={len(symbols)}, "
            f"lookback_minutes={scheduler.trades_compensation_lookback_minutes}, "
            f"window={scheduler._format_window_with_ms(fallback_since, until)}"
        )

        watermarks = scheduler.sync_repo.get_symbol_sync_watermarks(symbols)
        overlap_ms = int(scheduler.symbol_sync_overlap_minutes) * 60 * 1000
        symbol_since_map = {}
        for symbol in symbols:
            requested_since = (
                int(symbol_since_ms[symbol])
                if symbol_since_ms and symbol in symbol_since_ms and symbol_since_ms[symbol] is not None
                else fallback_since
            )
            symbol_watermark = watermarks.get(symbol)
            if symbol_watermark is not None:
                watermark_since = int(symbol_watermark) - overlap_ms
                requested_since = min(requested_since, watermark_since)
            symbol_since_map[symbol] = max(0, requested_since)

        since = min(symbol_since_map.values()) if symbol_since_map else fallback_since
        result = scheduler.processor.analyze_orders(
            since=since,
            until=until,
            traded_symbols=symbols,
            use_time_filter=scheduler.use_time_filter,
            symbol_since_map=symbol_since_map,
            return_symbol_status=True,
        )
        if not isinstance(result, (tuple, list)) or len(result) != 3:
            raise RuntimeError(f"analyze_orders返回结构异常: type={type(result)}, value={result}")
        df, success_symbols, failure_symbols = result

        save_elapsed, trades_saved = scheduler._persist_closed_trades_and_watermarks(
            df=df,
            force_full=False,
            success_symbols=success_symbols,
            failure_symbols=failure_symbols,
            until=until,
        )
        elapsed = time.perf_counter() - started_at
        scheduler.sync_repo.log_sync_run(
            run_type="trades_compensation",
            mode="triggered",
            status="success",
            symbol_count=len(symbols),
            rows_count=len(df),
            trades_saved=trades_saved,
            open_saved=0,
            elapsed_ms=int(elapsed * 1000),
        )
        logger.info(
            "触发式补偿同步完成: "
            f"symbols={len(symbols)}, rows={len(df)}, saved={trades_saved}, "
            f"save_elapsed={save_elapsed:.2f}s, total_elapsed={elapsed:.2f}s"
        )
        return True
    except Exception as exc:
        elapsed = time.perf_counter() - started_at
        scheduler.sync_repo.log_sync_run(
            run_type="trades_compensation",
            mode="triggered",
            status="error",
            symbol_count=len(symbols),
            rows_count=0,
            trades_saved=0,
            open_saved=0,
            elapsed_ms=int(elapsed * 1000),
            error_message=str(exc),
        )
        logger.error(
            "触发式补偿同步失败: "
            f"{exc}, symbols={symbols}, window={scheduler._format_window_with_ms(since, until) if until else 'n/a'}"
        )
        if success_symbols:
            scheduler.sync_repo.update_symbol_sync_success_batch(symbols=success_symbols, end_ms=until)
        if failure_symbols:
            scheduler.sync_repo.update_symbol_sync_failure_batch(failures=failure_symbols, end_ms=until)
        return False
    finally:
        scheduler._release_api_job_slot()
