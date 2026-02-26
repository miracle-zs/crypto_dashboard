import time
import traceback
import os

from app.binance_client import BinanceFuturesRestClient
from app.logger import logger


def _read_int_env(name: str, default: int, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None:
        return max(minimum, default)
    try:
        return max(minimum, int(raw))
    except Exception:
        logger.warning(f"环境变量 {name}={raw} 非法，使用默认值 {default}")
        return max(minimum, default)


def _configure_full_sync_request_budget() -> bool:
    enabled = str(os.getenv("FULL_SYNC_REQUEST_BUDGET_ENABLED", "1")).strip().lower() in ("1", "true", "yes")
    if not enabled:
        return False

    budget_per_minute = _read_int_env("FULL_SYNC_REQUEST_BUDGET_PER_MINUTE", 900, minimum=60)
    path_weights = {
        "/fapi/v1/allOrders": _read_int_env("FULL_SYNC_WEIGHT_ALL_ORDERS", 20, minimum=1),
        "/fapi/v1/klines": _read_int_env("FULL_SYNC_WEIGHT_KLINES", 2, minimum=1),
        "/fapi/v1/income": _read_int_env("FULL_SYNC_WEIGHT_INCOME", 30, minimum=1),
    }
    BinanceFuturesRestClient.configure_global_request_budget(
        enabled=True,
        per_minute=budget_per_minute,
        path_weights=path_weights,
    )
    logger.info(
        "全量同步请求预算器已启用: "
        f"budget={budget_per_minute}/min, "
        f"weights={{allOrders:{path_weights['/fapi/v1/allOrders']}, "
        f"klines:{path_weights['/fapi/v1/klines']}, "
        f"income:{path_weights['/fapi/v1/income']}}}"
    )
    return True


def run_sync_trades_data_impl(scheduler, *, force_full: bool = False):
    """同步交易数据到数据库（实际执行逻辑）"""
    if not scheduler.processor:
        logger.warning("无法同步: API密钥未配置")
        return True
    if scheduler._is_leaderboard_guard_window():
        logger.info(
            "跳过交易同步: 位于晨间涨幅榜保护窗口内 "
            f"({scheduler.leaderboard_alert_hour:02d}:{scheduler.leaderboard_alert_minute:02d} "
            f"前{scheduler.leaderboard_guard_before_minutes}分钟至后{scheduler.leaderboard_guard_after_minutes}分钟)"
        )
        return True
    if scheduler._is_api_cooldown_active(source="交易同步"):
        return True
    if not scheduler._try_enter_api_job_slot(source="交易同步"):
        return True

    sync_started_at = time.perf_counter()
    symbols_elapsed = 0.0
    analyze_elapsed = 0.0
    save_trades_elapsed = 0.0
    open_positions_elapsed = 0.0
    risk_check_elapsed = 0.0
    trades_saved = 0
    open_saved = 0
    symbol_count = 0
    budget_enabled = False

    try:
        logger.info("=" * 50)
        run_mode = "全量" if force_full else "增量"
        logger.info(f"开始同步交易数据... mode={run_mode}")
        if force_full:
            budget_enabled = _configure_full_sync_request_budget()

        # 更新同步状态为进行中
        scheduler.sync_repo.update_sync_status(status="syncing")

        last_entry_time = scheduler.sync_repo.get_last_entry_time()
        since, until, is_full_sync_run = scheduler._resolve_sync_window(
            force_full=force_full,
            last_entry_time=last_entry_time,
        )
        (
            df,
            success_symbols,
            failure_symbols,
            symbol_count,
            symbols_elapsed,
            analyze_elapsed,
        ) = scheduler._fetch_and_analyze_closed_trades(
            since=since,
            until=until,
            is_full_sync_run=is_full_sync_run,
        )

        save_trades_elapsed, trades_saved = scheduler._persist_closed_trades_and_watermarks(
            df=df,
            force_full=force_full,
            success_symbols=success_symbols,
            failure_symbols=failure_symbols,
            until=until,
        )

        # 检查持仓超时告警
        stage_started = time.perf_counter()
        scheduler.check_long_held_positions()
        risk_check_elapsed = time.perf_counter() - stage_started

        # 更新同步状态
        scheduler.sync_repo.update_sync_status(status="idle")

        # 显示统计信息
        stats = scheduler.sync_repo.get_statistics()
        logger.info("同步完成!")
        logger.info(f"数据库统计: 总交易数={stats['total_trades']}, 币种数={stats['unique_symbols']}")
        logger.info(f"时间范围: {stats['earliest_trade']} ~ {stats['latest_trade']}")
        total_elapsed = time.perf_counter() - sync_started_at
        logger.info(
            "同步耗时汇总: "
            f"symbols={symbols_elapsed:.2f}s, "
            f"analyze={analyze_elapsed:.2f}s, "
            f"save={save_trades_elapsed:.2f}s, "
            f"open_positions={open_positions_elapsed:.2f}s, "
            f"risk_check={risk_check_elapsed:.2f}s, "
            f"total={total_elapsed:.2f}s, "
            f"symbol_count={symbol_count}, "
            f"trades_saved={trades_saved}, "
            f"open_saved={open_saved}"
        )
        scheduler.sync_repo.log_sync_run(
            run_type="trades_sync",
            mode="full" if force_full else "incremental",
            status="success",
            symbol_count=symbol_count,
            rows_count=len(df),
            trades_saved=trades_saved,
            open_saved=open_saved,
            elapsed_ms=int(total_elapsed * 1000),
        )
        logger.info("=" * 50)
        return True

    except Exception as exc:
        error_msg = f"同步失败: {str(exc)}"
        logger.error(error_msg)
        total_elapsed = time.perf_counter() - sync_started_at
        logger.error(
            "同步失败耗时汇总: "
            f"symbols={symbols_elapsed:.2f}s, "
            f"analyze={analyze_elapsed:.2f}s, "
            f"save={save_trades_elapsed:.2f}s, "
            f"open_positions={open_positions_elapsed:.2f}s, "
            f"risk_check={risk_check_elapsed:.2f}s, "
            f"total={total_elapsed:.2f}s"
        )
        scheduler.sync_repo.update_sync_status(status="error", error_message=error_msg)
        scheduler.sync_repo.log_sync_run(
            run_type="trades_sync",
            mode="full" if force_full else "incremental",
            status="error",
            symbol_count=symbol_count,
            rows_count=0,
            trades_saved=trades_saved,
            open_saved=open_saved,
            elapsed_ms=int(total_elapsed * 1000),
            error_message=error_msg,
        )
        logger.error(traceback.format_exc())
        return False
    finally:
        if budget_enabled:
            BinanceFuturesRestClient.configure_global_request_budget(enabled=False)
            logger.info("全量同步请求预算器已关闭")
        scheduler._release_api_job_slot()
