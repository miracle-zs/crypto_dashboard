import time
from datetime import datetime
from zoneinfo import ZoneInfo

from app.logger import logger

UTC8 = ZoneInfo("Asia/Shanghai")


def run_sync_trades_incremental(scheduler):
    return scheduler.sync_trades_incremental()


def run_sync_open_positions(scheduler):
    """独立同步未平仓订单，避免与闭仓ETL耦合"""
    if not scheduler.processor:
        logger.warning("无法同步未平仓: API密钥未配置")
        return
    if scheduler._is_api_cooldown_active(source="未平仓同步"):
        return
    if not scheduler._try_enter_api_job_slot(source="未平仓同步"):
        return

    started_at = time.perf_counter()
    try:
        until = int(datetime.now(UTC8).timestamp() * 1000)
        open_since = max(0, until - scheduler.open_positions_lookback_days * 24 * 60 * 60 * 1000)
        logger.info(
            "开始同步未平仓订单... "
            f"lookback_days={scheduler.open_positions_lookback_days}, "
            f"window={scheduler._format_window_with_ms(open_since, until)}"
        )
        open_positions = scheduler.processor.get_open_positions(open_since, until, traded_symbols=None)
        if open_positions is None:
            logger.warning("未平仓同步跳过：PositionRisk请求失败，保留数据库现有持仓")
            scheduler.sync_repo.log_sync_run(
                run_type="open_positions_sync",
                mode="incremental",
                status="skipped",
                symbol_count=0,
                rows_count=0,
                trades_saved=0,
                open_saved=0,
                elapsed_ms=int((time.perf_counter() - started_at) * 1000),
                error_message="position_risk_failed",
            )
            return
        if open_positions:
            open_count = scheduler.sync_repo.save_open_positions(open_positions)
            logger.info(f"保存 {open_count} 条未平仓订单")
            scheduler.check_same_symbol_reentry_alert()
            scheduler.check_open_positions_profit_alert(threshold_pct=scheduler.profit_alert_threshold_pct)
        else:
            open_count = 0
            scheduler.sync_repo.save_open_positions([])
            logger.info("当前无未平仓订单")
        elapsed = time.perf_counter() - started_at
        logger.info(f"未平仓同步完成: elapsed={elapsed:.2f}s")
        scheduler.sync_repo.log_sync_run(
            run_type="open_positions_sync",
            mode="incremental",
            status="success",
            symbol_count=0,
            rows_count=open_count,
            trades_saved=0,
            open_saved=open_count,
            elapsed_ms=int(elapsed * 1000),
        )
    except Exception as exc:
        logger.error(f"未平仓同步失败: {exc}")
        scheduler.sync_repo.log_sync_run(
            run_type="open_positions_sync",
            mode="incremental",
            status="error",
            symbol_count=0,
            rows_count=0,
            trades_saved=0,
            open_saved=0,
            elapsed_ms=int((time.perf_counter() - started_at) * 1000),
            error_message=str(exc),
        )
    finally:
        scheduler._release_api_job_slot()
