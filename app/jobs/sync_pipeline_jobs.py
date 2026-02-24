import time
from datetime import datetime, timedelta

import pandas as pd

from app.logger import logger
from app.services.sync_planning_service import build_symbol_since_map


def resolve_sync_window(scheduler, *, force_full: bool, last_entry_time: str | None, utc8):
    is_full_sync_run = force_full
    if force_full:
        is_full_sync_run = True
        if scheduler.start_date:
            try:
                start_dt = datetime.strptime(scheduler.start_date, "%Y-%m-%d").replace(tzinfo=utc8)
                start_dt = start_dt.replace(hour=23, minute=0, second=0, microsecond=0)
                since = int(start_dt.timestamp() * 1000)
                logger.info(f"全量更新模式(FORCE_FULL_SYNC) - 从自定义日期 {scheduler.start_date} 开始")
            except ValueError as exc:
                logger.error(f"日期格式错误: {exc}，使用默认DAYS_TO_FETCH")
                since = int((datetime.now(utc8) - timedelta(days=scheduler.days_to_fetch)).timestamp() * 1000)
        else:
            logger.warning("FORCE_FULL_SYNC=1 但未设置 START_DATE，回退为 DAYS_TO_FETCH 窗口")
            since = int((datetime.now(utc8) - timedelta(days=scheduler.days_to_fetch)).timestamp() * 1000)
    elif last_entry_time:
        try:
            last_dt = datetime.strptime(last_entry_time, "%Y-%m-%d %H:%M:%S").replace(tzinfo=utc8)
            since = int((last_dt - timedelta(minutes=scheduler.sync_lookback_minutes)).timestamp() * 1000)
            logger.info(f"增量更新模式 - 从最近入场时间 {last_entry_time} 回溯 {scheduler.sync_lookback_minutes} 分钟")
        except ValueError as exc:
            logger.error(f"入场时间解析失败: {exc}，使用默认DAYS_TO_FETCH")
            since = int((datetime.now(utc8) - timedelta(days=scheduler.days_to_fetch)).timestamp() * 1000)
    else:
        logger.info(f"增量冷启动 - 获取最近 {scheduler.days_to_fetch} 天数据")
        since = int((datetime.now(utc8) - timedelta(days=scheduler.days_to_fetch)).timestamp() * 1000)

    if scheduler.end_date:
        try:
            end_dt = datetime.strptime(scheduler.end_date, "%Y-%m-%d").replace(tzinfo=utc8)
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999000)
            until = int(end_dt.timestamp() * 1000)
            logger.info(f"使用自定义结束日期: {scheduler.end_date}")
        except ValueError:
            until = int(datetime.now(utc8).timestamp() * 1000)
    else:
        until = int(datetime.now(utc8).timestamp() * 1000)
    return since, until, is_full_sync_run


def fetch_and_analyze_closed_trades(
    scheduler,
    *,
    since: int,
    until: int,
    is_full_sync_run: bool,
):
    logger.info("从Binance API抓取数据...")
    stage_started = time.perf_counter()
    prefetched_fee_totals = None
    if hasattr(scheduler.processor, "get_traded_symbols_and_fee_totals"):
        traded_symbols, prefetched_fee_totals = scheduler.processor.get_traded_symbols_and_fee_totals(since, until)
    else:
        traded_symbols = scheduler.processor.get_traded_symbols(since, until)
    symbols_elapsed = time.perf_counter() - stage_started
    symbol_count = len(traded_symbols)
    logger.info(f"拉取活跃交易币种完成: count={symbol_count}, elapsed={symbols_elapsed:.2f}s")

    stage_started = time.perf_counter()
    symbol_since_map = None
    if not is_full_sync_run and traded_symbols:
        watermarks = scheduler.sync_repo.get_symbol_sync_watermarks(traded_symbols)
        symbol_since_map, warmed_symbols = build_symbol_since_map(
            traded_symbols=traded_symbols,
            watermarks=watermarks,
            since=since,
            overlap_minutes=scheduler.symbol_sync_overlap_minutes,
        )
        logger.info(
            "增量水位策略: "
            f"symbols={len(traded_symbols)}, "
            f"warm={warmed_symbols}, "
            f"cold={len(traded_symbols) - warmed_symbols}, "
            f"overlap_minutes={scheduler.symbol_sync_overlap_minutes}"
        )

    if traded_symbols:
        analysis_result = scheduler.processor.analyze_orders(
            since=since,
            until=until,
            traded_symbols=traded_symbols,
            use_time_filter=scheduler.use_time_filter,
            symbol_since_map=symbol_since_map,
            prefetched_fee_totals=prefetched_fee_totals,
            return_symbol_status=True,
        )
        if not isinstance(analysis_result, (tuple, list)) or len(analysis_result) != 3:
            raise RuntimeError(f"analyze_orders返回结构异常: type={type(analysis_result)}, value={analysis_result}")
        df, success_symbols, failure_symbols = analysis_result
    else:
        df = pd.DataFrame()
        success_symbols = []
        failure_symbols = {}
        logger.info("无活跃币种，跳过闭仓ETL分析")

    analyze_elapsed = time.perf_counter() - stage_started
    logger.info(f"闭仓ETL完成: rows={len(df)}, elapsed={analyze_elapsed:.2f}s")
    return df, success_symbols, failure_symbols, symbol_count, symbols_elapsed, analyze_elapsed


def persist_closed_trades_and_watermarks(
    scheduler,
    *,
    df: pd.DataFrame,
    force_full: bool,
    success_symbols: list[str],
    failure_symbols: dict[str, str],
    until: int,
) -> tuple[float, int]:
    save_trades_elapsed = 0.0
    trades_saved = 0
    if df.empty:
        logger.info("没有新数据需要更新")
    else:
        is_full_sync = force_full
        logger.info(f"保存 {len(df)} 条记录到数据库 (覆盖模式={is_full_sync})...")
        stage_started = time.perf_counter()
        saved_count = scheduler.sync_repo.save_trades(df, overwrite=is_full_sync)
        save_trades_elapsed += time.perf_counter() - stage_started
        trades_saved = saved_count

        if saved_count > 0:
            logger.info("检测到新平仓单，重算统计快照...")
            stage_started = time.perf_counter()
            scheduler.sync_repo.recompute_trade_summary()
            save_trades_elapsed += time.perf_counter() - stage_started

    if success_symbols:
        stage_started = time.perf_counter()
        scheduler.sync_repo.update_symbol_sync_success_batch(symbols=success_symbols, end_ms=until)
        save_trades_elapsed += time.perf_counter() - stage_started
        logger.info(f"同步水位推进: success_symbols={len(success_symbols)}")
    if failure_symbols:
        stage_started = time.perf_counter()
        scheduler.sync_repo.update_symbol_sync_failure_batch(failures=failure_symbols, end_ms=until)
        save_trades_elapsed += time.perf_counter() - stage_started
        logger.warning(f"同步水位未推进(失败): failed_symbols={len(failure_symbols)}")
    return save_trades_elapsed, trades_saved
