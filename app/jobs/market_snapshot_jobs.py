import time

from app.core.job_runtime import JobCategory, try_enter_slot
from app.logger import logger
from app.notifier import send_server_chan_notification
from app.services.market_snapshot_service import (
    build_all_market_snapshots,
    build_rebound_snapshot,
    build_top_gainers_snapshot,
    update_daily_kline_cache,
)


def refresh_daily_kline_cache_job(scheduler):
    source = "日K线缓存更新"
    if not scheduler.processor:
        return {"ok": False, "reason": "api_keys_missing", "message": "API密钥未配置"}
    if scheduler._is_api_cooldown_active(source=source):
        return {"ok": False, "reason": "cooldown_active", "message": "Binance API处于冷却中"}
    if not try_enter_slot(scheduler, source=source, category=JobCategory.HEAVY):
        return {"ok": False, "reason": "lock_busy", "message": "任务槽位繁忙"}
    try:
        result = update_daily_kline_cache(
            scheduler,
            history_days=365,
            max_weight_per_60s=scheduler.historical_task_weight_budget_per_60s,
        )
        return {"ok": True, **result}
    except Exception as exc:
        logger.error(f"{source}失败: {exc}")
        return {"ok": False, "reason": "exception", "message": str(exc)}
    finally:
        scheduler._release_api_job_slot()


def build_and_save_all_market_snapshots_job(scheduler, utc8):
    source = "全市场榜单数据任务"
    if not scheduler.processor:
        return {"ok": False, "reason": "api_keys_missing", "message": "API密钥未配置"}
    if scheduler._is_api_cooldown_active(source=source):
        return {"ok": False, "reason": "cooldown_active", "message": "Binance API处于冷却中"}
    if not try_enter_slot(scheduler, source=source, category=JobCategory.HEAVY):
        return {"ok": False, "reason": "lock_busy", "message": "任务槽位繁忙"}
    try:
        result = build_all_market_snapshots(scheduler, utc8)
        leaderboard = result["leaderboard"]
        rebounds = result["rebounds"]
        scheduler.snapshot_repo.save_leaderboard_snapshot(leaderboard)
        scheduler.snapshot_repo.save_rebound_7d_snapshot(rebounds[14])
        scheduler.snapshot_repo.save_rebound_30d_snapshot(rebounds[30])
        scheduler.snapshot_repo.save_rebound_60d_snapshot(rebounds[60])
        scheduler.snapshot_repo.save_rebound_365d_snapshot(rebounds[365])
        scheduler.snapshot_repo.upsert_leaderboard_daily_metrics_for_date(
            str(leaderboard.get("snapshot_date"))
        )
        logger.info(
            "五组市场快照已保存: "
            f"date={leaderboard.get('snapshot_date')}, actual_weight={result['request_weight']}"
        )
        return {"ok": True, **result}
    except Exception as exc:
        logger.error(f"{source}失败: {exc}")
        return {"ok": False, "reason": "exception", "message": str(exc)}
    finally:
        scheduler._release_api_job_slot()


def build_top_gainers_snapshot_job(scheduler, utc8):
    return build_top_gainers_snapshot(scheduler, utc8)


def get_top_gainers_snapshot_job(scheduler, *, source: str, utc8):
    del utc8
    try:
        snapshot = scheduler.snapshot_repo.get_latest_leaderboard_snapshot()
    except Exception as exc:
        logger.error(f"{source}读取快照失败: {exc}")
        return {"ok": False, "reason": "exception", "message": str(exc)}
    if not snapshot or snapshot.get("top", 0) <= 0:
        return {"ok": False, "reason": "no_data", "message": "尚未生成涨幅榜快照"}
    return {"ok": True, **snapshot}


def send_morning_top_gainers_job(scheduler, *, source: str, schedule_hour: int, schedule_minute: int, utc8):
    started_at = time.perf_counter()
    logger.info(
        "晨间涨幅榜任务开始执行: "
        f"schedule={schedule_hour:02d}:{schedule_minute:02d}"
    )
    result = get_top_gainers_snapshot_job(scheduler, source=source, utc8=utc8)
    logger.info(
        "晨间涨幅榜快照结果: "
        f"ok={result.get('ok')}, "
        f"reason={result.get('reason', '')}, "
        f"candidates={result.get('candidates', 0)}, "
        f"effective={result.get('effective', 0)}, "
        f"top={result.get('top', 0)}"
    )
    if not result.get("ok"):
        logger.warning(
            f"晨间涨幅榜任务跳过: reason={result.get('reason')}, message={result.get('message', '')}"
        )
        return

    title = f"【币安合约市场涨跌幅榜 Top {result['top']}】"
    content = (
        "### 币安合约市场晨间涨跌幅榜\n\n"
        f"**更新时间:** {result['snapshot_time']} (UTC+8)\n"
        f"**计算区间:** {result['window_start_utc']} UTC 至当前\n\n"
        "#### 涨幅榜 Top10\n\n"
        "| 排名 | 币种 | 涨幅 | 24h成交额 |\n"
        "|:---:|:---:|:---:|:---:|\n"
    )

    for i, row in enumerate(result["rows"], start=1):
        symbol = row["symbol"]
        change = f"{row['change']:.2f}%"
        volume = f"{int(row['volume'] / 1_000_000)}M"
        content += f"| {i} | {symbol} | {change} | {volume} |\n"

    losers_rows = result.get("losers_rows", [])
    if losers_rows:
        content += (
            "\n#### 跌幅榜 Top10\n\n"
            "| 排名 | 币种 | 跌幅 | 24h成交额 |\n"
            "|:---:|:---:|:---:|:---:|\n"
        )
        for i, row in enumerate(losers_rows, start=1):
            symbol = row["symbol"]
            change = f"{row['change']:.2f}%"
            volume = f"{int(row['volume'] / 1_000_000)}M"
            content += f"| {i} | {symbol} | {change} | {volume} |\n"

    send_server_chan_notification(title, content)
    logger.info(
        "晨间涨幅榜已发送: "
        f"candidates={result['candidates']}, "
        f"effective={result['effective']}, "
        f"top={result['top']}, "
        f"losers_top={len(result.get('losers_rows', []))}, "
        f"elapsed={time.perf_counter() - started_at:.2f}s"
    )


def build_rebound_snapshot_job(
    scheduler,
    *,
    utc8,
    window_days: int,
    top_n: int,
    kline_workers: int,
    weight_budget_per_minute: int,
    label: str,
):
    return build_rebound_snapshot(
        scheduler,
        utc8=utc8,
        window_days=window_days,
        top_n=top_n,
        kline_workers=kline_workers,
        weight_budget_per_minute=weight_budget_per_minute,
        label=label,
    )


def get_rebound_snapshot_job(scheduler, *, source: str, build_snapshot=None, load_snapshot=None):
    if load_snapshot is not None:
        try:
            snapshot = load_snapshot()
        except Exception as exc:
            logger.error(f"{source}读取快照失败: {exc}")
            return {"ok": False, "reason": "exception", "message": str(exc)}
        if not snapshot or snapshot.get("top", 0) <= 0:
            return {"ok": False, "reason": "no_data", "message": "尚未生成反弹榜快照"}
        return {"ok": True, **snapshot}

    if not scheduler.processor:
        return {"ok": False, "reason": "api_keys_missing", "message": "API密钥未配置"}
    if scheduler._is_api_cooldown_active(source=source):
        return {"ok": False, "reason": "cooldown_active", "message": "Binance API处于冷却中"}
    if not try_enter_slot(scheduler, source=source, category=JobCategory.HEAVY):
        return {"ok": False, "reason": "lock_busy", "message": "任务槽位繁忙"}

    try:
        snapshot = build_snapshot()
        if snapshot["top"] <= 0:
            return {"ok": False, "reason": "no_data", "message": "未生成有效榜单", **snapshot}
        return {"ok": True, **snapshot}
    except Exception as exc:
        logger.error(f"{source}失败: {exc}")
        return {"ok": False, "reason": "exception", "message": str(exc)}
    finally:
        scheduler._release_api_job_slot()


def snapshot_morning_rebound_job(
    scheduler,
    *,
    source: str,
    label: str,
    schedule_hour: int,
    schedule_minute: int,
    get_snapshot,
    save_snapshot,
):
    started_at = time.perf_counter()
    logger.info(
        f"晨间{label}任务开始执行: "
        f"schedule={schedule_hour:02d}:{schedule_minute:02d}"
    )
    result = get_snapshot(source=source)
    logger.info(
        f"晨间{label}快照结果: "
        f"ok={result.get('ok')}, "
        f"reason={result.get('reason', '')}, "
        f"candidates={result.get('candidates', 0)}, "
        f"effective={result.get('effective', 0)}, "
        f"top={result.get('top', 0)}"
    )
    if not result.get("ok"):
        logger.warning(
            f"晨间{label}任务跳过: reason={result.get('reason')}, message={result.get('message', '')}"
        )
        return

    try:
        save_snapshot(result)
        logger.info(
            f"{label}快照已保存: date={result.get('snapshot_date')}, top={result.get('top')}"
        )
    except Exception as exc:
        logger.error(f"保存{label}快照失败: {exc}")

    logger.info(
        f"晨间{label}任务完成: "
        f"elapsed={time.perf_counter() - started_at:.2f}s"
    )
