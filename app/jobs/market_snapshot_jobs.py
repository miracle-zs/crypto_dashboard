import time

from app.logger import logger
from app.notifier import send_server_chan_notification
from app.services.market_snapshot_service import build_rebound_snapshot, build_top_gainers_snapshot


def build_top_gainers_snapshot_job(scheduler, utc8):
    return build_top_gainers_snapshot(scheduler, utc8)


def get_top_gainers_snapshot_job(scheduler, *, source: str, utc8):
    if not scheduler.processor:
        return {"ok": False, "reason": "api_keys_missing", "message": "API密钥未配置"}
    if scheduler._is_api_cooldown_active(source=source):
        return {"ok": False, "reason": "cooldown_active", "message": "Binance API处于冷却中"}
    if not scheduler._try_enter_api_job_slot(source=source):
        return {"ok": False, "reason": "lock_busy", "message": "任务槽位繁忙"}

    try:
        snapshot = build_top_gainers_snapshot_job(scheduler, utc8)
        if snapshot["top"] <= 0:
            return {"ok": False, "reason": "no_data", "message": "未生成有效榜单", **snapshot}
        return {"ok": True, **snapshot}
    except Exception as exc:
        logger.error(f"{source}失败: {exc}")
        return {"ok": False, "reason": "exception", "message": str(exc)}
    finally:
        scheduler._release_api_job_slot()


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

    try:
        scheduler.snapshot_repo.save_leaderboard_snapshot(result)
        logger.info(
            f"涨幅榜快照已保存: date={result.get('snapshot_date')}, top={result.get('top')}"
        )
    except Exception as exc:
        logger.error(f"保存涨幅榜快照失败: {exc}")

    try:
        metrics_payload = scheduler.snapshot_repo.upsert_leaderboard_daily_metrics_for_date(
            str(result.get("snapshot_date"))
        )
        if metrics_payload:
            logger.info(
                "涨跌幅指标已保存: "
                f"date={result.get('snapshot_date')}, "
                f"m1={metrics_payload.get('metric1', {}).get('probability_pct')}, "
                f"m2={metrics_payload.get('metric2', {}).get('probability_pct')}, "
                f"m3_eval={metrics_payload.get('metric3', {}).get('evaluated_count')}"
            )
    except Exception as exc:
        logger.error(f"保存涨跌幅指标失败: {exc}")

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


def get_rebound_snapshot_job(scheduler, *, source: str, build_snapshot):
    if not scheduler.processor:
        return {"ok": False, "reason": "api_keys_missing", "message": "API密钥未配置"}
    if scheduler._is_api_cooldown_active(source=source):
        return {"ok": False, "reason": "cooldown_active", "message": "Binance API处于冷却中"}
    if not scheduler._try_enter_api_job_slot(source=source):
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
