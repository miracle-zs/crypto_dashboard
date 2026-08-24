from functools import partial

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.jobs.noon_loss_job import run_noon_loss_check
from app.jobs.sync_jobs import run_sync_open_positions, run_sync_trades_incremental
from app.logger import logger


def _register_sync_jobs(scheduler, *, utc8):
    scheduler.scheduler.add_job(partial(run_sync_open_positions, scheduler), "date")
    if not scheduler.enable_user_stream:
        scheduler.scheduler.add_job(scheduler.sync_balance_data, "date")

    trades_interval_minutes = (
        scheduler.trades_incremental_fallback_interval_minutes
        if scheduler.enable_triggered_trades_compensation
        else scheduler.update_interval_minutes
    )
    scheduler.scheduler.add_job(
        func=partial(run_sync_trades_incremental, scheduler),
        trigger=IntervalTrigger(minutes=trades_interval_minutes),
        id="sync_trades_incremental",
        name="同步交易数据(游标增量)",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
        replace_existing=True,
    )
    scheduler.scheduler.add_job(
        func=partial(run_sync_open_positions, scheduler),
        trigger=IntervalTrigger(minutes=scheduler.open_positions_update_interval_minutes),
        id="sync_open_positions",
        name="同步未平仓订单",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
        replace_existing=True,
    )

    # The former daily 03:30 scan is now one bounded weekly reconciliation.
    if scheduler.enable_daily_full_sync:
        scheduler.scheduler.add_job(
            func=partial(
                scheduler.sync_trades_full,
                lookback_days=scheduler.days_to_fetch,
            ),
            trigger=CronTrigger(
                day_of_week="sun",
                hour=scheduler.daily_full_sync_hour,
                minute=scheduler.daily_full_sync_minute,
                timezone=utc8,
            ),
            id="sync_trades_full_weekly",
            name="同步交易数据(每周全量校验)",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
            replace_existing=True,
        )

    scheduler.scheduler.add_job(
        func=scheduler.validate_recent_trade_history,
        trigger=CronTrigger(
            hour=scheduler.history_validation_hour,
            minute=scheduler.history_validation_minute,
            timezone=utc8,
        ),
        id="validate_recent_trade_history",
        name="校验最近24-48小时交易数据",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=600,
        replace_existing=True,
    )
    if scheduler.enable_daily_open_positions_full_sync:
        scheduler.scheduler.add_job(
            func=scheduler.sync_open_positions_full_window,
            trigger=CronTrigger(
                day_of_week="sun",
                hour=scheduler.open_positions_full_sync_hour,
                minute=scheduler.open_positions_full_sync_minute,
                timezone=utc8,
            ),
            id="sync_open_positions_full_weekly",
            name="同步未平仓订单(每周全量窗口)",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
            replace_existing=True,
        )

    if not scheduler.enable_user_stream:
        scheduler.scheduler.add_job(
            func=scheduler.sync_balance_data,
            trigger=IntervalTrigger(minutes=scheduler.balance_sync_interval_minutes),
            id="sync_balance",
            name="同步账户余额与TRANSFER流水",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
            replace_existing=True,
        )

    return trades_interval_minutes


def _register_market_jobs(scheduler, *, utc8):
    scheduler.scheduler.add_job(
        func=scheduler.refresh_daily_kline_cache,
        trigger=CronTrigger(
            hour=scheduler.daily_kline_update_hour,
            minute=scheduler.daily_kline_update_minute,
            timezone=utc8,
        ),
        id="refresh_daily_klines",
        name="日K线缓存回填/增量更新",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=900,
        replace_existing=True,
    )

    if (
        scheduler.enable_leaderboard_alert
        or scheduler.enable_rebound_7d_snapshot
        or scheduler.enable_rebound_30d_snapshot
        or scheduler.enable_rebound_60d_snapshot
        or scheduler.enable_rebound_365d_snapshot
    ):
        scheduler.scheduler.add_job(
            func=scheduler.build_and_save_all_market_snapshots,
            trigger=CronTrigger(
                hour=scheduler.market_snapshot_hour,
                minute=scheduler.market_snapshot_minute,
                timezone=utc8,
            ),
            id="build_all_market_snapshots",
            name="生成涨跌幅榜与四组反弹榜",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=900,
            replace_existing=True,
        )

    if scheduler.enable_leaderboard_alert:
        scheduler.scheduler.add_job(
            func=scheduler.send_morning_top_gainers,
            trigger=CronTrigger(
                hour=scheduler.leaderboard_alert_hour,
                minute=scheduler.leaderboard_alert_minute,
                timezone=utc8,
            ),
            id="send_morning_top_gainers",
            name="发送晨间涨幅榜快照",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
            replace_existing=True,
        )


def _register_risk_jobs(scheduler, *, utc8):
    scheduler.scheduler.add_job(
        func=scheduler.check_risk_before_sleep,
        trigger=CronTrigger(hour=23, minute=0, timezone=utc8),
        id="risk_check_sleep",
        name="睡前风控检查",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.scheduler.add_job(
        func=scheduler.review_noon_loss_at_night,
        trigger=CronTrigger(
            hour=scheduler.noon_review_hour,
            minute=scheduler.noon_review_minute,
            timezone=utc8,
        ),
        id="review_noon_loss_night",
        name="午间止损夜间复盘",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
        replace_existing=True,
    )
    scheduler.scheduler.add_job(
        func=partial(run_noon_loss_check, scheduler),
        trigger=CronTrigger(
            hour=scheduler.noon_loss_check_hour,
            minute=scheduler.noon_loss_check_minute,
            timezone=utc8,
        ),
        id="check_losses_noon",
        name="午间浮亏检查",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
        replace_existing=True,
    )


def register_scheduler_jobs(scheduler, *, utc8):
    trades_interval_minutes = _register_sync_jobs(scheduler, utc8=utc8)
    _register_market_jobs(scheduler, utc8=utc8)
    _register_risk_jobs(scheduler, utc8=utc8)
    scheduler.scheduler.start()

    logger.info(
        "后台调度已启动: "
        f"trades={trades_interval_minutes}min, "
        f"positions={scheduler.open_positions_update_interval_minutes}min, "
        f"balance_transfer={scheduler.balance_sync_interval_minutes}min, "
        f"daily_klines={scheduler.daily_kline_update_hour:02d}:{scheduler.daily_kline_update_minute:02d}, "
        f"market_snapshots={scheduler.market_snapshot_hour:02d}:{scheduler.market_snapshot_minute:02d}, "
        f"global_rolling_weight={scheduler.background_weight_budget_per_60s}/60s"
    )
