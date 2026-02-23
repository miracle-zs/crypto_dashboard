from functools import partial

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from app.jobs.noon_loss_job import run_noon_loss_check
from app.jobs.sync_jobs import run_sync_open_positions, run_sync_trades_incremental
from app.logger import logger


def register_scheduler_jobs(scheduler, *, utc8):
    logger.info("立即执行首次数据同步...")
    scheduler.scheduler.add_job(partial(run_sync_trades_incremental, scheduler), "date")
    scheduler.scheduler.add_job(partial(run_sync_open_positions, scheduler), "date")
    scheduler.scheduler.add_job(scheduler.sync_balance_data, "date")

    scheduler.scheduler.add_job(
        func=partial(run_sync_trades_incremental, scheduler),
        trigger=IntervalTrigger(minutes=scheduler.update_interval_minutes),
        id="sync_trades_incremental",
        name="同步交易数据(增量)",
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

    if scheduler.enable_daily_full_sync:
        scheduler.scheduler.add_job(
            func=scheduler.sync_trades_full,
            trigger=CronTrigger(
                hour=scheduler.daily_full_sync_hour,
                minute=scheduler.daily_full_sync_minute,
                timezone=utc8,
            ),
            id="sync_trades_full_daily",
            name="同步交易数据(全量)",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=600,
            replace_existing=True,
        )
        logger.info(
            "全量同步任务已启动: "
            f"每天 {scheduler.daily_full_sync_hour:02d}:{scheduler.daily_full_sync_minute:02d} 执行"
        )
    else:
        logger.info("全量同步任务未启用: ENABLE_DAILY_FULL_SYNC=0")

    if not scheduler.enable_user_stream:
        scheduler.scheduler.add_job(
            func=scheduler.sync_balance_data,
            trigger=IntervalTrigger(minutes=1),
            id="sync_balance",
            name="同步账户余额",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=60,
            replace_existing=True,
        )
    else:
        logger.info("已启用用户数据流，跳过轮询余额同步任务")

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

    if scheduler.enable_leaderboard_alert:
        scheduler.scheduler.add_job(
            func=scheduler.send_morning_top_gainers,
            trigger=CronTrigger(
                hour=scheduler.leaderboard_alert_hour,
                minute=scheduler.leaderboard_alert_minute,
                timezone=utc8,
            ),
            id="send_morning_top_gainers",
            name="晨间涨幅榜",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
            replace_existing=True,
        )
        logger.info(
            "晨间涨幅榜任务已启动: "
            f"每天 {scheduler.leaderboard_alert_hour:02d}:{scheduler.leaderboard_alert_minute:02d} 执行"
        )
    else:
        logger.info("晨间涨幅榜任务未启用: ENABLE_LEADERBOARD_ALERT=0")

    if scheduler.enable_rebound_7d_snapshot:
        scheduler.scheduler.add_job(
            func=scheduler.snapshot_morning_rebound_7d,
            trigger=CronTrigger(
                hour=scheduler.rebound_7d_hour,
                minute=scheduler.rebound_7d_minute,
                timezone=utc8,
            ),
            id="snapshot_morning_rebound_7d",
            name="晨间14D反弹榜",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
            replace_existing=True,
        )
        logger.info(
            "晨间14D反弹榜任务已启动: "
            f"每天 {scheduler.rebound_7d_hour:02d}:{scheduler.rebound_7d_minute:02d} 执行"
        )
    else:
        logger.info("晨间14D反弹榜任务未启用: ENABLE_REBOUND_7D_SNAPSHOT=0")

    if scheduler.enable_rebound_30d_snapshot:
        scheduler.scheduler.add_job(
            func=scheduler.snapshot_morning_rebound_30d,
            trigger=CronTrigger(
                hour=scheduler.rebound_30d_hour,
                minute=scheduler.rebound_30d_minute,
                timezone=utc8,
            ),
            id="snapshot_morning_rebound_30d",
            name="晨间30D反弹榜",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
            replace_existing=True,
        )
        logger.info(
            "晨间30D反弹榜任务已启动: "
            f"每天 {scheduler.rebound_30d_hour:02d}:{scheduler.rebound_30d_minute:02d} 执行"
        )
    else:
        logger.info("晨间30D反弹榜任务未启用: ENABLE_REBOUND_30D_SNAPSHOT=0")

    if scheduler.enable_rebound_60d_snapshot:
        scheduler.scheduler.add_job(
            func=scheduler.snapshot_morning_rebound_60d,
            trigger=CronTrigger(
                hour=scheduler.rebound_60d_hour,
                minute=scheduler.rebound_60d_minute,
                timezone=utc8,
            ),
            id="snapshot_morning_rebound_60d",
            name="晨间60D反弹榜",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
            replace_existing=True,
        )
        logger.info(
            "晨间60D反弹榜任务已启动: "
            f"每天 {scheduler.rebound_60d_hour:02d}:{scheduler.rebound_60d_minute:02d} 执行"
        )
    else:
        logger.info("晨间60D反弹榜任务未启用: ENABLE_REBOUND_60D_SNAPSHOT=0")

    scheduler.scheduler.start()
    logger.info(f"增量交易同步任务已启动: 每 {scheduler.update_interval_minutes} 分钟自动更新一次")
    logger.info(
        f"未平仓同步任务已启动: 每 {scheduler.open_positions_update_interval_minutes} 分钟自动更新一次 "
        f"(lookback_days={scheduler.open_positions_lookback_days})"
    )
    logger.info("余额监控任务已启动: 每 1 分钟自动更新一次")
    logger.info("睡前风控检查已启动: 每天 23:00 执行")
    logger.info(
        "午间浮亏检查已启动: "
        f"每天 {scheduler.noon_loss_check_hour:02d}:{scheduler.noon_loss_check_minute:02d} 执行"
    )
    logger.info(
        "午间止损夜间复盘已启动: "
        f"每天 {scheduler.noon_review_hour:02d}:{scheduler.noon_review_minute:02d} 执行, "
        f"target_day_offset={scheduler.noon_review_target_day_offset}"
    )
