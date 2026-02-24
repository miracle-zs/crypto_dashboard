from dataclasses import dataclass
import os

from app.logger import logger


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes")


def _env_int(name: str, default: int, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None:
        value = default
    else:
        try:
            value = int(raw)
        except ValueError:
            logger.warning(f"环境变量 {name}={raw} 非法，使用默认值 {default}")
            value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


def _env_float(name: str, default: float, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None:
        value = default
    else:
        try:
            value = float(raw)
        except ValueError:
            logger.warning(f"环境变量 {name}={raw} 非法，使用默认值 {default}")
            value = default
    if minimum is not None:
        value = max(minimum, value)
    return value


# compatibility aliases used by scheduler refactor path
read_int_env = _env_int
read_float_env = _env_float


@dataclass(frozen=True)
class SchedulerConfig:
    scheduler_timezone: str
    days_to_fetch: int
    update_interval_minutes: int
    trades_incremental_fallback_interval_minutes: int
    open_positions_update_interval_minutes: int
    start_date: str | None
    end_date: str | None
    sync_lookback_minutes: int
    symbol_sync_overlap_minutes: int
    open_positions_lookback_days: int
    enable_daily_open_positions_full_sync: bool
    open_positions_full_lookback_days: int
    open_positions_full_sync_hour: int
    open_positions_full_sync_minute: int
    enable_daily_full_sync: bool
    daily_full_sync_hour: int
    daily_full_sync_minute: int
    use_time_filter: bool
    enable_user_stream: bool
    force_full_sync: bool
    enable_leaderboard_alert: bool
    leaderboard_top_n: int
    leaderboard_min_quote_volume: float
    leaderboard_max_symbols: int
    leaderboard_kline_workers: int
    leaderboard_weight_budget_per_minute: int
    leaderboard_alert_hour: int
    leaderboard_alert_minute: int
    leaderboard_guard_before_minutes: int
    leaderboard_guard_after_minutes: int
    enable_rebound_7d_snapshot: bool
    rebound_7d_top_n: int
    rebound_7d_kline_workers: int
    rebound_7d_weight_budget_per_minute: int
    rebound_7d_hour: int
    rebound_7d_minute: int
    enable_rebound_30d_snapshot: bool
    rebound_30d_top_n: int
    rebound_30d_kline_workers: int
    rebound_30d_weight_budget_per_minute: int
    rebound_30d_hour: int
    rebound_30d_minute: int
    enable_rebound_60d_snapshot: bool
    rebound_60d_top_n: int
    rebound_60d_kline_workers: int
    rebound_60d_weight_budget_per_minute: int
    rebound_60d_hour: int
    rebound_60d_minute: int
    noon_loss_check_hour: int
    noon_loss_check_minute: int
    noon_review_hour: int
    noon_review_minute: int
    noon_review_target_day_offset: int
    enable_profit_alert: bool
    enable_reentry_alert: bool
    profit_alert_threshold_pct: float
    api_job_lock_wait_seconds: int
    enable_triggered_trades_compensation: bool
    trades_compensation_lookback_minutes: int


def load_scheduler_config() -> SchedulerConfig:
    update_interval_minutes = _env_int("UPDATE_INTERVAL_MINUTES", 10, minimum=1)
    trades_incremental_fallback_interval_minutes = _env_int(
        "TRADES_INCREMENTAL_FALLBACK_INTERVAL_MINUTES",
        1440,
        minimum=1,
    )
    daily_full_sync_hour = _env_int("DAILY_FULL_SYNC_HOUR", 3, minimum=0) % 24
    daily_full_sync_minute = _env_int("DAILY_FULL_SYNC_MINUTE", 30, minimum=0) % 60
    open_positions_full_default_minute = (daily_full_sync_minute + 20) % 60

    rebound_7d_kline_workers = _env_int("REBOUND_7D_KLINE_WORKERS", 6, minimum=1)
    rebound_7d_weight_budget_per_minute = _env_int(
        "REBOUND_7D_WEIGHT_BUDGET_PER_MINUTE", 900, minimum=60
    )
    rebound_7d_hour = _env_int("REBOUND_7D_HOUR", 7, minimum=0)
    rebound_7d_minute = _env_int("REBOUND_7D_MINUTE", 30, minimum=0)

    return SchedulerConfig(
        scheduler_timezone=os.getenv("SCHEDULER_TIMEZONE", "Asia/Shanghai"),
        days_to_fetch=_env_int("DAYS_TO_FETCH", 30, minimum=1),
        update_interval_minutes=update_interval_minutes,
        trades_incremental_fallback_interval_minutes=trades_incremental_fallback_interval_minutes,
        open_positions_update_interval_minutes=_env_int(
            "OPEN_POSITIONS_UPDATE_INTERVAL_MINUTES",
            update_interval_minutes,
            minimum=1,
        ),
        start_date=os.getenv("START_DATE"),
        end_date=os.getenv("END_DATE"),
        sync_lookback_minutes=_env_int("SYNC_LOOKBACK_MINUTES", 1440, minimum=1),
        symbol_sync_overlap_minutes=_env_int("SYMBOL_SYNC_OVERLAP_MINUTES", 1440, minimum=1),
        open_positions_lookback_days=_env_int("OPEN_POSITIONS_LOOKBACK_DAYS", 3, minimum=1),
        enable_daily_open_positions_full_sync=_env_bool("ENABLE_DAILY_OPEN_POSITIONS_FULL_SYNC", True),
        open_positions_full_lookback_days=_env_int("OPEN_POSITIONS_FULL_LOOKBACK_DAYS", 60, minimum=1),
        open_positions_full_sync_hour=_env_int(
            "OPEN_POSITIONS_FULL_SYNC_HOUR",
            daily_full_sync_hour,
            minimum=0,
        ) % 24,
        open_positions_full_sync_minute=_env_int(
            "OPEN_POSITIONS_FULL_SYNC_MINUTE",
            open_positions_full_default_minute,
            minimum=0,
        ) % 60,
        enable_daily_full_sync=_env_bool("ENABLE_DAILY_FULL_SYNC", True),
        daily_full_sync_hour=daily_full_sync_hour,
        daily_full_sync_minute=daily_full_sync_minute,
        use_time_filter=_env_bool("SYNC_USE_TIME_FILTER", True),
        enable_user_stream=_env_bool("ENABLE_USER_STREAM", False),
        force_full_sync=_env_bool("FORCE_FULL_SYNC", False),
        enable_leaderboard_alert=_env_bool("ENABLE_LEADERBOARD_ALERT", True),
        leaderboard_top_n=_env_int("LEADERBOARD_TOP_N", 10, minimum=1),
        leaderboard_min_quote_volume=_env_float("LEADERBOARD_MIN_QUOTE_VOLUME", 50_000_000, minimum=0.0),
        leaderboard_max_symbols=_env_int("LEADERBOARD_MAX_SYMBOLS", 120, minimum=0),
        leaderboard_kline_workers=_env_int("LEADERBOARD_KLINE_WORKERS", 6, minimum=1),
        leaderboard_weight_budget_per_minute=_env_int("LEADERBOARD_WEIGHT_BUDGET_PER_MINUTE", 900, minimum=60),
        leaderboard_alert_hour=_env_int("LEADERBOARD_ALERT_HOUR", 7, minimum=0) % 24,
        leaderboard_alert_minute=_env_int("LEADERBOARD_ALERT_MINUTE", 40, minimum=0) % 60,
        leaderboard_guard_before_minutes=_env_int("LEADERBOARD_GUARD_BEFORE_MINUTES", 2, minimum=0),
        leaderboard_guard_after_minutes=_env_int("LEADERBOARD_GUARD_AFTER_MINUTES", 5, minimum=0),
        enable_rebound_7d_snapshot=_env_bool("ENABLE_REBOUND_7D_SNAPSHOT", True),
        rebound_7d_top_n=_env_int("REBOUND_7D_TOP_N", 10, minimum=1),
        rebound_7d_kline_workers=rebound_7d_kline_workers,
        rebound_7d_weight_budget_per_minute=rebound_7d_weight_budget_per_minute,
        rebound_7d_hour=rebound_7d_hour % 24,
        rebound_7d_minute=rebound_7d_minute % 60,
        enable_rebound_30d_snapshot=_env_bool("ENABLE_REBOUND_30D_SNAPSHOT", True),
        rebound_30d_top_n=_env_int("REBOUND_30D_TOP_N", 10, minimum=1),
        rebound_30d_kline_workers=_env_int("REBOUND_30D_KLINE_WORKERS", rebound_7d_kline_workers, minimum=1),
        rebound_30d_weight_budget_per_minute=_env_int(
            "REBOUND_30D_WEIGHT_BUDGET_PER_MINUTE",
            rebound_7d_weight_budget_per_minute,
            minimum=60,
        ),
        rebound_30d_hour=_env_int("REBOUND_30D_HOUR", rebound_7d_hour, minimum=0) % 24,
        rebound_30d_minute=_env_int("REBOUND_30D_MINUTE", rebound_7d_minute + 2, minimum=0) % 60,
        enable_rebound_60d_snapshot=_env_bool("ENABLE_REBOUND_60D_SNAPSHOT", True),
        rebound_60d_top_n=_env_int("REBOUND_60D_TOP_N", 10, minimum=1),
        rebound_60d_kline_workers=_env_int("REBOUND_60D_KLINE_WORKERS", rebound_7d_kline_workers, minimum=1),
        rebound_60d_weight_budget_per_minute=_env_int(
            "REBOUND_60D_WEIGHT_BUDGET_PER_MINUTE",
            rebound_7d_weight_budget_per_minute,
            minimum=60,
        ),
        rebound_60d_hour=_env_int("REBOUND_60D_HOUR", rebound_7d_hour, minimum=0) % 24,
        rebound_60d_minute=_env_int("REBOUND_60D_MINUTE", rebound_7d_minute + 4, minimum=0) % 60,
        noon_loss_check_hour=_env_int("NOON_LOSS_CHECK_HOUR", 11, minimum=0) % 24,
        noon_loss_check_minute=_env_int("NOON_LOSS_CHECK_MINUTE", 50, minimum=0) % 60,
        noon_review_hour=_env_int("NOON_REVIEW_HOUR", 23, minimum=0) % 24,
        noon_review_minute=_env_int("NOON_REVIEW_MINUTE", 2, minimum=0) % 60,
        noon_review_target_day_offset=_env_int("NOON_REVIEW_TARGET_DAY_OFFSET", 0),
        enable_profit_alert=_env_bool("ENABLE_PROFIT_ALERT", True),
        enable_reentry_alert=_env_bool("ENABLE_REENTRY_ALERT", True),
        profit_alert_threshold_pct=_env_float("PROFIT_ALERT_THRESHOLD_PCT", 20.0, minimum=0.0),
        api_job_lock_wait_seconds=_env_int("API_JOB_LOCK_WAIT_SECONDS", 8, minimum=0),
        enable_triggered_trades_compensation=_env_bool("ENABLE_TRIGGERED_TRADES_COMPENSATION", True),
        trades_compensation_lookback_minutes=_env_int("TRADES_COMPENSATION_LOOKBACK_MINUTES", 1440, minimum=1),
    )
