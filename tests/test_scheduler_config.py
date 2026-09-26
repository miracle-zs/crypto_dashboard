import os
from app.core.scheduler_config import load_scheduler_config


def test_scheduler_config_defaults(monkeypatch):
    for key in list(os.environ.keys()):
        if key in ("PATH", "PYTHONPATH", "HOME", "USER", "SHELL", "TMPDIR", "PYTEST_CURRENT_TEST"):
            continue
        monkeypatch.delenv(key, raising=False)

    config = load_scheduler_config()

    assert config.update_interval_minutes == 10
    assert config.trades_incremental_fallback_interval_minutes == 1440
    assert config.open_positions_update_interval_minutes == 5
    assert config.balance_sync_interval_minutes == 15
    assert config.sync_lookback_minutes == 30
    assert config.symbol_sync_overlap_minutes == 30
    assert config.enable_daily_full_sync is True
    assert config.enable_daily_open_positions_full_sync is False
    assert config.enable_triggered_trades_compensation is True
    assert config.trades_compensation_lookback_minutes == 30
    assert config.leaderboard_alert_hour == 7
    assert config.leaderboard_alert_minute == 30
    assert config.daily_kline_update_hour == 6
    assert config.daily_kline_update_minute == 15
    assert config.market_snapshot_hour == 7
    assert config.market_snapshot_minute == 0
    assert config.historical_task_weight_budget_per_60s == 200
    assert config.background_weight_budget_per_60s == 250
    assert config.history_validation_hour == 2
    assert config.history_validation_minute == 10
    assert config.history_validation_lookback_hours == 48
    assert config.enable_rebound_365d_snapshot is True
    assert config.rebound_365d_top_n == 10
    assert config.rebound_365d_hour == 7
    assert config.rebound_365d_minute == 36
    assert config.noon_loss_check_hour == 11
    assert config.noon_loss_check_minute == 50


def test_scheduler_config_invalid_values_fallback(monkeypatch):
    monkeypatch.setenv("UPDATE_INTERVAL_MINUTES", "x")
    monkeypatch.setenv("LEADERBOARD_MIN_QUOTE_VOLUME", "x")
    monkeypatch.setenv("API_JOB_LOCK_WAIT_SECONDS", "-3")

    config = load_scheduler_config()

    assert config.update_interval_minutes == 10
    assert config.leaderboard_min_quote_volume == 50_000_000
    assert config.api_job_lock_wait_seconds == 0


def test_scheduler_weight_budgets_are_hard_capped(monkeypatch):
    monkeypatch.setenv("HISTORICAL_TASK_WEIGHT_BUDGET_PER_60S", "900")
    monkeypatch.setenv("BACKGROUND_WEIGHT_BUDGET_PER_60S", "1200")
    monkeypatch.setenv("LEADERBOARD_WEIGHT_BUDGET_PER_MINUTE", "900")
    monkeypatch.setenv("REBOUND_7D_WEIGHT_BUDGET_PER_MINUTE", "900")
    monkeypatch.setenv("REBOUND_30D_WEIGHT_BUDGET_PER_MINUTE", "900")
    monkeypatch.setenv("REBOUND_60D_WEIGHT_BUDGET_PER_MINUTE", "900")
    monkeypatch.setenv("REBOUND_365D_WEIGHT_BUDGET_PER_MINUTE", "900")

    config = load_scheduler_config()

    assert config.historical_task_weight_budget_per_60s == 200
    assert config.background_weight_budget_per_60s == 250
    assert config.leaderboard_weight_budget_per_minute == 200
    assert config.rebound_7d_weight_budget_per_minute == 200
    assert config.rebound_30d_weight_budget_per_minute == 200
    assert config.rebound_60d_weight_budget_per_minute == 200
    assert config.rebound_365d_weight_budget_per_minute == 200


def test_scheduler_config_time_normalization(monkeypatch):
    monkeypatch.setenv("LEADERBOARD_ALERT_HOUR", "49")
    monkeypatch.setenv("LEADERBOARD_ALERT_MINUTE", "121")
    monkeypatch.setenv("REBOUND_7D_MINUTE", "120")
    monkeypatch.setenv("REBOUND_365D_MINUTE", "122")

    config = load_scheduler_config()

    assert config.leaderboard_alert_hour == 1
    assert config.leaderboard_alert_minute == 1
    assert config.rebound_7d_minute == 0
    assert config.rebound_365d_minute == 2


def test_scheduler_config_balance_sync_float_interval(monkeypatch):
    monkeypatch.setenv("BALANCE_SYNC_INTERVAL_MINUTES", "0.5")
    config = load_scheduler_config()
    assert config.balance_sync_interval_minutes == 0.5

