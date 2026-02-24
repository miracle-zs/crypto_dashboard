from app.core.scheduler_config import load_scheduler_config


def test_scheduler_config_defaults(monkeypatch):
    monkeypatch.delenv("UPDATE_INTERVAL_MINUTES", raising=False)
    monkeypatch.delenv("OPEN_POSITIONS_UPDATE_INTERVAL_MINUTES", raising=False)
    monkeypatch.delenv("LEADERBOARD_ALERT_HOUR", raising=False)
    monkeypatch.delenv("LEADERBOARD_ALERT_MINUTE", raising=False)
    monkeypatch.delenv("NOON_LOSS_CHECK_HOUR", raising=False)
    monkeypatch.delenv("NOON_LOSS_CHECK_MINUTE", raising=False)

    config = load_scheduler_config()

    assert config.update_interval_minutes == 10
    assert config.trades_incremental_fallback_interval_minutes == 1440
    assert config.open_positions_update_interval_minutes == 10
    assert config.enable_triggered_trades_compensation is True
    assert config.trades_compensation_lookback_minutes == 1440
    assert config.leaderboard_alert_hour == 7
    assert config.leaderboard_alert_minute == 40
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


def test_scheduler_config_time_normalization(monkeypatch):
    monkeypatch.setenv("LEADERBOARD_ALERT_HOUR", "49")
    monkeypatch.setenv("LEADERBOARD_ALERT_MINUTE", "121")
    monkeypatch.setenv("REBOUND_7D_MINUTE", "120")

    config = load_scheduler_config()

    assert config.leaderboard_alert_hour == 1
    assert config.leaderboard_alert_minute == 1
    assert config.rebound_7d_minute == 0
