from pathlib import Path


def test_rebound_api_uses_repository_instead_of_db_snapshot_calls():
    text = Path("app/api/rebound_api.py").read_text(encoding="utf-8")
    forbidden = [
        "db.get_rebound_7d_snapshot_by_date",
        "db.get_latest_rebound_7d_snapshot",
        "db.list_rebound_7d_snapshot_dates",
        "db.get_rebound_30d_snapshot_by_date",
        "db.get_latest_rebound_30d_snapshot",
        "db.list_rebound_30d_snapshot_dates",
        "db.get_rebound_60d_snapshot_by_date",
        "db.get_latest_rebound_60d_snapshot",
        "db.list_rebound_60d_snapshot_dates",
        "db.get_open_positions",
    ]
    for pattern in forbidden:
        assert pattern not in text


def test_balance_service_uses_repository_for_balance_queries():
    text = Path("app/services/balance_service.py").read_text(encoding="utf-8")
    forbidden = [
        "db.get_balance_history",
        "db.get_transfers",
    ]
    for pattern in forbidden:
        assert pattern not in text


def test_leaderboard_api_uses_repository_for_snapshot_queries():
    text = Path("app/api/leaderboard_api.py").read_text(encoding="utf-8")
    forbidden = [
        "db.list_leaderboard_snapshot_dates",
        "db.get_leaderboard_daily_metrics_by_dates",
        "db.upsert_leaderboard_daily_metrics_for_date",
    ]
    for pattern in forbidden:
        assert pattern not in text


def test_scheduler_jobs_do_not_use_db_directly():
    job_files = [
        "app/jobs/alert_jobs.py",
        "app/jobs/noon_loss_job.py",
        "app/jobs/risk_jobs.py",
        "app/jobs/sync_jobs.py",
    ]
    for file_path in job_files:
        text = Path(file_path).read_text(encoding="utf-8")
        assert "scheduler.db." not in text


def test_scheduler_uses_repositories_for_snapshot_and_balance_writes():
    text = Path("app/scheduler.py").read_text(encoding="utf-8")
    forbidden = [
        "self.db.save_leaderboard_snapshot",
        "self.db.save_rebound_7d_snapshot",
        "self.db.save_rebound_30d_snapshot",
        "self.db.save_rebound_60d_snapshot",
        "self.db.save_balance_history",
        "self.db.save_transfer_income",
    ]
    for pattern in forbidden:
        assert pattern not in text


def test_trade_repository_avoids_direct_passthrough_for_core_queries():
    text = Path("app/repositories/trade_repository.py").read_text(encoding="utf-8")
    forbidden = [
        "return self.db.get_trade_summary()",
        "return self.db.get_statistics()",
        "return self.db.recompute_trade_summary()",
        "return self.db.get_all_trades()",
        "return self.db.get_transfers()",
    ]
    for pattern in forbidden:
        assert pattern not in text


def test_sync_repository_avoids_direct_passthrough_for_summary_queries():
    text = Path("app/repositories/sync_repository.py").read_text(encoding="utf-8")
    forbidden = [
        "return self.db.recompute_trade_summary()",
        "return self.db.get_statistics()",
        "return self.db.get_last_entry_time()",
        "return self.db.update_sync_status(**kwargs)",
        "return self.db.get_symbol_sync_watermarks(symbols)",
        "return self.db.update_symbol_sync_success(**kwargs)",
        "return self.db.update_symbol_sync_failure(**kwargs)",
        "return self.db.save_trades(df, overwrite=overwrite)",
    ]
    for pattern in forbidden:
        assert pattern not in text


def test_settings_repository_avoids_direct_passthrough():
    text = Path("app/repositories/settings_repository.py").read_text(encoding="utf-8")
    assert "return self.db.set_position_long_term(symbol, order_id, is_long_term)" not in text


def test_services_avoid_direct_db_calls_for_system_watchnotes_and_trades():
    service_files = [
        "app/services/trades_api_service.py",
        "app/services/system_api_service.py",
        "app/services/watchnotes_service.py",
        "app/routes/system.py",
        "app/routes/trades.py",
    ]
    forbidden = [
        "db.get_daily_stats",
        "db.get_monthly_target",
        "db.get_monthly_pnl",
        "db.set_monthly_target",
        "db.list_noon_loss_review_history",
        "db.get_noon_loss_review_history_summary",
        "db.list_sync_run_logs",
        "db.get_watch_notes",
        "db.add_watch_note",
        "db.delete_watch_note",
        "db.get_sync_status",
    ]
    for file_path in service_files:
        text = Path(file_path).read_text(encoding="utf-8")
        for pattern in forbidden:
            assert pattern not in text
