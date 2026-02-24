def test_scheduler_job_modules_exist():
    from app.jobs.noon_loss_job import run_noon_loss_check
    from app.jobs.sync_jobs import run_sync_open_positions, run_sync_trades_incremental
    from app.jobs.trades_compensation_jobs import (
        request_trades_compensation_job,
        run_pending_trades_compensation_job,
        sync_trades_compensation_job,
    )

    assert callable(run_noon_loss_check)
    assert callable(run_sync_trades_incremental)
    assert callable(run_sync_open_positions)
    assert callable(request_trades_compensation_job)
    assert callable(run_pending_trades_compensation_job)
    assert callable(sync_trades_compensation_job)


def test_scheduler_still_registers_existing_job_ids(monkeypatch):
    from app.scheduler import TradeDataScheduler

    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")

    scheduler = TradeDataScheduler()
    calls = []

    def fake_add_job(*args, **kwargs):
        calls.append(kwargs.get("id"))
        return None

    scheduler.scheduler.add_job = fake_add_job
    scheduler.start()

    assert "sync_trades_incremental" in calls
    assert "check_losses_noon" in calls


def test_scheduler_trades_incremental_uses_fallback_interval_when_triggered_enabled(monkeypatch):
    from app.scheduler import TradeDataScheduler

    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.setenv("ENABLE_TRIGGERED_TRADES_COMPENSATION", "1")
    monkeypatch.setenv("TRADES_INCREMENTAL_FALLBACK_INTERVAL_MINUTES", "45")
    monkeypatch.setenv("UPDATE_INTERVAL_MINUTES", "5")

    scheduler = TradeDataScheduler()
    captured = {}

    def fake_add_job(*args, **kwargs):
        if kwargs.get("id") == "sync_trades_incremental":
            captured["trigger"] = kwargs.get("trigger")
        return None

    scheduler.scheduler.add_job = fake_add_job
    scheduler.start()

    assert int(captured["trigger"].interval.total_seconds() // 60) == 45


def test_scheduler_trades_incremental_uses_update_interval_when_triggered_disabled(monkeypatch):
    from app.scheduler import TradeDataScheduler

    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.setenv("ENABLE_TRIGGERED_TRADES_COMPENSATION", "0")
    monkeypatch.setenv("TRADES_INCREMENTAL_FALLBACK_INTERVAL_MINUTES", "45")
    monkeypatch.setenv("UPDATE_INTERVAL_MINUTES", "7")

    scheduler = TradeDataScheduler()
    captured = {}

    def fake_add_job(*args, **kwargs):
        if kwargs.get("id") == "sync_trades_incremental":
            captured["trigger"] = kwargs.get("trigger")
        return None

    scheduler.scheduler.add_job = fake_add_job
    scheduler.start()

    assert int(captured["trigger"].interval.total_seconds() // 60) == 7


def test_scheduler_user_stream_mode_skips_balance_job_registration(monkeypatch):
    from app.scheduler import TradeDataScheduler

    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.setenv("ENABLE_USER_STREAM", "1")

    scheduler = TradeDataScheduler()
    calls = []

    def fake_add_job(*args, **kwargs):
        calls.append((args, kwargs))
        return None

    scheduler.scheduler.add_job = fake_add_job
    scheduler.start()

    assert all(kwargs.get("id") != "sync_balance" for _args, kwargs in calls)
    assert all(not args or args[0] is not scheduler.sync_balance_data for args, _kwargs in calls)


def test_scheduler_noon_methods_delegate_to_job_module(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()
    calls = {"noon": 0, "review": 0}

    def fake_noon(s):
        calls["noon"] += 1
        assert s is scheduler
        return "noon-ok"

    def fake_review(s, snapshot_date=None, send_notification=True):
        calls["review"] += 1
        assert s is scheduler
        return f"review:{snapshot_date}:{send_notification}"

    monkeypatch.setattr("app.scheduler.run_noon_loss_check", fake_noon)
    monkeypatch.setattr("app.scheduler.run_noon_loss_review", fake_review)

    assert scheduler.check_recent_losses_at_noon() == "noon-ok"
    assert scheduler.review_noon_loss_at_night("2026-02-20", False) == "review:2026-02-20:False"
    assert calls == {"noon": 1, "review": 1}


def test_scheduler_open_positions_sync_delegates_to_job_module(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()
    calls = {"open": 0}

    def fake_open_sync(s):
        calls["open"] += 1
        assert s is scheduler
        return "open-ok"

    monkeypatch.setattr("app.scheduler.run_sync_open_positions", fake_open_sync)

    assert scheduler.sync_open_positions_data() == "open-ok"
    assert calls == {"open": 1}


def test_scheduler_risk_methods_delegate_to_job_module(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()
    calls = {"stale": 0, "sleep": 0}

    def fake_stale(s):
        calls["stale"] += 1
        assert s is scheduler
        return "stale-ok"

    def fake_sleep(s):
        calls["sleep"] += 1
        assert s is scheduler
        return "sleep-ok"

    monkeypatch.setattr("app.scheduler.run_long_held_positions_check", fake_stale)
    monkeypatch.setattr("app.scheduler.run_sleep_risk_check", fake_sleep)

    assert scheduler.check_long_held_positions() == "stale-ok"
    assert scheduler.check_risk_before_sleep() == "sleep-ok"
    assert calls == {"stale": 1, "sleep": 1}


def test_scheduler_alert_methods_delegate_to_job_module(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()
    calls = {"reentry": 0, "profit": 0}

    def fake_reentry(s):
        calls["reentry"] += 1
        assert s is scheduler
        return "reentry-ok"

    def fake_profit(s, threshold_pct):
        calls["profit"] += 1
        assert s is scheduler
        return f"profit-ok:{threshold_pct}"

    monkeypatch.setattr("app.scheduler.run_reentry_alert_check", fake_reentry)
    monkeypatch.setattr("app.scheduler.run_profit_alert_check", fake_profit)

    assert scheduler.check_same_symbol_reentry_alert() == "reentry-ok"
    assert scheduler.check_open_positions_profit_alert(25.0) == "profit-ok:25.0"
    assert calls == {"reentry": 1, "profit": 1}


def test_scheduler_compensation_methods_delegate_to_job_module(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()
    calls = {"request": 0, "pending": 0, "sync": 0}

    def fake_request(s, symbols, reason="open_positions_change", symbol_since_ms=None):
        calls["request"] += 1
        assert s is scheduler
        return ("request", symbols, reason, symbol_since_ms)

    def fake_pending(s):
        calls["pending"] += 1
        assert s is scheduler
        return "pending-ok"

    def fake_sync(s, symbols, reason="triggered", symbol_since_ms=None):
        calls["sync"] += 1
        assert s is scheduler
        return ("sync", symbols, reason, symbol_since_ms)

    monkeypatch.setattr("app.scheduler.request_trades_compensation_job", fake_request)
    monkeypatch.setattr("app.scheduler.run_pending_trades_compensation_job", fake_pending)
    monkeypatch.setattr("app.scheduler.sync_trades_compensation_job", fake_sync)

    assert scheduler.request_trades_compensation(["BTC"], reason="x") == ("request", ["BTC"], "x", None)
    assert scheduler._run_pending_trades_compensation() == "pending-ok"
    assert scheduler.sync_trades_compensation(symbols=["ETH"], reason="y") == ("sync", ["ETH"], "y", None)
    assert calls == {"request": 1, "pending": 1, "sync": 1}


def test_sync_trades_data_handles_empty_symbol_list(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()

    class FakeProcessor:
        def get_traded_symbols(self, since, until):
            return []

    class FakeSyncRepo:
        def update_sync_status(self, **kwargs):
            return None

        def get_last_entry_time(self):
            return None

        def get_statistics(self):
            return {"total_trades": 0, "unique_symbols": 0, "earliest_trade": None, "latest_trade": None}

        def log_sync_run(self, **kwargs):
            return None

    scheduler.processor = FakeProcessor()
    scheduler.sync_repo = FakeSyncRepo()

    monkeypatch.setattr(scheduler, "_is_leaderboard_guard_window", lambda: False)
    monkeypatch.setattr(scheduler, "_is_api_cooldown_active", lambda source: False)
    monkeypatch.setattr(scheduler, "_try_enter_api_job_slot", lambda source: True)
    monkeypatch.setattr(scheduler, "_release_api_job_slot", lambda: None)
    monkeypatch.setattr(scheduler, "check_long_held_positions", lambda: None)

    assert scheduler._sync_trades_data_impl(force_full=False) is True


def test_sync_trades_data_uses_batch_watermark_updates(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()

    class FakeProcessor:
        def get_traded_symbols(self, since, until):
            return ["BTCUSDT", "ETHUSDT"]

        def analyze_orders(self, **kwargs):
            return (
                __import__("pandas").DataFrame(),
                ["BTCUSDT"],
                {"ETHUSDT": "failed"},
            )

    class FakeSyncRepo:
        def __init__(self):
            self.success_batch_calls = []
            self.failure_batch_calls = []

        def update_sync_status(self, **kwargs):
            return None

        def get_last_entry_time(self):
            return None

        def get_symbol_sync_watermarks(self, symbols):
            return {}

        def update_symbol_sync_success_batch(self, symbols, end_ms):
            self.success_batch_calls.append((list(symbols), end_ms))

        def update_symbol_sync_failure_batch(self, failures, end_ms):
            self.failure_batch_calls.append((dict(failures), end_ms))

        def get_statistics(self):
            return {"total_trades": 0, "unique_symbols": 0, "earliest_trade": None, "latest_trade": None}

        def log_sync_run(self, **kwargs):
            return None

    fake_repo = FakeSyncRepo()
    scheduler.processor = FakeProcessor()
    scheduler.sync_repo = fake_repo

    monkeypatch.setattr(scheduler, "_is_leaderboard_guard_window", lambda: False)
    monkeypatch.setattr(scheduler, "_is_api_cooldown_active", lambda source: False)
    monkeypatch.setattr(scheduler, "_try_enter_api_job_slot", lambda source: True)
    monkeypatch.setattr(scheduler, "_release_api_job_slot", lambda: None)
    monkeypatch.setattr(scheduler, "check_long_held_positions", lambda: None)

    assert scheduler._sync_trades_data_impl(force_full=False) is True
    assert len(fake_repo.success_batch_calls) == 1
    assert len(fake_repo.failure_batch_calls) == 1


def test_sync_trades_data_prefers_single_income_pass_api(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()

    class FakeProcessor:
        def get_traded_symbols_and_fee_totals(self, since, until):
            return ["BTCUSDT"], {"BTCUSDT": -1.0}

        def analyze_orders(self, **kwargs):
            assert kwargs.get("prefetched_fee_totals") == {"BTCUSDT": -1.0}
            return (__import__("pandas").DataFrame(), ["BTCUSDT"], {})

    class FakeSyncRepo:
        def update_sync_status(self, **kwargs):
            return None

        def get_last_entry_time(self):
            return None

        def get_symbol_sync_watermarks(self, symbols):
            return {}

        def update_symbol_sync_success_batch(self, symbols, end_ms):
            return None

        def update_symbol_sync_failure_batch(self, failures, end_ms):
            return None

        def get_statistics(self):
            return {"total_trades": 0, "unique_symbols": 0, "earliest_trade": None, "latest_trade": None}

        def log_sync_run(self, **kwargs):
            return None

    scheduler.processor = FakeProcessor()
    scheduler.sync_repo = FakeSyncRepo()

    monkeypatch.setattr(scheduler, "_is_leaderboard_guard_window", lambda: False)
    monkeypatch.setattr(scheduler, "_is_api_cooldown_active", lambda source: False)
    monkeypatch.setattr(scheduler, "_try_enter_api_job_slot", lambda source: True)
    monkeypatch.setattr(scheduler, "_release_api_job_slot", lambda: None)
    monkeypatch.setattr(scheduler, "check_long_held_positions", lambda: None)

    assert scheduler._sync_trades_data_impl(force_full=False) is True


def test_request_trades_compensation_merges_symbols_with_earliest_since(monkeypatch):
    from app.scheduler import TradeDataScheduler, UTC8
    from datetime import datetime

    scheduler = TradeDataScheduler()
    added = {"count": 0}

    def fake_add_job(*args, **kwargs):
        added["count"] += 1
        return None

    scheduler.scheduler.add_job = fake_add_job

    now_ms = int(datetime.now(UTC8).timestamp() * 1000)
    scheduler.request_trades_compensation(
        ["BTCUSDT"],
        reason="test",
        symbol_since_ms={"BTCUSDT": now_ms - 3 * 60 * 60 * 1000},
    )
    scheduler.request_trades_compensation(
        ["BTCUSDT", "ETHUSDT"],
        reason="test",
        symbol_since_ms={"BTCUSDT": now_ms - 60 * 60 * 1000, "ETHUSDT": now_ms - 2 * 60 * 60 * 1000},
    )

    assert added["count"] == 2
    assert "BTCUSDT" in scheduler._pending_compensation_since_ms
    assert "ETHUSDT" in scheduler._pending_compensation_since_ms
    assert scheduler._pending_compensation_since_ms["BTCUSDT"] <= now_ms - 3 * 60 * 60 * 1000


def test_request_trades_compensation_normalizes_base_symbols(monkeypatch):
    from app.scheduler import TradeDataScheduler, UTC8
    from datetime import datetime

    scheduler = TradeDataScheduler()
    scheduler.scheduler.add_job = lambda *args, **kwargs: None
    now_ms = int(datetime.now(UTC8).timestamp() * 1000)

    scheduler.request_trades_compensation(
        ["PIPPIN"],
        reason="test",
        symbol_since_ms={"PIPPIN": now_ms - 5 * 60 * 60 * 1000},
    )

    assert "PIPPINUSDT" in scheduler._pending_compensation_since_ms
