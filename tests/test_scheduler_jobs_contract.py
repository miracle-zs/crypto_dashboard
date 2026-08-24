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


def test_scheduler_registers_one_market_data_pipeline_and_no_per_window_scans(monkeypatch):
    from app.scheduler import TradeDataScheduler

    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.setenv("ENABLE_USER_STREAM", "0")
    monkeypatch.setenv("ENABLE_DAILY_FULL_SYNC", "0")
    scheduler = TradeDataScheduler()
    jobs = {}

    def fake_add_job(*args, **kwargs):
        if kwargs.get("id"):
            jobs[kwargs["id"]] = kwargs
        return None

    scheduler.scheduler.add_job = fake_add_job
    scheduler.start()

    assert "refresh_daily_klines" in jobs
    assert "build_all_market_snapshots" in jobs
    assert "send_morning_top_gainers" in jobs
    assert "validate_recent_trade_history" in jobs
    assert "snapshot_morning_rebound_7d" not in jobs
    assert "snapshot_morning_rebound_30d" not in jobs
    assert "snapshot_morning_rebound_60d" not in jobs
    assert "snapshot_morning_rebound_365d" not in jobs
    assert "sync_trades_full_daily" not in jobs
    assert int(jobs["sync_balance"]["trigger"].interval.total_seconds() // 60) == 15


def test_scheduler_enables_bounded_weekly_full_sync_by_default(monkeypatch):
    from functools import partial

    from app.scheduler import TradeDataScheduler

    monkeypatch.setenv("BINANCE_API_KEY", "k")
    monkeypatch.setenv("BINANCE_API_SECRET", "s")
    monkeypatch.delenv("ENABLE_DAILY_FULL_SYNC", raising=False)
    monkeypatch.setenv("DAYS_TO_FETCH", "60")
    scheduler = TradeDataScheduler()
    jobs = {}

    def fake_add_job(*args, **kwargs):
        if kwargs.get("id"):
            jobs[kwargs["id"]] = kwargs
        return None

    scheduler.scheduler.add_job = fake_add_job
    scheduler.scheduler.start = lambda: None
    scheduler.start()

    weekly = jobs["sync_trades_full_weekly"]
    assert isinstance(weekly["func"], partial)
    assert weekly["func"].keywords == {"lookback_days": 60}
    assert "day_of_week='sun'" in str(weekly["trigger"])


def test_weekly_full_window_ignores_custom_start_date():
    from datetime import datetime
    from types import SimpleNamespace
    from zoneinfo import ZoneInfo

    from app.jobs.sync_pipeline_jobs import resolve_sync_window

    utc8 = ZoneInfo("Asia/Shanghai")
    scheduler = SimpleNamespace(
        start_date="2026-02-01",
        end_date=None,
        days_to_fetch=60,
        sync_lookback_minutes=30,
    )

    since, until, is_full = resolve_sync_window(
        scheduler,
        force_full=True,
        last_entry_time=None,
        utc8=utc8,
        full_lookback_days=60,
    )

    expected_window_ms = 60 * 24 * 60 * 60 * 1000
    custom_start_ms = int(datetime(2026, 2, 1, 23, tzinfo=utc8).timestamp() * 1000)
    assert is_full is True
    assert abs((until - since) - expected_window_ms) < 2_000
    assert since > custom_start_ms


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


def test_scheduler_sync_trades_impl_delegates_to_job_module(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()

    def fake_impl(s, force_full=False):
        assert s is scheduler
        return f"impl:{force_full}"

    monkeypatch.setattr("app.scheduler.run_sync_trades_data_impl", fake_impl)

    assert scheduler._sync_trades_data_impl(force_full=False) == "impl:False"
    assert scheduler._sync_trades_data_impl(force_full=True) == "impl:True"


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

    assert scheduler._sync_trades_data_impl(force_full=False) is False
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


def test_full_sync_crops_each_symbol_to_income_activity_range():
    from types import SimpleNamespace

    from app.jobs.sync_pipeline_jobs import fetch_and_analyze_closed_trades

    captured = {}

    class FakeProcessor:
        def get_traded_symbols_fee_totals_and_ranges(self, since, until):
            return ["BTCUSDT"], {"BTCUSDT": -1.0}, {"BTCUSDT": (200_000, 300_000)}

        def analyze_orders(self, **kwargs):
            captured.update(kwargs)
            return (__import__("pandas").DataFrame(), ["BTCUSDT"], {})

    scheduler = SimpleNamespace(
        processor=FakeProcessor(),
        sync_repo=SimpleNamespace(),
        symbol_sync_overlap_minutes=1,
        use_time_filter=True,
    )

    fetch_and_analyze_closed_trades(
        scheduler,
        since=100_000,
        until=500_000,
        is_full_sync_run=True,
    )

    assert captured["symbol_since_map"] == {"BTCUSDT": 140_000}
    assert captured["symbol_until_map"] == {"BTCUSDT": 360_000}


def test_incremental_pipeline_uses_endpoint_cursors_and_active_symbols_only():
    from types import SimpleNamespace

    from app.jobs.sync_pipeline_jobs import fetch_and_analyze_closed_trades

    captured = {}

    class FakeRepo:
        def get_sync_cursor(self, stream):
            assert stream == "income"
            return {"last_id": 1, "last_time_ms": 1_000_000}

        def get_symbol_sync_watermarks(self, symbols):
            return {symbol: 900_000 for symbol in symbols}

        def get_sync_cursors(self, stream, symbols):
            return {
                symbol: {"last_id": 2 if stream == "orders" else 3, "last_time_ms": 900_000}
                for symbol in symbols
            }

    class FakeProcessor:
        def get_incremental_income_activity(self, since, until):
            captured["income_window"] = (since, until)
            return (
                ["BTCUSDT"],
                {"BTCUSDT": -1.0},
                {"BTCUSDT": (800_000, 950_000)},
                {"last_id": 11, "last_time_ms": 950_000},
            )

        def analyze_orders(self, **kwargs):
            captured["analyze"] = kwargs
            return (
                __import__("pandas").DataFrame(),
                ["BTCUSDT"],
                {},
                {
                    "BTCUSDT": {
                        "orders": {"last_id": 12, "last_time_ms": 960_000},
                        "trades": {"last_id": 13, "last_time_ms": 970_000},
                    }
                },
            )

    scheduler = SimpleNamespace(
        processor=FakeProcessor(),
        sync_repo=FakeRepo(),
        symbol_sync_overlap_minutes=10,
        use_time_filter=True,
    )

    result = fetch_and_analyze_closed_trades(
        scheduler,
        since=0,
        until=2_000_000,
        is_full_sync_run=False,
    )

    assert captured["income_window"] == (400_000, 2_000_000)
    assert captured["analyze"]["traded_symbols"] == ["BTCUSDT"]
    assert captured["analyze"]["return_cursor_updates"] is True
    assert set(captured["analyze"]["order_cursors"]) == {"BTCUSDT"}
    assert result[6]["BTCUSDT"]["trades"]["last_id"] == 13
    assert result[7]["last_id"] == 11


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
