def test_scheduler_job_modules_exist():
    from app.jobs.noon_loss_job import run_noon_loss_check
    from app.jobs.sync_jobs import run_sync_open_positions, run_sync_trades_incremental

    assert callable(run_noon_loss_check)
    assert callable(run_sync_trades_incremental)
    assert callable(run_sync_open_positions)


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
