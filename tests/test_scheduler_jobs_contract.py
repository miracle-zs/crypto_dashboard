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
