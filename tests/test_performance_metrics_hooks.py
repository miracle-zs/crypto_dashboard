def test_metrics_timer_records_elapsed_ms():
    from app.core.metrics import measure_ms

    with measure_ms("unit-test-metric") as snapshot:
        pass

    assert snapshot.elapsed_ms >= 0


def test_sync_trades_data_logs_error_when_impl_returns_false(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()
    statuses = []

    def fake_impl(self, force_full=False):
        return False

    def fake_log_job_metric(*, job_name, status, snapshot):
        statuses.append((job_name, status))

    monkeypatch.setattr(TradeDataScheduler, "_sync_trades_data_impl", fake_impl)
    monkeypatch.setattr("app.scheduler.log_job_metric", fake_log_job_metric)

    scheduler.sync_trades_data()

    assert statuses
    assert statuses[-1] == ("sync_trades_data", "error")


def test_sync_trades_data_logs_error_when_impl_raises(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()
    statuses = []

    def fake_impl(self, force_full=False):
        raise RuntimeError("boom")

    def fake_log_job_metric(*, job_name, status, snapshot):
        statuses.append((job_name, status))

    monkeypatch.setattr(TradeDataScheduler, "_sync_trades_data_impl", fake_impl)
    monkeypatch.setattr("app.scheduler.log_job_metric", fake_log_job_metric)

    try:
        scheduler.sync_trades_data()
    except RuntimeError:
        pass

    assert statuses
    assert statuses[-1] == ("sync_trades_data", "error")


def test_sync_trades_incremental_logs_error_when_sync_returns_false(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()
    statuses = []

    def fake_sync(self, force_full=False, emit_metric=True):
        return False

    def fake_log_job_metric(*, job_name, status, snapshot):
        statuses.append((job_name, status))

    monkeypatch.setattr(TradeDataScheduler, "sync_trades_data", fake_sync)
    monkeypatch.setattr("app.scheduler.log_job_metric", fake_log_job_metric)

    scheduler.sync_trades_incremental()

    assert ("sync_trades_incremental", "error") in statuses


def test_sync_trades_full_logs_error_when_sync_returns_false(monkeypatch):
    from app.scheduler import TradeDataScheduler

    scheduler = TradeDataScheduler()
    statuses = []

    def fake_sync(self, force_full=False, emit_metric=True):
        return False

    def fake_log_job_metric(*, job_name, status, snapshot):
        statuses.append((job_name, status))

    monkeypatch.setattr(TradeDataScheduler, "sync_trades_data", fake_sync)
    monkeypatch.setattr("app.scheduler.log_job_metric", fake_log_job_metric)

    scheduler.sync_trades_full()

    assert ("sync_trades_full", "error") in statuses
