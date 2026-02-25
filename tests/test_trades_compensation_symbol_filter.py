from app.jobs.trades_compensation_jobs import sync_trades_compensation_job


class _FakeProcessor:
    def __init__(self):
        self.analyze_called = False
        self.analyze_kwargs = {}

    def get_all_symbols(self):
        return ["BTCUSDT", "ETHUSDT"]

    def analyze_orders(self, **kwargs):
        self.analyze_called = True
        self.analyze_kwargs = kwargs
        return [], [], {}


class _FakeSyncRepo:
    def __init__(self):
        self.logged = []
        self.updated_success = []
        self.updated_failure = []

    def get_symbol_sync_watermarks(self, symbols):
        return {}

    def log_sync_run(self, **kwargs):
        self.logged.append(kwargs)

    def update_symbol_sync_success_batch(self, symbols, end_ms):
        self.updated_success.append((list(symbols), int(end_ms)))

    def update_symbol_sync_failure_batch(self, failures, end_ms):
        self.updated_failure.append((dict(failures), int(end_ms)))


class _FakeScheduler:
    def __init__(self):
        self.processor = _FakeProcessor()
        self.sync_repo = _FakeSyncRepo()
        self.symbol_sync_overlap_minutes = 15
        self.trades_compensation_lookback_minutes = 1440
        self.use_time_filter = True
        self.released = False

    def _is_api_cooldown_active(self, source):
        return False

    def _try_enter_api_job_slot(self, source):
        return True

    def _release_api_job_slot(self):
        self.released = True

    def _format_window_with_ms(self, start_ms, end_ms):
        return f"{start_ms}-{end_ms}"

    def _persist_closed_trades_and_watermarks(
        self,
        *,
        df,
        force_full,
        success_symbols,
        failure_symbols,
        until,
    ):
        return 0.0, 0


def test_sync_trades_compensation_filters_unsupported_symbols():
    scheduler = _FakeScheduler()

    ok = sync_trades_compensation_job(
        scheduler,
        symbols=["BTCUSDT", "PIPPINUSDT"],
        reason="triggered",
        symbol_since_ms={"BTCUSDT": 1000, "PIPPINUSDT": 2000},
    )

    assert ok is True
    assert scheduler.processor.analyze_called is True
    assert scheduler.processor.analyze_kwargs["traded_symbols"] == ["BTCUSDT"]
    assert scheduler.processor.analyze_kwargs["symbol_since_map"] == {"BTCUSDT": 1000}
    assert scheduler.released is True


def test_sync_trades_compensation_returns_when_no_supported_symbols():
    scheduler = _FakeScheduler()

    ok = sync_trades_compensation_job(
        scheduler,
        symbols=["PIPPINUSDT"],
        reason="triggered",
        symbol_since_ms={"PIPPINUSDT": 2000},
    )

    assert ok is True
    assert scheduler.processor.analyze_called is False
    assert scheduler.released is True
