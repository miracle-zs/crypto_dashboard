from app.jobs.sync_jobs import run_sync_open_positions


class _FakeProcessor:
    def __init__(self, open_positions):
        self._open_positions = open_positions

    def get_open_positions(self, since, until, traded_symbols=None):
        return list(self._open_positions)


class _FakeSyncRepo:
    def __init__(self, previous_rows):
        self._previous_rows = list(previous_rows)
        self.saved_rows = None

    def get_open_positions(self):
        return list(self._previous_rows)

    def save_open_positions(self, rows):
        self.saved_rows = list(rows)
        return len(rows)

    def log_sync_run(self, **kwargs):
        return None


class _FakeScheduler:
    def __init__(self, previous_rows, current_rows):
        self.enable_triggered_trades_compensation = True
        self.processor = _FakeProcessor(current_rows)
        self.sync_repo = _FakeSyncRepo(previous_rows)
        self.open_positions_lookback_days = 3
        self.profit_alert_threshold_pct = 20.0
        self.requested_symbols = []

    def _is_api_cooldown_active(self, source):
        return False

    def _try_enter_api_job_slot(self, source):
        return True

    def _release_api_job_slot(self):
        return None

    def _format_window_with_ms(self, start, end):
        return f"{start}-{end}"

    def check_same_symbol_reentry_alert(self):
        return None

    def check_open_positions_profit_alert(self, threshold_pct):
        return None

    def request_trades_compensation(self, symbols, reason, symbol_since_ms=None):
        self.requested_symbols.append((list(symbols), reason, dict(symbol_since_ms or {})))


def test_run_sync_open_positions_triggers_compensation_for_closed_symbols():
    previous_rows = [
        {"symbol": "BTCUSDT", "order_id": 1, "entry_time": "2026-02-20 10:00:00"},
        {"symbol": "ETHUSDT", "order_id": 2, "entry_time": "2026-02-21 10:00:00"},
    ]
    current_rows = [
        {"symbol": "ETHUSDT", "order_id": 2},
    ]
    scheduler = _FakeScheduler(previous_rows, current_rows)

    run_sync_open_positions(scheduler)

    assert scheduler.requested_symbols[0][0] == ["BTCUSDT"]
    assert scheduler.requested_symbols[0][1] == "open_position_closed"
    assert "BTCUSDT" in scheduler.requested_symbols[0][2]


def test_run_sync_open_positions_does_not_trigger_when_no_closed_positions():
    previous_rows = [
        {"symbol": "ETHUSDT", "order_id": 2},
    ]
    current_rows = [
        {"symbol": "ETHUSDT", "order_id": 2},
    ]
    scheduler = _FakeScheduler(previous_rows, current_rows)

    run_sync_open_positions(scheduler)

    assert scheduler.requested_symbols == []
