from datetime import datetime, timedelta
from types import SimpleNamespace
from zoneinfo import ZoneInfo

from app.jobs.alert_jobs import run_profit_alert_check, run_reentry_alert_check
from app.jobs.noon_loss_job import run_noon_loss_check
from app.jobs.risk_jobs import run_long_held_positions_check


UTC8 = ZoneInfo("Asia/Shanghai")


def test_reentry_alert_job_uses_batch_repo_write(monkeypatch):
    calls = {"batch": []}

    class FakeRiskRepo:
        def get_open_positions(self):
            return [
                {
                    "symbol": "BTC",
                    "order_id": 1,
                    "side": "LONG",
                    "entry_time": "2026-02-20 08:00:00",
                    "reentry_alerted": 0,
                },
                {
                    "symbol": "BTC",
                    "order_id": 2,
                    "side": "LONG",
                    "entry_time": "2026-02-20 12:00:00",
                    "reentry_alerted": 0,
                },
            ]

        def set_positions_reentry_alerted_batch(self, items):
            calls["batch"].append(list(items))

    scheduler = SimpleNamespace(
        risk_repo=FakeRiskRepo(),
        scheduler=SimpleNamespace(timezone=UTC8),
    )
    monkeypatch.setattr("app.jobs.alert_jobs.send_server_chan_notification", lambda title, content: None)

    run_reentry_alert_check(scheduler)
    assert calls["batch"] == [[("BTC", 2)]]


def test_profit_alert_job_uses_batch_repo_write(monkeypatch):
    calls = {"batch": []}

    class FakeRiskRepo:
        def get_open_positions(self):
            return [
                {
                    "symbol": "BTC",
                    "order_id": 1,
                    "side": "LONG",
                    "qty": 1.0,
                    "entry_price": 100.0,
                    "entry_amount": 100.0,
                    "entry_time": "2026-02-20 08:00:00",
                    "profit_alerted": 0,
                }
            ]

        def set_positions_profit_alerted_batch(self, items):
            calls["batch"].append(list(items))

    scheduler = SimpleNamespace(
        enable_profit_alert=True,
        risk_repo=FakeRiskRepo(),
        _normalize_futures_symbol=lambda symbol: f"{str(symbol).upper()}USDT",
        _get_mark_price_map=lambda symbols: {"BTCUSDT": 130.0},
    )
    monkeypatch.setattr("app.jobs.alert_jobs.send_server_chan_notification", lambda title, content: None)

    run_profit_alert_check(scheduler, threshold_pct=20.0)
    assert calls["batch"] == [[("BTC", 1)]]


def test_long_held_risk_job_uses_batch_price_and_repo_write(monkeypatch):
    calls = {"mark": 0, "batch": []}
    old_entry = (datetime.now(UTC8) - timedelta(hours=49)).strftime("%Y-%m-%d %H:%M:%S")

    class FakeRiskRepo:
        def get_open_positions(self):
            return [
                {
                    "symbol": "ETH",
                    "order_id": 9,
                    "side": "LONG",
                    "entry_time": old_entry,
                    "entry_price": 100.0,
                    "qty": 1.0,
                    "alerted": 0,
                    "is_long_term": 0,
                }
            ]

        def set_positions_alerted_batch(self, items):
            calls["batch"].append(list(items))

    def fake_mark_price_map(symbols):
        calls["mark"] += 1
        return {"ETHUSDT": 110.0}

    scheduler = SimpleNamespace(
        risk_repo=FakeRiskRepo(),
        _normalize_futures_symbol=lambda symbol: f"{str(symbol).upper()}USDT",
        _get_mark_price_map=fake_mark_price_map,
    )
    monkeypatch.setattr("app.jobs.risk_jobs.send_server_chan_notification", lambda title, content: None)

    run_long_held_positions_check(scheduler)
    assert calls["mark"] == 1
    assert calls["batch"] == [[("ETH", 9)]]


def test_profit_alert_prefers_repository_candidate_query(monkeypatch):
    class FakeRiskRepo:
        def get_profit_alert_candidates(self):
            return [
                {
                    "symbol": "BTC",
                    "order_id": 1,
                    "side": "LONG",
                    "qty": 1.0,
                    "entry_price": 100.0,
                    "entry_amount": 100.0,
                    "entry_time": "2026-02-20 08:00:00",
                    "profit_alerted": 0,
                }
            ]

        def get_open_positions(self):
            raise AssertionError("should not call full open_positions query")

        def set_positions_profit_alerted_batch(self, items):
            return len(items)

    scheduler = SimpleNamespace(
        enable_profit_alert=True,
        risk_repo=FakeRiskRepo(),
        _normalize_futures_symbol=lambda symbol: f"{str(symbol).upper()}USDT",
        _get_mark_price_map=lambda symbols: {"BTCUSDT": 130.0},
    )
    monkeypatch.setattr("app.jobs.alert_jobs.send_server_chan_notification", lambda title, content: None)
    run_profit_alert_check(scheduler, threshold_pct=20.0)


def test_long_held_prefers_repository_candidate_query(monkeypatch):
    old_entry = (datetime.now(UTC8) - timedelta(hours=49)).strftime("%Y-%m-%d %H:%M:%S")

    class FakeRiskRepo:
        def get_long_held_alert_candidates(self, entry_before, re_alert_before_utc):
            return [
                {
                    "symbol": "ETH",
                    "order_id": 9,
                    "side": "LONG",
                    "entry_time": old_entry,
                    "entry_price": 100.0,
                    "qty": 1.0,
                    "alerted": 0,
                    "is_long_term": 0,
                }
            ]

        def get_open_positions(self):
            raise AssertionError("should not call full open_positions query")

        def set_positions_alerted_batch(self, items):
            return len(items)

    scheduler = SimpleNamespace(
        risk_repo=FakeRiskRepo(),
        _normalize_futures_symbol=lambda symbol: f"{str(symbol).upper()}USDT",
        _get_mark_price_map=lambda symbols: {"ETHUSDT": 110.0},
    )
    monkeypatch.setattr("app.jobs.risk_jobs.send_server_chan_notification", lambda title, content: None)
    run_long_held_positions_check(scheduler)


def test_noon_loss_check_uses_batch_mark_price_map(monkeypatch):
    old_entry = (datetime.now(UTC8) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    calls = {"mark": 0, "saved": 0}

    class FakeRiskRepo:
        def get_open_positions(self):
            return [
                {
                    "symbol": "BTC",
                    "order_id": 1,
                    "side": "LONG",
                    "entry_time": old_entry,
                    "entry_price": 100.0,
                    "qty": 1.0,
                    "is_long_term": 0,
                }
            ]

        def save_noon_loss_snapshot(self, payload):
            calls["saved"] += 1
            assert payload["loss_count"] == 1
            assert len(payload["rows"]) == 1

    class FakeTradeRepo:
        def get_balance_history(self, limit=1):
            return [{"balance": 1000.0}]

    class NeverClient:
        def public_get(self, *args, **kwargs):
            raise AssertionError("should not call single-symbol ticker endpoint")

    class FakeProcessor:
        client = NeverClient()

    def fake_mark_price_map(symbols):
        calls["mark"] += 1
        return {"BTCUSDT": 90.0}

    scheduler = SimpleNamespace(
        risk_repo=FakeRiskRepo(),
        trade_repo=FakeTradeRepo(),
        processor=FakeProcessor(),
        noon_loss_check_hour=11,
        noon_loss_check_minute=50,
        _normalize_futures_symbol=lambda symbol: f"{str(symbol).upper()}USDT",
        _get_mark_price_map=fake_mark_price_map,
    )
    monkeypatch.setattr("app.jobs.noon_loss_job.send_server_chan_notification", lambda title, content: None)

    run_noon_loss_check(scheduler)
    assert calls["mark"] == 1
    assert calls["saved"] == 1
