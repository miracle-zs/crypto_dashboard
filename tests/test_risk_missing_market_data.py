from types import SimpleNamespace

import pytest

from app.jobs.noon_loss_job import run_noon_loss_check, run_noon_loss_review
from app.models import CrashRiskResponse
from app.services.crash_risk_service import CrashRiskService


@pytest.fixture(autouse=True)
def prevent_notifications(monkeypatch):
    sent = []
    monkeypatch.setattr("app.jobs.noon_loss_job.send_server_chan_notification", lambda title, body: sent.append((title, body)))
    return sent


def test_noon_check_does_not_save_a_partial_price_result(prevent_notifications):
    saved = []
    positions = [
        {"symbol": "BTC", "side": "LONG", "entry_price": 100, "qty": 1},
        {"symbol": "ETH", "side": "LONG", "entry_price": 100, "qty": 1},
    ]
    scheduler = SimpleNamespace(
        risk_repo=SimpleNamespace(get_open_positions=lambda: positions, save_noon_loss_snapshot=saved.append),
        trade_repo=SimpleNamespace(get_balance_history=lambda **kwargs: [{"balance": 1000}]),
        _normalize_futures_symbol=lambda symbol: symbol + "USDT",
        _get_mark_price_map=lambda symbols: {"BTCUSDT": 90},
    )
    run_noon_loss_check(scheduler)
    assert saved == []
    assert len(prevent_notifications) == 1
    assert "未完成" in prevent_notifications[0][0]


def test_noon_check_does_not_report_zero_percentage_without_balance():
    saved = []
    scheduler = SimpleNamespace(
        risk_repo=SimpleNamespace(
            get_open_positions=lambda: [{"symbol": "BTC", "side": "LONG", "entry_price": 100, "qty": 1}],
            save_noon_loss_snapshot=saved.append,
        ),
        trade_repo=SimpleNamespace(get_balance_history=lambda **kwargs: []),
        _normalize_futures_symbol=lambda symbol: symbol + "USDT",
        _get_mark_price_map=lambda symbols: {"BTCUSDT": 90},
    )
    run_noon_loss_check(scheduler)
    assert saved == []


def test_night_review_does_not_invent_missing_noon_snapshot():
    saved = []
    scheduler = SimpleNamespace(risk_repo=SimpleNamespace(
        get_noon_loss_snapshot_by_date=lambda date: None,
        save_noon_loss_review_snapshot=saved.append,
    ))
    run_noon_loss_review(scheduler, snapshot_date="2026-10-28", send_notification=False)
    assert saved == []


def test_night_review_does_not_replace_missing_price_with_noon_price():
    saved = []
    noon = {"rows": [{"symbol": "BTC", "side": "LONG", "entry_price": 100,
                      "qty": 1, "current_price": 90, "current_pnl": -10}]}
    scheduler = SimpleNamespace(
        risk_repo=SimpleNamespace(get_noon_loss_snapshot_by_date=lambda date: noon, save_noon_loss_review_snapshot=saved.append),
        trade_repo=SimpleNamespace(get_balance_history=lambda **kwargs: [{"balance": 1000}]),
        _normalize_futures_symbol=lambda symbol: symbol + "USDT",
        _get_mark_price_map=lambda symbols: {},
    )
    run_noon_loss_review(scheduler, snapshot_date="2026-10-28", send_notification=False)
    assert saved == []


def test_night_review_uses_current_price_when_data_is_complete():
    saved = []
    noon = {"rows": [{"symbol": "BTC", "side": "LONG", "entry_price": 100,
                      "qty": 1, "current_price": 90, "current_pnl": -10}]}
    scheduler = SimpleNamespace(
        risk_repo=SimpleNamespace(get_noon_loss_snapshot_by_date=lambda date: noon, save_noon_loss_review_snapshot=saved.append),
        trade_repo=SimpleNamespace(get_balance_history=lambda **kwargs: [{"balance": 1000}]),
        _normalize_futures_symbol=lambda symbol: symbol + "USDT",
        _get_mark_price_map=lambda symbols: {"BTCUSDT": 80},
    )
    run_noon_loss_review(scheduler, snapshot_date="2026-10-28", send_notification=False)
    assert saved[0]["hold_loss_total"] == -20
    assert saved[0]["delta_loss_total"] == 10
    assert saved[0]["rows"][0]["price_source"] == "mark_price"


def test_crash_risk_marks_missing_inputs_and_preserves_previous_snapshot():
    saved = []
    class Repo:
        def __init__(self, db): pass
        def get_candidate_symbols_snapshot_union(self):
            return {"snapshot_date": "2026-10-28", "symbols": ["BTCUSDT", "ETHUSDT"]}
        def save_crash_risk_snapshot(self, value): saved.append(value)
    service = CrashRiskService(repository_cls=Repo)
    service.fetch_symbol_inputs = lambda symbol: (
        {"closes": [100, 110], "highs": [101, 111], "lows": [99, 109],
         "volumes": [10, 12], "open_interests": [1000, 900]}
        if symbol == "BTCUSDT" else {}
    )
    result = service.refresh_from_leaderboard_snapshot(object())
    unknown = next(row for row in result["rows"] if row["symbol"] == "ETHUSDT")
    assert unknown["risk_score"] is None
    assert unknown["stage"] == "数据不足"
    assert result["status"] == "partial"
    assert result["summary"]["unknown"] == 1
    assert saved == []
    serialized = CrashRiskResponse(**result).model_dump()
    assert serialized["status"] == "partial"
    assert next(row for row in serialized["rows"] if row["symbol"] == "ETHUSDT")["risk_score"] is None


def test_crash_risk_saves_fully_evaluable_snapshot():
    saved = []
    class Repo:
        def __init__(self, db): pass
        def get_candidate_symbols_snapshot_union(self):
            return {"snapshot_date": "2026-10-28", "symbols": ["BTCUSDT"]}
        def save_crash_risk_snapshot(self, value): saved.append(value)
    service = CrashRiskService(repository_cls=Repo)
    service.fetch_symbol_inputs = lambda symbol: {
        "closes": [100, 110], "highs": [101, 111], "lows": [99, 109],
        "volumes": [10, 12], "open_interests": [1000, 900],
    }
    result = service.refresh_from_leaderboard_snapshot(object())
    assert result["status"] == "complete"
    assert result["summary"]["unknown"] == 0
    assert saved == [result]
