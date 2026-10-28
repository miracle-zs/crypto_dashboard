from types import SimpleNamespace
import asyncio
from datetime import datetime

import pytest

from app.core.time import UTC8
from app.database import Database
from app.jobs.noon_loss_job import run_noon_loss_check
from app.models import OpenPositionsResponse
from app.repositories import SyncRepository, TradeRepository
from app.services.market_price_service import MarketPriceService
from app.services.positions_service import PositionsService
from app.services.trade_etl_service import extract_open_positions_for_symbol


@pytest.mark.parametrize(("side", "order_side", "quantity"), [
    ("LONG", "BUY", 10.0), ("SHORT", "SELL", -10.0),
])
def test_partial_order_history_preserves_lot_and_reports_unknown_remainder(side, order_side, quantity):
    processor = SimpleNamespace(
        _create_worker_client=lambda: object(),
        get_all_orders=lambda *args, **kwargs: [{
            "orderId": 123, "side": order_side, "positionSide": side,
            "executedQty": "6", "avgPrice": "90", "updateTime": 2000,
        }],
        _latest_position_risk_map={("BTCUSDT", side): {"entryPrice": "100"}},
    )
    positions, _ = extract_open_positions_for_symbol(
        processor, symbol="BTCUSDT", real_net_qty=quantity, since=1000, until=3000
    )
    assert sum(position["qty"] for position in positions) == 10.0
    assert positions[0]["order_id"] == 123
    assert positions[0]["entry_price"] == 90.0
    assert positions[1]["is_incomplete"] is True
    assert positions[1]["qty"] == 4.0
    assert positions[1]["entry_price"] is None
    assert positions[1]["entry_time"] is None


def test_incomplete_position_survives_storage_and_is_not_reported_as_zero_pnl(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "positions.db"))
    repo = SyncRepository(db)
    known = {"date": "20260101", "symbol": "BTC", "side": "LONG", "entry_time": "2026-01-01 00:00:00", "entry_price": 90.0, "qty": 6.0, "entry_amount": 540.0, "order_id": 123}
    unknown = {"date": None, "symbol": "BTC", "side": "LONG", "entry_time": None, "entry_price": None, "qty": 4.0, "entry_amount": None, "order_id": 0, "is_incomplete": True}
    repo.save_open_positions([known])
    conn = db._get_connection()
    conn.execute("UPDATE open_positions SET is_long_term=1 WHERE order_id=123")
    conn.commit()
    conn.close()
    repo.save_open_positions([known, unknown])
    stored = repo.get_open_positions()
    assert next(p for p in stored if p["order_id"] == 123)["is_long_term"] == 1
    monkeypatch.setattr(MarketPriceService, "get_symbol_cached_price", lambda symbol: (100.0, datetime.now(UTC8).timestamp()))
    payload = asyncio.run(PositionsService()._build_open_positions_payload(db, "2026-01-01", datetime.now(UTC8)))
    OpenPositionsResponse(**payload)
    assert payload["summary"]["total_notional"] == 1000.0
    assert payload["summary"]["total_unrealized_pnl"] is None
    missing = next(p for p in payload["positions"] if p["is_incomplete"])
    assert missing["entry_time"] is None
    assert missing["holding_minutes"] is None
    assert missing["unrealized_pnl"] is None
    assert all(row["date"] is not None for row in TradeRepository(db).get_daily_stats())


def test_no_order_history_does_not_invent_an_entry_time():
    processor = SimpleNamespace(
        _create_worker_client=lambda: object(),
        get_all_orders=lambda *args, **kwargs: [],
        _latest_position_risk_map={"BTCUSDT": {"entryPrice": "100"}},
    )
    positions, _ = extract_open_positions_for_symbol(
        processor, symbol="BTCUSDT", real_net_qty=10.0, since=1000, until=3000
    )
    assert positions[0]["entry_time"] is None
    assert positions[0]["date"] is None
    assert positions[0]["entry_price"] == 100.0


def test_noon_check_does_not_save_a_false_zero_for_unknown_cost(monkeypatch):
    saved = []
    monkeypatch.setattr("app.jobs.noon_loss_job.send_server_chan_notification", lambda *args: None)
    scheduler = SimpleNamespace(risk_repo=SimpleNamespace(
        get_open_positions=lambda: [{"symbol": "BTC", "entry_price": None}],
        save_noon_loss_snapshot=saved.append,
    ))
    run_noon_loss_check(scheduler)
    assert saved == []
