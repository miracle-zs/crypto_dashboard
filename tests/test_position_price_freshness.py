import asyncio
from datetime import datetime, timedelta

from app.core.time import UTC8
from app.database import Database
from app.models import OpenPositionsResponse
from app.repositories import SyncRepository
from app.services.market_price_service import MarketPriceService
from app.services.positions_service import PositionsService


def _position(symbol, order_id):
    return {"date": "20261028", "symbol": symbol, "side": "LONG", "entry_time": "2026-10-28 08:00:00",
            "entry_price": 100.0, "qty": 1.0, "entry_amount": 100.0, "order_id": order_id}


def test_fresh_symbol_does_not_hide_another_positions_old_price(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "prices.db"))
    SyncRepository(db).save_open_positions([_position("BTC", 1), _position("ETH", 2)])
    now = datetime(2026, 10, 28, 12, 0, tzinfo=UTC8)
    prices = {"BTCUSDT": (110.0, (now - timedelta(hours=1)).timestamp()),
              "ETHUSDT": (120.0, now.timestamp()), "OTHERUSDT": (1.0, now.timestamp())}
    monkeypatch.setattr(MarketPriceService, "get_latest_cached_prices", lambda: ({key: value[0] for key, value in prices.items()}, now.timestamp()))
    monkeypatch.setattr(MarketPriceService, "get_symbol_cached_price", lambda symbol: prices.get(symbol, (None, None)))
    payload = asyncio.run(PositionsService()._build_open_positions_payload(db, "2026-10-28", now))
    OpenPositionsResponse(**payload)
    rows = {row["symbol"]: row for row in payload["positions"]}
    assert payload["stale"] is True
    assert payload["price_as_of"] == (now - timedelta(hours=1)).isoformat()
    assert rows["BTC"]["price_stale"] is True
    assert rows["ETH"]["price_stale"] is False
    assert rows["BTC"]["price_as_of"] == (now - timedelta(hours=1)).isoformat()


def test_missing_price_is_stale_even_when_another_symbol_is_fresh(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "missing.db"))
    SyncRepository(db).save_open_positions([_position("BTC", 1), _position("ETH", 2)])
    now = datetime(2026, 10, 28, 12, 0, tzinfo=UTC8)
    monkeypatch.setattr(MarketPriceService, "get_latest_cached_prices", lambda: ({"BTCUSDT": 100.0}, now.timestamp()))
    monkeypatch.setattr(MarketPriceService, "get_symbol_cached_price", lambda symbol: (100.0, now.timestamp()) if symbol == "BTCUSDT" else (None, None))
    payload = asyncio.run(PositionsService()._build_open_positions_payload(db, "2026-10-28", now))
    rows = {row["symbol"]: row for row in payload["positions"]}
    assert payload["stale"] is True
    assert rows["ETH"]["price_stale"] is True
    assert rows["ETH"]["price_as_of"] is None
    assert rows["ETH"]["mark_price"] is None
    assert rows["ETH"]["notional"] is None
    assert payload["summary"]["total_notional"] is None
    assert payload["summary"]["total_unrealized_pnl"] is None


def test_price_becoming_stale_changes_incremental_version(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "version.db"))
    pos = _position("BTC", 1)
    pos["entry_time"] = None
    SyncRepository(db).save_open_positions([pos])
    now = datetime(2026, 10, 28, 12, 0, tzinfo=UTC8)
    monkeypatch.setattr(MarketPriceService, "get_symbol_cached_price", lambda symbol: (100.0, now.timestamp()))
    service = PositionsService()
    fresh = asyncio.run(service._build_open_positions_payload(db, "2026-10-28", now))
    stale = asyncio.run(service._build_open_positions_payload(db, "2026-10-28", now + timedelta(minutes=16)))
    assert fresh["stale"] is False
    assert stale["stale"] is True
    fresh_version = service._get_or_bump_version(fresh)
    assert service._get_or_bump_version(stale) > fresh_version
