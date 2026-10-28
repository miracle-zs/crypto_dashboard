import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.api.trades_api import router
from app.core.deps import get_db
from app.database import Database
from app.repositories import SyncRepository, TradeRepository
from app.services.trade_income_aggregation import summarize_income_records
from app.trade_processor import TradeDataProcessor


def test_funding_is_not_allocated_as_trade_fee():
    records = [
        {"symbol": "BTCUSDT", "incomeType": "COMMISSION", "income": "-2"},
        {"symbol": "BTCUSDT", "incomeType": "FUNDING_FEE", "income": "-5"},
    ]
    symbols, fees = summarize_income_records(records)
    assert symbols == ["BTCUSDT"]
    assert fees == {"BTCUSDT": -2.0}
    assert summarize_income_records(records, {"FUNDING_FEE"})[1] == {"BTCUSDT": -2.0}


def test_legacy_fee_fetch_also_excludes_funding():
    processor = TradeDataProcessor.__new__(TradeDataProcessor)
    processor._fetch_income_history = lambda **kwargs: [
        {"symbol": "BTCUSDT", "incomeType": "COMMISSION", "income": "-2", "time": 1000},
        {"symbol": "BTCUSDT", "incomeType": "FUNDING_FEE", "income": "-5", "time": 2000},
    ]
    assert processor.get_fees_for_symbol("BTCUSDT", 0, 3000) == {1000: -2.0}


def test_funding_summary_reads_only_recorded_usdt_funding_without_duplicates(tmp_path):
    db = Database(str(tmp_path / "funding.db"))
    sync = SyncRepository(db)
    repo = TradeRepository(db)
    assert repo.get_funding_summary() == {
        "asset": "USDT", "amount": None, "record_count": 0,
        "first_record_ms": None, "last_record_ms": None,
    }
    facts = [
        {"symbol": "BTCUSDT", "incomeType": "FUNDING_FEE", "income": "-5", "asset": "USDT", "time": 1000, "tranId": 1},
        {"symbol": "BTCUSDT", "incomeType": "FUNDING_FEE", "income": "2", "asset": "USDT", "time": 2000, "tranId": 2},
        {"symbol": "BTCUSDT", "incomeType": "COMMISSION", "income": "-1", "asset": "USDT", "time": 3000, "tranId": 3},
        {"symbol": "BTCUSDT", "incomeType": "FUNDING_FEE", "income": "100", "asset": "BTC", "time": 4000, "tranId": 4},
    ]
    sync.save_income_facts(facts)
    sync.save_income_facts(facts)
    result = repo.get_funding_summary()
    assert result["amount"] == pytest.approx(-3.0)
    assert result["record_count"] == 2
    assert result["first_record_ms"] == 1000
    assert result["last_record_ms"] == 2000

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_db] = lambda: db
    with TestClient(app) as client:
        response = client.get("/api/funding-summary")
    assert response.status_code == 200
    assert response.json() == result
