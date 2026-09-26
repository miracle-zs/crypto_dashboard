from unittest.mock import MagicMock
import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.crash_risk_service import CrashRiskService


def test_get_crash_risk_is_pure_read_no_network(monkeypatch):
    """GET /api/crash-risk must never call fetch_symbol_inputs or touch network."""
    client = TestClient(app)

    network_called = []

    def mock_fetch(self, symbol, **kwargs):
        network_called.append(symbol)
        raise RuntimeError(f"Network call forbidden on GET! Symbol: {symbol}")

    monkeypatch.setattr(CrashRiskService, "fetch_symbol_inputs", mock_fetch)

    # When we GET /api/crash-risk, even with candidate snapshots in DB, no network call is made
    response = client.get("/api/crash-risk")
    assert response.status_code == 200
    assert len(network_called) == 0

    body = response.json()
    assert "as_of" in body
    assert "summary" in body
    assert "rows" in body


def test_post_refresh_updates_snapshot_and_get_reads_it(monkeypatch):
    """POST /api/crash-risk/refresh computes and saves snapshot, then GET returns it without network."""
    client = TestClient(app)

    mock_series = {
        "closes": [100.0, 105.0, 108.0],
        "highs": [101.0, 106.0, 109.0],
        "lows": [99.0, 102.0, 104.0],
        "volumes": [1000.0, 1200.0, 1500.0],
        "open_interests": [5000.0, 5200.0, 4900.0],
    }

    fetch_calls = []

    def mock_fetch(self, symbol, **kwargs):
        fetch_calls.append(symbol)
        return mock_series

    monkeypatch.setattr(CrashRiskService, "fetch_symbol_inputs", mock_fetch)

    # Mock candidate symbols
    monkeypatch.setattr(
        "app.repositories.crash_risk_repository.CrashRiskRepository.get_candidate_symbols_snapshot_union",
        lambda self: {
            "source": "test_union",
            "snapshot_date": "2026-09-26",
            "snapshot_time": "2026-09-26 10:00:00",
            "symbols": ["BTCUSDT"],
        },
    )

    # POST refresh: should fetch and save
    post_res = client.post("/api/crash-risk/refresh")
    assert post_res.status_code == 200
    post_data = post_res.json()
    assert post_data["as_of"] == "2026-09-26 10:00:00"
    assert len(post_data["rows"]) == 1
    assert post_data["rows"][0]["symbol"] == "BTCUSDT"
    assert len(fetch_calls) == 1

    # Now block network completely!
    def forbid_fetch(self, symbol, **kwargs):
        raise AssertionError("Network called on GET!")

    monkeypatch.setattr(CrashRiskService, "fetch_symbol_inputs", forbid_fetch)

    # GET must return the saved snapshot with ZERO network calls!
    get_res = client.get("/api/crash-risk")
    assert get_res.status_code == 200
    get_data = get_res.json()
    assert get_data["as_of"] == "2026-09-26 10:00:00"
    assert len(get_data["rows"]) == 1
    assert get_data["rows"][0]["symbol"] == "BTCUSDT"
