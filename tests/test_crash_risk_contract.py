def test_crash_risk_contract_shape(client, monkeypatch):
    from app.api import crash_risk_api

    monkey_payload = {
        "as_of": "2026-04-10",
        "summary": {"total": 1, "high_risk": 1, "warning": 0, "watch": 0},
        "source_snapshot": {
            "source": "leaderboard_snapshot",
            "snapshot_date": "2026-04-10",
            "snapshot_time": "2026-04-10 08:00:00",
            "window_start_utc": "2026-04-09 16:00:00",
        },
        "rows": [],
    }

    async def fake_run_in_thread(func, *args, **kwargs):
        return monkey_payload

    monkeypatch.setattr(crash_risk_api, "run_in_thread", fake_run_in_thread)
    response = client.get("/api/crash-risk")
    assert response.status_code == 200
    body = response.json()
    assert "as_of" in body
    assert "summary" in body
    assert "source_snapshot" in body
    assert body["source_snapshot"]["source"] == "leaderboard_snapshot"
    assert "rows" in body


def test_crash_risk_refresh_contract_shape(client, monkeypatch):
    from app.api import crash_risk_api

    monkey_payload = {
        "as_of": "2026-04-10",
        "summary": {"total": 1, "high_risk": 1, "warning": 0, "watch": 0},
        "source_snapshot": {
            "source": "leaderboard_snapshot",
            "snapshot_date": "2026-04-10",
            "snapshot_time": "2026-04-10 08:00:00",
            "window_start_utc": "2026-04-09 16:00:00",
        },
        "rows": [],
    }

    async def fake_run_in_thread(func, *args, **kwargs):
        return monkey_payload

    monkeypatch.setattr(crash_risk_api, "run_in_thread", fake_run_in_thread)
    response = client.post("/api/crash-risk/refresh")
    assert response.status_code in (200, 202)
    body = response.json()
    assert "as_of" in body
    assert "summary" in body
    assert "source_snapshot" in body
    assert body["source_snapshot"]["snapshot_time"] == "2026-04-10 08:00:00"
    assert "rows" in body


def test_crash_risk_service_returns_expected_top_level_keys():
    from app.services.crash_risk_service import CrashRiskService

    service = CrashRiskService()
    body = service.build_empty_response()
    assert sorted(body.keys()) == ["as_of", "rows", "summary"]


def test_crash_risk_uses_latest_leaderboard_snapshot_only(monkeypatch):
    from app.services.crash_risk_service import CrashRiskService

    calls = []

    class FakeCrashRiskRepository:
        def __init__(self, db):
            self.db = db

        def get_latest_leaderboard_snapshot(self):
            calls.append(self.db)
            return {"snapshot_date": "2026-04-09", "rows": [{"symbol": "BTCUSDT"}]}

    monkeypatch.setattr("app.services.crash_risk_service.CrashRiskRepository", FakeCrashRiskRepository)
    monkeypatch.setattr(
        "app.services.crash_risk_service.CrashRiskService.fetch_symbol_inputs",
        lambda self, symbol: {
            "closes": [100.0, 105.0],
            "highs": [101.0, 106.0],
            "lows": [99.0, 100.0],
            "volumes": [10.0, 12.0],
            "open_interests": [1000.0, 980.0],
        },
    )

    service = CrashRiskService()
    payload = service.build_from_leaderboard_snapshot(object())

    assert payload["as_of"] == "2026-04-09"
    assert payload["rows"]
    assert calls


def test_crash_risk_candidate_pool_is_union_of_leaderboard_and_rebound_snapshots(monkeypatch):
    from app.services.crash_risk_service import CrashRiskService

    class FakeCrashRiskRepository:
        def __init__(self, db):
            self.db = db

        def get_candidate_symbols_snapshot_union(self):
            return {
                "snapshot_date": "2026-04-10",
                "snapshot_time": "2026-04-10 08:00:00",
                "window_start_utc": "2026-04-09 16:00:00",
                "source": "snapshot_union",
                "symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BTCUSDT", "DOGEUSDT"],
            }

    seen_symbols = []

    def fake_fetch_symbol_inputs(self, symbol):
        seen_symbols.append(symbol)
        return {
            "closes": [100.0, 105.0],
            "highs": [101.0, 106.0],
            "lows": [99.0, 100.0],
            "volumes": [10.0, 12.0],
            "open_interests": [1000.0, 980.0],
        }

    monkeypatch.setattr("app.services.crash_risk_service.CrashRiskRepository", FakeCrashRiskRepository)
    monkeypatch.setattr("app.services.crash_risk_service.CrashRiskService.fetch_symbol_inputs", fake_fetch_symbol_inputs)

    payload = CrashRiskService().build_from_leaderboard_snapshot(object())

    assert sorted(seen_symbols) == ["BTCUSDT", "DOGEUSDT", "ETHUSDT", "SOLUSDT"]
    assert sorted(row["symbol"] for row in payload["rows"]) == ["BTCUSDT", "DOGEUSDT", "ETHUSDT", "SOLUSDT"]


def test_crash_risk_api_uses_threaded_snapshot_lookup(client, monkeypatch):
    from app.api import crash_risk_api

    called = []

    async def fake_run_in_thread(func, *args, **kwargs):
        called.append(getattr(func, "__name__", "<callable>"))
        return {"as_of": None, "summary": {"total": 0, "high_risk": 0, "warning": 0, "watch": 0}, "rows": []}

    monkeypatch.setattr(crash_risk_api, "run_in_thread", fake_run_in_thread)

    response = client.get("/api/crash-risk")

    assert response.status_code == 200
    assert called


def test_crash_risk_refresh_api_uses_threaded_snapshot_lookup(client, monkeypatch):
    from app.api import crash_risk_api

    called = []

    async def fake_run_in_thread(func, *args, **kwargs):
        called.append(getattr(func, "__name__", "<callable>"))
        return {"as_of": None, "summary": {"total": 0, "high_risk": 0, "warning": 0, "watch": 0}, "rows": []}

    monkeypatch.setattr(crash_risk_api, "run_in_thread", fake_run_in_thread)

    response = client.post("/api/crash-risk/refresh")

    assert response.status_code in (200, 202)
    assert called
