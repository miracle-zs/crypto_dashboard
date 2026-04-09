# Crash Risk Page Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a standalone `/crash-risk` page that scores only the latest `leaderboard` symbols for structural crash risk and explains the drivers behind each score.

**Architecture:** Reuse the existing leaderboard snapshot pipeline as the candidate source, add a dedicated `crash_risk` service/API/template flow, and keep the first version rule-based and manually refreshable. The scoring logic stays in a focused service so thresholds and drivers remain explainable and easy to iterate.

**Tech Stack:** Python 3.11+, FastAPI, existing Binance client integrations, Jinja2 templates, Vanilla JS, Pytest

---

### Task 1: Lock the API response contract

**Files:**
- Create: `tests/test_crash_risk_contract.py`
- Modify: `tests/test_main_thin_routes.py`

**Step 1: Write the failing test**

```python
def test_crash_risk_contract_shape(client):
    response = client.get("/api/crash-risk")
    assert response.status_code == 200
    body = response.json()
    assert "as_of" in body
    assert "summary" in body
    assert "rows" in body
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_crash_risk_contract.py::test_crash_risk_contract_shape -v`  
Expected: FAIL because the route does not exist yet.

**Step 3: Write minimal implementation**

```python
@router.get("/api/crash-risk")
async def get_crash_risk():
    return {"as_of": None, "summary": {}, "rows": []}
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_crash_risk_contract.py::test_crash_risk_contract_shape -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add tests/test_crash_risk_contract.py tests/test_main_thin_routes.py app/api/crash_risk_api.py app/main.py
git commit -m "test: add crash risk API contract scaffold"
```

### Task 2: Add candidate-source and scoring service scaffolding

**Files:**
- Create: `app/services/crash_risk_service.py`
- Modify: `app/services/__init__.py`
- Test: `tests/test_crash_risk_contract.py`

**Step 1: Write the failing test**

```python
from app.services.crash_risk_service import CrashRiskService

def test_crash_risk_service_returns_expected_top_level_keys():
    service = CrashRiskService()
    body = service.build_empty_response()
    assert sorted(body.keys()) == ["as_of", "rows", "source_snapshot", "summary"]
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_crash_risk_contract.py::test_crash_risk_service_returns_expected_top_level_keys -v`  
Expected: FAIL because the service module does not exist yet.

**Step 3: Write minimal implementation**

```python
class CrashRiskService:
    def build_empty_response(self):
        return {
            "as_of": None,
            "source_snapshot": None,
            "summary": {"total": 0, "high_risk": 0, "warning": 0, "watch": 0},
            "rows": [],
        }
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_crash_risk_contract.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/services/crash_risk_service.py app/services/__init__.py tests/test_crash_risk_contract.py
git commit -m "feat: add crash risk service scaffold"
```

### Task 3: Read latest leaderboard symbols as the only candidate pool

**Files:**
- Create: `app/repositories/crash_risk_repository.py`
- Modify: `app/services/crash_risk_service.py`
- Test: `tests/test_crash_risk_contract.py`

**Step 1: Write the failing test**

```python
def test_crash_risk_uses_latest_leaderboard_symbols_only(fake_db):
    service = CrashRiskService()
    payload = service.build_from_leaderboard_snapshot(fake_db)
    assert payload["source_snapshot"] is not None
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_crash_risk_contract.py::test_crash_risk_uses_latest_leaderboard_symbols_only -v`  
Expected: FAIL because snapshot loading is not wired.

**Step 3: Write minimal implementation**

```python
class CrashRiskRepository:
    def __init__(self, db):
        self.db = db

    def get_latest_leaderboard_snapshot(self):
        return self.db.get_latest_leaderboard_snapshot()
```

```python
def build_from_leaderboard_snapshot(self, db):
    snapshot = CrashRiskRepository(db).get_latest_leaderboard_snapshot()
    ...
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_crash_risk_contract.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/repositories/crash_risk_repository.py app/services/crash_risk_service.py tests/test_crash_risk_contract.py
git commit -m "feat: source crash risk candidates from latest leaderboard snapshot"
```

### Task 4: Implement rule-based component scoring

**Files:**
- Modify: `app/services/crash_risk_service.py`
- Create: `tests/test_crash_risk_scoring.py`

**Step 1: Write the failing test**

```python
def test_crash_risk_scores_include_component_breakdown():
    row = score_symbol(example_market_series)
    assert set(row["component_scores"]) == {
        "oi_divergence",
        "price_extension",
        "momentum_fade",
        "support_fragility",
        "trigger_confirmation",
    }
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_crash_risk_scoring.py::test_crash_risk_scores_include_component_breakdown -v`  
Expected: FAIL because the scoring helper does not exist.

**Step 3: Write minimal implementation**

```python
def score_symbol(series):
    return {
        "risk_score": ...,
        "stage": ...,
        "drivers": [...],
        "component_scores": {...},
    }
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_crash_risk_scoring.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/services/crash_risk_service.py tests/test_crash_risk_scoring.py
git commit -m "feat: implement explainable crash risk scoring"
```

### Task 5: Add market-data integration for 1H price, OI, and volume inputs

**Files:**
- Modify: `app/binance_client.py`
- Modify: `app/services/crash_risk_service.py`
- Test: `tests/test_crash_risk_scoring.py`

**Step 1: Write the failing test**

```python
def test_crash_risk_service_requests_required_market_inputs(fake_binance_client):
    service = CrashRiskService(binance_client=fake_binance_client)
    service.fetch_symbol_inputs("BULLAUSDT")
    assert fake_binance_client.requested_endpoints == ["klines_1h", "open_interest_hist_1h"]
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_crash_risk_scoring.py::test_crash_risk_service_requests_required_market_inputs -v`  
Expected: FAIL because market-input fetching is incomplete.

**Step 3: Write minimal implementation**

```python
def fetch_symbol_inputs(self, symbol):
    klines = self.binance_client.get_futures_klines(symbol=symbol, interval="1h", limit=...)
    oi = self.binance_client.get_open_interest_hist(symbol=symbol, period="1h", limit=...)
    return {"klines": klines, "open_interest": oi}
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_crash_risk_scoring.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/binance_client.py app/services/crash_risk_service.py tests/test_crash_risk_scoring.py
git commit -m "feat: fetch crash risk market inputs from binance client"
```

### Task 6: Wire the dedicated API endpoints

**Files:**
- Create: `app/api/crash_risk_api.py`
- Modify: `app/main.py`
- Test: `tests/test_crash_risk_contract.py`

**Step 1: Write the failing test**

```python
def test_crash_risk_refresh_endpoint_exists(client):
    response = client.post("/api/crash-risk/refresh")
    assert response.status_code in {200, 202}
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_crash_risk_contract.py::test_crash_risk_refresh_endpoint_exists -v`  
Expected: FAIL because the refresh route does not exist yet.

**Step 3: Write minimal implementation**

```python
@router.post("/api/crash-risk/refresh")
async def refresh_crash_risk(...):
    return service.refresh(...)
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_crash_risk_contract.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/api/crash_risk_api.py app/main.py tests/test_crash_risk_contract.py
git commit -m "feat: add crash risk API endpoints"
```

### Task 7: Build the standalone page and client-side rendering

**Files:**
- Create: `templates/crash_risk.html`
- Create: `static/js/crash-risk.js`
- Modify: `templates/index.html`
- Test: `tests/test_health_and_static.py`

**Step 1: Write the failing test**

```python
def test_crash_risk_page_renders(client):
    response = client.get("/crash-risk")
    assert response.status_code == 200
    assert "结构性崩盘预警" in response.text
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_health_and_static.py::test_crash_risk_page_renders -v`  
Expected: FAIL because the page does not exist yet.

**Step 3: Write minimal implementation**

```html
<section id="crash-risk-app">
  <h1>结构性崩盘预警</h1>
</section>
```

```javascript
async function fetchCrashRisk() {
  const response = await fetch('/api/crash-risk');
  return response.json();
}
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_health_and_static.py::test_crash_risk_page_renders -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add templates/crash_risk.html static/js/crash-risk.js templates/index.html tests/test_health_and_static.py
git commit -m "feat: add standalone crash risk page"
```

### Task 8: Add row expansion and explanation rendering

**Files:**
- Modify: `templates/crash_risk.html`
- Modify: `static/js/crash-risk.js`
- Test: `tests/test_crash_risk_contract.py`

**Step 1: Write the failing test**

```python
def test_crash_risk_rows_include_drivers_and_component_scores(client):
    body = client.get("/api/crash-risk").json()
    if body["rows"]:
        row = body["rows"][0]
        assert "drivers" in row
        assert "component_scores" in row
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_crash_risk_contract.py::test_crash_risk_rows_include_drivers_and_component_scores -v`  
Expected: FAIL until explanation fields are returned.

**Step 3: Write minimal implementation**

```javascript
function renderDrivers(drivers) {
  return drivers.map((item) => `<span class="driver-pill">${item}</span>`).join('');
}
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_crash_risk_contract.py tests/test_health_and_static.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add templates/crash_risk.html static/js/crash-risk.js app/services/crash_risk_service.py tests/test_crash_risk_contract.py
git commit -m "feat: render crash risk drivers and score breakdown"
```

### Task 9: Add manual refresh flow and loading-state coverage

**Files:**
- Modify: `app/api/crash_risk_api.py`
- Modify: `static/js/crash-risk.js`
- Modify: `templates/crash_risk.html`
- Test: `tests/test_crash_risk_contract.py`

**Step 1: Write the failing test**

```python
def test_crash_risk_refresh_returns_latest_payload_shape(client):
    response = client.post("/api/crash-risk/refresh")
    assert response.status_code in {200, 202}
    body = response.json()
    assert "as_of" in body
    assert "rows" in body
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_crash_risk_contract.py::test_crash_risk_refresh_returns_latest_payload_shape -v`  
Expected: FAIL until refresh returns the full payload.

**Step 3: Write minimal implementation**

```python
@router.post("/api/crash-risk/refresh")
async def refresh_crash_risk(...):
    return service.refresh_latest(...)
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_crash_risk_contract.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/api/crash_risk_api.py static/js/crash-risk.js templates/crash_risk.html tests/test_crash_risk_contract.py
git commit -m "feat: support manual crash risk refresh"
```

### Task 10: Verify end-to-end behavior and update docs

**Files:**
- Modify: `README.md`
- Modify: `docs/data_dictionary.md`
- Modify: `docs/data_flow.md`
- Test: `tests/test_crash_risk_contract.py`
- Test: `tests/test_health_and_static.py`
- Test: `tests/test_main_thin_routes.py`

**Step 1: Write the failing doc/test assertion**

```python
def test_crash_risk_route_registered(client):
    assert client.get("/crash-risk").status_code == 200
```

**Step 2: Run verification suite**

Run: `python3 -m pytest tests/test_crash_risk_contract.py tests/test_health_and_static.py tests/test_main_thin_routes.py -v`  
Expected: FAIL until docs/routes are fully wired.

**Step 3: Write minimal implementation**

```markdown
- `/crash-risk`：结构性崩盘预警页
- `/api/crash-risk`：崩盘风险榜单接口
```

**Step 4: Run verification suite**

Run: `python3 -m pytest tests/test_crash_risk_contract.py tests/test_health_and_static.py tests/test_main_thin_routes.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add README.md docs/data_dictionary.md docs/data_flow.md tests/test_crash_risk_contract.py tests/test_health_and_static.py tests/test_main_thin_routes.py
git commit -m "docs: document crash risk page and data flow"
```
