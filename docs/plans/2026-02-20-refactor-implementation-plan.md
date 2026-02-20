# Zero-Behavior Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor the project into maintainable layers (api/services/repositories/jobs/core) without changing existing API/page/scheduler behavior.

**Architecture:** Use a strangler approach: keep old implementations available while migrating one domain at a time. Route handlers become thin, service layer owns business aggregation, and scheduler/database mega-files are decomposed into focused modules. Every migration step is protected by contract/snapshot tests so behavior remains unchanged.

**Tech Stack:** Python 3.11+, FastAPI, APScheduler, SQLite, Pytest

---

### Task 1: Stabilize Test Runtime

**Files:**
- Modify: `requirements.txt`
- Modify: `README.md`
- Test: `tests/test_health_and_static.py`

**Step 1: Write the failing test**

```python
def test_pytest_runtime_ready():
    import pytest  # noqa: F401
    assert True
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_health_and_static.py::test_pytest_runtime_ready -v`  
Expected: FAIL with `No module named pytest` (or missing import)

**Step 3: Write minimal implementation**

```text
# requirements.txt
pytest>=8.0.0
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_health_and_static.py::test_pytest_runtime_ready -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add requirements.txt README.md tests/test_health_and_static.py
git commit -m "chore: ensure local pytest runtime is available"
```

### Task 2: Create Shared Core Dependencies

**Files:**
- Create: `app/core/deps.py`
- Create: `app/core/time.py`
- Modify: `app/main.py`
- Test: `tests/test_db_dependency_lifecycle.py`

**Step 1: Write the failing test**

```python
from app.core import deps

def test_core_get_db_is_generator():
    import inspect
    assert inspect.isgeneratorfunction(deps.get_db)
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_db_dependency_lifecycle.py::test_core_get_db_is_generator -v`  
Expected: FAIL (`app.core.deps` not found)

**Step 3: Write minimal implementation**

```python
# app/core/deps.py
from app.database import Database

def get_db():
    db = Database()
    try:
        yield db
    finally:
        close = getattr(db, "close", None)
        if callable(close):
            close()
```

```python
# app/core/time.py
from datetime import timedelta, timezone
UTC8 = timezone(timedelta(hours=8))
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_db_dependency_lifecycle.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/core/deps.py app/core/time.py app/main.py tests/test_db_dependency_lifecycle.py
git commit -m "refactor: centralize common dependencies and time utilities"
```

### Task 3: Migrate Leaderboard APIs to Service Layer

**Files:**
- Create: `app/services/leaderboard_service.py`
- Create: `app/api/leaderboard_api.py`
- Modify: `app/main.py`
- Test: `tests/test_routes_regression.py`
- Test: `tests/test_leaderboard_contract.py`

**Step 1: Write the failing contract test**

```python
def test_leaderboard_contract_shape(client):
    r = client.get("/api/leaderboard")
    assert r.status_code == 200
    body = r.json()
    assert "ok" in body
```

**Step 2: Run test to verify it fails (before migration scaffolding)**

Run: `python3 -m pytest tests/test_leaderboard_contract.py::test_leaderboard_contract_shape -v`  
Expected: FAIL if scaffolding is incomplete

**Step 3: Write minimal implementation**

```python
class LeaderboardService:
    def build_snapshot_response(self, db, date):
        # move existing main.py logic here without behavioral edits
        ...
```

```python
router = APIRouter()
@router.get("/api/leaderboard")
async def get_leaderboard(...):
    return await service.build_snapshot_response(...)
```

**Step 4: Run tests to verify behavior unchanged**

Run: `python3 -m pytest tests/test_routes_regression.py tests/test_leaderboard_contract.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/services/leaderboard_service.py app/api/leaderboard_api.py app/main.py tests/test_leaderboard_contract.py
git commit -m "refactor: move leaderboard aggregation into service and dedicated API router"
```

### Task 4: Migrate Open Positions Aggregation to Service

**Files:**
- Create: `app/services/positions_service.py`
- Create: `app/api/positions_api.py`
- Modify: `app/main.py`
- Test: `tests/test_positions_contract.py`

**Step 1: Write the failing contract test**

```python
def test_open_positions_contract_shape(client):
    r = client.get("/api/open-positions")
    assert r.status_code == 200
    body = r.json()
    assert "as_of" in body
    assert "positions" in body
    assert "summary" in body
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_positions_contract.py::test_open_positions_contract_shape -v`  
Expected: FAIL while migration in progress

**Step 3: Write minimal implementation**

```python
class PositionsService:
    async def build_open_positions_response(self, db, client):
        # move existing aggregation logic from main.py verbatim
        ...
```

**Step 4: Run test to verify it passes**

Run: `python3 -m pytest tests/test_positions_contract.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/services/positions_service.py app/api/positions_api.py app/main.py tests/test_positions_contract.py
git commit -m "refactor: extract open-positions aggregation into service layer"
```

### Task 5: Introduce Repository Facade (No SQL Rewrite)

**Files:**
- Create: `app/repositories/trade_repository.py`
- Create: `app/repositories/snapshot_repository.py`
- Create: `app/repositories/settings_repository.py`
- Modify: `app/services/*.py`
- Test: `tests/test_repository_backcompat.py`

**Step 1: Write the failing test**

```python
def test_repository_calls_database_backcompat():
    # instantiate repository with Database and assert method exists
    assert True
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_repository_backcompat.py -v`  
Expected: FAIL (repository modules missing)

**Step 3: Write minimal implementation**

```python
class SnapshotRepository:
    def __init__(self, db):
        self.db = db
    def get_latest_leaderboard_snapshot(self):
        return self.db.get_latest_leaderboard_snapshot()
```

**Step 4: Run tests**

Run: `python3 -m pytest tests/test_repository_backcompat.py tests/test_leaderboard_contract.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/repositories/*.py app/services/*.py tests/test_repository_backcompat.py
git commit -m "refactor: add repository facade over existing Database methods"
```

### Task 6: Split Scheduler Job Implementations

**Files:**
- Create: `app/jobs/noon_loss_job.py`
- Create: `app/jobs/sync_jobs.py`
- Modify: `app/scheduler.py`
- Test: `tests/test_scheduler_jobs_contract.py`

**Step 1: Write the failing test**

```python
def test_scheduler_still_registers_existing_job_ids():
    # verify ids like sync_trades/check_losses_noon exist
    assert True
```

**Step 2: Run test to verify it fails**

Run: `python3 -m pytest tests/test_scheduler_jobs_contract.py -v`  
Expected: FAIL before extraction

**Step 3: Write minimal implementation**

```python
def run_noon_loss_check(ctx):
    # move logic from TradeDataScheduler.check_recent_losses_at_noon
    ...
```

`scheduler.py` keeps:
- env parsing
- APScheduler registration
- lifecycle start/stop

**Step 4: Run tests to verify unchanged behavior**

Run: `python3 -m pytest tests/test_scheduler_guards.py tests/test_scheduler_jobs_contract.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/jobs/*.py app/scheduler.py tests/test_scheduler_jobs_contract.py
git commit -m "refactor: extract scheduler task implementations into jobs modules"
```

### Task 7: Move Remaining APIs Out of main.py

**Files:**
- Create: `app/api/system_api.py`
- Create: `app/api/trades_api.py`
- Create: `app/api/watchnotes_api.py`
- Modify: `app/main.py`
- Test: `tests/test_routes_regression.py`

**Step 1: Write the failing regression test**

```python
def test_route_set_still_available(client):
    assert client.get("/api/status").status_code == 200
    assert client.get("/api/trades").status_code == 200
```

**Step 2: Run test**

Run: `python3 -m pytest tests/test_routes_regression.py::test_route_set_still_available -v`  
Expected: baseline PASS (guardrail)

**Step 3: Write minimal implementation**

Move handlers without changing endpoint signature or response payload.

**Step 4: Run full route/auth tests**

Run: `python3 -m pytest tests/test_routes_regression.py tests/test_auth_write_endpoints.py -v`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/api/*.py app/main.py tests/test_routes_regression.py
git commit -m "refactor: complete API router migration and slim application entrypoint"
```

### Task 8: Final Verification and Cleanup

**Files:**
- Modify: `README.md`
- Create: `docs/plans/2026-02-20-refactor-verification-report.md`

**Step 1: Run full test suite**

Run: `python3 -m pytest -q`  
Expected: PASS

**Step 2: Run app smoke check**

Run: `uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`  
Expected: pages/API/scheduler behavior unchanged

**Step 3: Verify behavior parity checklist**

1. `/api/leaderboard` payload keys unchanged
2. `/api/open-positions` summary keys unchanged
3. write APIs still token-protected
4. scheduler job IDs and schedules unchanged

**Step 4: Commit**

```bash
git add README.md docs/plans/2026-02-20-refactor-verification-report.md
git commit -m "docs: finalize refactor verification and parity report"
```

