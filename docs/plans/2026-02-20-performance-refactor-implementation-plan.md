# Performance-First Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Perform a full, performance-first refactor that improves API latency, scheduler throughput, DB query efficiency, and live-monitor responsiveness while preserving operational correctness.

**Architecture:** Execute in four phases with strict TDD and verification gates. Introduce observability first, then refactor scheduler concurrency controls, then split database responsibilities and optimize hot queries, and finally optimize API caching plus frontend incremental loading.

**Tech Stack:** Python 3.11+, FastAPI, APScheduler, SQLite, Pytest

---

### Task 1: Establish Performance Baseline Instrumentation

**Files:**
- Create: `app/core/metrics.py`
- Modify: `app/main.py`
- Modify: `app/scheduler.py`
- Test: `tests/test_performance_metrics_hooks.py`

**Step 1: Write the failing test**

```python
def test_metrics_timer_records_elapsed_ms():
    from app.core.metrics import measure_ms

    with measure_ms("unit-test-metric") as snapshot:
        pass

    assert snapshot.elapsed_ms >= 0
```

**Step 2: Run test to verify it fails**

Run: `conda run -n base python -m pytest -q tests/test_performance_metrics_hooks.py`  
Expected: FAIL with `ModuleNotFoundError: app.core.metrics`

**Step 3: Write minimal implementation**

1. Add `measure_ms` context manager with elapsed time recording.
2. Add lightweight metric logging helpers for API and job scopes.
3. Wrap selected API endpoints and scheduler entry points with timers.

**Step 4: Run test to verify it passes**

Run: `conda run -n base python -m pytest -q tests/test_performance_metrics_hooks.py`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/core/metrics.py app/main.py app/scheduler.py tests/test_performance_metrics_hooks.py
git commit -m "perf: add unified performance instrumentation hooks"
```

### Task 2: Extract Scheduler Runtime Control Plane

**Files:**
- Create: `app/core/job_runtime.py`
- Modify: `app/scheduler.py`
- Modify: `app/core/scheduler_config.py`
- Test: `tests/test_scheduler_runtime_control.py`

**Step 1: Write the failing test**

```python
def test_job_runtime_lock_timeout_returns_false():
    from app.core.job_runtime import JobRuntimeController

    ctl = JobRuntimeController(lock_wait_seconds=0)
    assert ctl.try_acquire("unit") is True
```

**Step 2: Run test to verify it fails**

Run: `conda run -n base python -m pytest -q tests/test_scheduler_runtime_control.py`  
Expected: FAIL because `job_runtime` module does not exist

**Step 3: Write minimal implementation**

1. Implement `JobRuntimeController` with lock acquire/release, cooldown guard, and budget helper.
2. Replace direct lock/cooldown code in `scheduler.py` with controller calls.
3. Keep existing env semantics and defaults unchanged.

**Step 4: Run test to verify it passes**

Run: `conda run -n base python -m pytest -q tests/test_scheduler_runtime_control.py tests/test_scheduler_jobs_contract.py`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/core/job_runtime.py app/core/scheduler_config.py app/scheduler.py tests/test_scheduler_runtime_control.py
git commit -m "refactor: extract scheduler runtime control plane"
```

### Task 3: Split Database Into Domain Repositories (Backward-Compatible)

**Files:**
- Create: `app/repositories/sync_repository.py`
- Create: `app/repositories/risk_repository.py`
- Modify: `app/repositories/__init__.py`
- Modify: `app/services/*`
- Modify: `app/scheduler.py`
- Test: `tests/test_repository_backcompat.py`
- Test: `tests/test_repository_usage_rules.py`

**Step 1: Write the failing test**

```python
def test_sync_repository_exposes_existing_sync_methods():
    from app.repositories.sync_repository import SyncRepository
    repo = SyncRepository(db=None)
    assert hasattr(repo, "get_last_entry_time")
```

**Step 2: Run test to verify it fails**

Run: `conda run -n base python -m pytest -q tests/test_repository_backcompat.py`  
Expected: FAIL because new repositories are missing

**Step 3: Write minimal implementation**

1. Add repository facades that delegate to existing `Database` methods.
2. Migrate hot service/scheduler code paths to repository calls.
3. Preserve database schema and return contracts.

**Step 4: Run tests to verify it passes**

Run: `conda run -n base python -m pytest -q tests/test_repository_backcompat.py tests/test_repository_usage_rules.py`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/repositories app/services app/scheduler.py tests/test_repository_backcompat.py tests/test_repository_usage_rules.py
git commit -m "refactor: split sync and risk database access into repositories"
```

### Task 4: Optimize Hot Queries and Add Index Migrations

**Files:**
- Modify: `app/database.py`
- Modify: `app/repositories/*`
- Create: `tests/test_query_hotpath_performance.py`

**Step 1: Write the failing test**

```python
def test_open_positions_hot_query_uses_index():
    assert False
```

**Step 2: Run test to verify it fails**

Run: `conda run -n base python -m pytest -q tests/test_query_hotpath_performance.py`  
Expected: FAIL

**Step 3: Write minimal implementation**

1. Add/adjust indexes for open positions, snapshots, sync_state hot filters.
2. Replace repetitive row-by-row queries with batch retrieval in hot APIs.
3. Add optional query-plan debug logging for hot endpoints.

**Step 4: Run tests to verify it passes**

Run: `conda run -n base python -m pytest -q tests/test_query_hotpath_performance.py tests/test_positions_contract.py tests/test_leaderboard_contract.py`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/database.py app/repositories tests/test_query_hotpath_performance.py
git commit -m "perf: optimize hot SQL paths and index coverage"
```

### Task 5: Introduce API Caching and Invalidation

**Files:**
- Create: `app/core/cache.py`
- Modify: `app/services/positions_service.py`
- Modify: `app/services/leaderboard_service.py`
- Modify: `app/services/rebound_service.py`
- Test: `tests/test_api_cache_behavior.py`

**Step 1: Write the failing test**

```python
def test_short_ttl_cache_hits_second_request():
    from app.core.cache import TTLCache
    c = TTLCache()
    c.set("k", 1, ttl_seconds=1)
    assert c.get("k") == 1
```

**Step 2: Run test to verify it fails**

Run: `conda run -n base python -m pytest -q tests/test_api_cache_behavior.py`  
Expected: FAIL because cache module is missing

**Step 3: Write minimal implementation**

1. Add in-memory TTL cache utility with thread-safe get/set/invalidate.
2. Cache responses for hot APIs with conservative TTL.
3. Add explicit invalidation after related writes/snapshot updates.

**Step 4: Run tests to verify it passes**

Run: `conda run -n base python -m pytest -q tests/test_api_cache_behavior.py tests/test_routes_regression.py`  
Expected: PASS

**Step 5: Commit**

```bash
git add app/core/cache.py app/services tests/test_api_cache_behavior.py
git commit -m "perf: add short-ttl API caching with targeted invalidation"
```

### Task 6: Optimize Live Monitor Data Loading

**Files:**
- Modify: `templates/live_monitor.html`
- Modify: `app/api/positions_api.py`
- Modify: `app/services/positions_service.py`
- Test: `tests/test_live_monitor_incremental_api.py`

**Step 1: Write the failing test**

```python
def test_open_positions_incremental_param_supported(client):
    r = client.get("/api/open-positions?since_version=1")
    assert r.status_code == 200
```

**Step 2: Run test to verify it fails**

Run: `conda run -n base python -m pytest -q tests/test_live_monitor_incremental_api.py`  
Expected: FAIL before incremental support exists

**Step 3: Write minimal implementation**

1. Add incremental parameter handling (`since_version` or equivalent cursor).
2. Return only changed sections when possible.
3. Update frontend polling logic to consume incremental payload safely.

**Step 4: Run tests to verify it passes**

Run: `conda run -n base python -m pytest -q tests/test_live_monitor_incremental_api.py tests/test_routes_regression.py`  
Expected: PASS

**Step 5: Commit**

```bash
git add templates/live_monitor.html app/api/positions_api.py app/services/positions_service.py tests/test_live_monitor_incremental_api.py
git commit -m "perf: add incremental live monitor data fetching"
```

### Task 7: Full Verification and Release Notes

**Files:**
- Create: `docs/plans/2026-02-20-performance-refactor-verification-report.md`
- Modify: `README.md`

**Step 1: Run full verification**

Run: `conda run -n base python -m pytest -q tests`  
Expected: All tests pass.

**Step 2: Run smoke server startup**

Run: `conda run -n base uvicorn app.main:app --host 0.0.0.0 --port 8000`  
Expected: app startup success log.

**Step 3: Capture performance before/after summary**

1. API P95 baseline vs post-refactor
2. Scheduler job elapsed baseline vs post-refactor
3. Hot query elapsed baseline vs post-refactor

**Step 4: Write verification report and update README performance notes**

Expected: clear rollout notes, known risks, rollback steps.

**Step 5: Commit**

```bash
git add docs/plans/2026-02-20-performance-refactor-verification-report.md README.md
git commit -m "docs: add performance refactor verification and rollout notes"
```

