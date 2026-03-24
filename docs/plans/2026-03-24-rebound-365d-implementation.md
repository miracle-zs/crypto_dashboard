# 365D Rebound Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a full 365D rebound snapshot flow across storage, scheduler, API, and leaderboard UI.

**Architecture:** Extend the existing fixed-window rebound implementation with a parallel 365D path. Keep the current 14D/30D/60D structure intact, add the missing 365D repository/config/scheduler/api/template/javascript hooks, and verify behavior with focused regression tests.

**Tech Stack:** FastAPI, Jinja2, vanilla JavaScript, SQLite, pytest

---

### Task 1: Lock 365D contracts with failing tests

**Files:**
- Modify: `tests/test_leaderboard_rebound_contract.py`
- Modify: `tests/test_api_response_models.py`
- Modify: `tests/test_main_thin_routes.py`
- Modify: `tests/test_repository_usage_rules.py`
- Modify: `tests/test_scheduler_config.py`

**Step 1: Write the failing test**

Add assertions that:
- `/api/rebound-365d` and `/api/rebound-365d/dates` exist in route/model contract tests
- repository-usage guard rails mention `365d`
- scheduler config exposes normalized `rebound_365d_*` values

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_leaderboard_rebound_contract.py tests/test_api_response_models.py tests/test_main_thin_routes.py tests/test_repository_usage_rules.py tests/test_scheduler_config.py -q`

Expected: FAIL because 365D route/config/repository symbols do not exist yet.

**Step 3: Write minimal implementation**

Add the smallest code necessary to expose the 365D surface area while preserving existing window behavior.

**Step 4: Run test to verify it passes**

Run the same `pytest` command and confirm it is green.

**Step 5: Commit**

```bash
git add docs/plans/2026-03-24-rebound-365d-implementation.md tests/test_leaderboard_rebound_contract.py tests/test_api_response_models.py tests/test_main_thin_routes.py tests/test_repository_usage_rules.py tests/test_scheduler_config.py
git commit -m "test: add 365d rebound contracts"
```

### Task 2: Implement 365D persistence and service wiring

**Files:**
- Modify: `app/core/db_migrations/v1_initial.py`
- Modify: `app/repositories/rebound_snapshot_repository.py`
- Modify: `app/repositories/snapshot_repository.py`
- Modify: `app/services/rebound_service.py`
- Modify: `app/api/rebound_api.py`

**Step 1: Write the failing test**

Use the contract tests from Task 1 as the failing coverage for missing 365D persistence/service hooks.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_leaderboard_rebound_contract.py tests/test_api_response_models.py tests/test_repository_usage_rules.py -q`

Expected: FAIL on missing 365D methods/routes.

**Step 3: Write minimal implementation**

Add:
- `rebound_365d_snapshots` table + indexes
- repository save/get/list methods
- snapshot facade passthroughs
- `ReboundService` getter/date/time maps for `365d`
- `/api/rebound-365d` and `/api/rebound-365d/dates`

**Step 4: Run test to verify it passes**

Run the same pytest command and confirm it is green.

**Step 5: Commit**

```bash
git add app/core/db_migrations/v1_initial.py app/repositories/rebound_snapshot_repository.py app/repositories/snapshot_repository.py app/services/rebound_service.py app/api/rebound_api.py
git commit -m "feat: add 365d rebound api and storage"
```

### Task 3: Implement 365D scheduler support

**Files:**
- Modify: `app/core/scheduler_config.py`
- Modify: `app/core/scheduler_binding.py`
- Modify: `app/scheduler.py`
- Modify: `app/jobs/scheduler_startup_jobs.py`

**Step 1: Write the failing test**

Use `tests/test_scheduler_config.py` to require default and normalized 365D scheduler fields.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_scheduler_config.py -q`

Expected: FAIL because `SchedulerConfig` lacks `rebound_365d_*`.

**Step 3: Write minimal implementation**

Add config fields and wire them through binding/startup/jobs with a default schedule after 60D.

**Step 4: Run test to verify it passes**

Run the same pytest command and confirm it is green.

**Step 5: Commit**

```bash
git add app/core/scheduler_config.py app/core/scheduler_binding.py app/scheduler.py app/jobs/scheduler_startup_jobs.py
git commit -m "feat: schedule 365d rebound snapshots"
```

### Task 4: Render 365D panel on leaderboard

**Files:**
- Modify: `templates/leaderboard.html`
- Modify: `static/js/leaderboard.js`
- Optionally Modify: `app/routes/leaderboard.py`

**Step 1: Write the failing test**

Use route/API contract tests plus a targeted regression pass on `/leaderboard`.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_routes_regression.py tests/test_leaderboard_rebound_contract.py -q`

Expected: contract tests are already green after backend work; UI route remains protected by the page regression.

**Step 3: Write minimal implementation**

Add a fourth rebound panel and include 365D in the frontend loader/render loop.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_routes_regression.py tests/test_leaderboard_rebound_contract.py -q`

Expected: PASS.

**Step 5: Commit**

```bash
git add templates/leaderboard.html static/js/leaderboard.js
git commit -m "feat: show 365d rebound leaderboard"
```

### Task 5: Final verification

**Files:**
- No code changes required

**Step 1: Run focused verification**

Run:
- `pytest tests/test_leaderboard_rebound_contract.py tests/test_api_response_models.py tests/test_main_thin_routes.py tests/test_repository_usage_rules.py tests/test_scheduler_config.py tests/test_routes_regression.py -q`

**Step 2: Run broader smoke verification**

Run:
- `pytest tests/test_leaderboard_contract.py tests/test_routes_regression.py -q`

**Step 3: Confirm result**

Record any residual risk if a full-suite run is skipped.
