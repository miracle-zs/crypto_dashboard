# Leaderboard Rebound Drawdown Fields Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add two drawdown fields for symbols shown in leaderboard gainers and rebound panels: drawdown from 7-day high and drawdown from the panel's own window high.

**Architecture:** Compute the fields when snapshots are built so the leaderboard page keeps using snapshot data rather than making extra live K-line requests on page load. Reuse daily klines already fetched per symbol, attach the two drawdown fields into stored snapshot rows, and render them in leaderboard/rebound tables and mobile cards.

**Tech Stack:** FastAPI, repository-backed snapshot storage, vanilla JavaScript frontend, pytest.

---

### Task 1: Add failing backend tests for drawdown calculation

**Files:**
- Modify: `tests/test_market_snapshot_service_cache.py`
- Modify: `app/services/market_snapshot_service.py`

**Step 1: Write the failing test**

Add tests that build leaderboard and rebound snapshots from deterministic fake klines and assert:
- `drawdown_from_7d_high_pct`
- `drawdown_from_window_high_pct`

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_market_snapshot_service_cache.py -q`
Expected: FAIL because the new drawdown fields do not exist yet.

**Step 3: Write minimal implementation**

Add reusable helpers in `app/services/market_snapshot_service.py` to calculate:
- highest high over last 7 daily klines
- highest high over the requested window
- negative drawdown percent from current price

Populate those fields into leaderboard and rebound snapshot rows.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_market_snapshot_service_cache.py -q`
Expected: PASS

### Task 2: Expose and preserve the new fields through snapshot responses

**Files:**
- Modify: `tests/test_leaderboard_rebound_contract.py`
- Modify: `app/services/leaderboard_service.py`
- Modify: `app/services/rebound_service.py`

**Step 1: Write the failing test**

Extend contract tests to assert that when rows are present, the returned objects may carry:
- `drawdown_from_7d_high_pct`
- `drawdown_from_window_high_pct`

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_leaderboard_rebound_contract.py -q`
Expected: FAIL if the response enrichment path drops the new fields.

**Step 3: Write minimal implementation**

Ensure leaderboard/rebound response enrichment preserves the new row keys without renaming or stripping them.

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_leaderboard_rebound_contract.py -q`
Expected: PASS

### Task 3: Render the new drawdown metrics on the leaderboard page

**Files:**
- Modify: `templates/leaderboard.html`
- Modify: `static/js/leaderboard.js`

**Step 1: Write the failing test**

Add or update a regression test that verifies the leaderboard page assets still load and the script/template include labels for the new drawdown fields.

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_routes_regression.py -q`
Expected: FAIL if the new labels are not present.

**Step 3: Write minimal implementation**

Add display columns/details for the two drawdown fields in:
- `GAINERS TOP 10`
- `14D REBOUND TOP 10`
- `30D REBOUND TOP 10`
- `60D REBOUND TOP 10`
- `365D REBOUND TOP 10`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_routes_regression.py -q`
Expected: PASS

### Task 4: Verify the full targeted regression set

**Files:**
- Modify: `app/services/market_snapshot_service.py`
- Modify: `static/js/leaderboard.js`
- Modify: `templates/leaderboard.html`
- Modify: `tests/test_market_snapshot_service_cache.py`
- Modify: `tests/test_leaderboard_rebound_contract.py`
- Modify: `tests/test_routes_regression.py`

**Step 1: Run targeted verification**

Run: `pytest tests/test_market_snapshot_service_cache.py tests/test_leaderboard_rebound_contract.py tests/test_routes_regression.py -q`

**Step 2: Inspect output**

Confirm all targeted tests pass and no response-contract regression appears.
