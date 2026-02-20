# Performance Refactor Verification Report

**Date:** 2026-02-21  
**Plan:** `docs/plans/2026-02-20-performance-refactor-implementation-plan.md`  
**Branch:** `codex/perf-refactor-exec`

## 1. Verification Commands

### 1.1 Full test suite

Command:

```bash
conda run -n base python -m pytest -q tests
```

Result: `41 passed, 2 warnings`

### 1.2 Smoke startup

Command:

```bash
conda run -n base uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Observed:

1. `Application startup complete.` appears in logs.
2. Process then exits with bind error in sandbox: `error while attempting to bind on address ('0.0.0.0', 8000): operation not permitted`.

Conclusion: application startup path is healthy; sandbox blocks port binding in current environment.

## 2. Performance Summary (Baseline vs Post)

Benchmark date: 2026-02-21  
Method: identical synthetic harnesses executed on baseline (`main`) and post-refactor (`codex/perf-refactor-exec`).

| Metric | Baseline | Post-refactor | Change |
|---|---:|---:|---:|
| API P95 (`/api/open-positions`) | 3.975 ms | 1.421 ms | -64.25% |
| Scheduler job elapsed P95 (`sync_trades_data`, mocked IO) | 5.319 ms | 5.988 ms | +12.58% |
| Hot query P95 (`get_open_positions`, 8k rows) | 40.564 ms | 39.603 ms | -2.37% |

Notes:

1. API benchmark uses TestClient + dependency overrides + fixed dataset (40 runs after warmup).
2. Scheduler benchmark uses mocked processor/db to isolate orchestration overhead (30 runs).
3. Hot query benchmark uses temp SQLite with 8,000 `open_positions` rows (80 runs).

## 3. Rollout Notes

1. Keep `API_JOB_LOCK_WAIT_SECONDS` at default `8` initially.
2. Keep short TTL defaults for API caches:
   - `POSITIONS_CACHE_TTL_SECONDS=3`
   - `LEADERBOARD_CACHE_TTL_SECONDS=10`
   - `REBOUND_CACHE_TTL_SECONDS=10`
3. Enable query-plan logging only during diagnostics:
   - `DB_QUERY_PLAN_DEBUG=1`

## 4. Known Risks

1. Scheduler synthetic P95 regressed (+12.58%); likely added runtime-control/instrumentation overhead.
2. Sandbox/network restrictions can cause scheduler startup jobs to log external API resolution failures during smoke checks.
3. `sync_trades_data` metric log currently reports `status=success` for some internal error paths (status propagation should be tightened in a follow-up patch).

## 5. Rollback Steps

1. Revert to the pre-refactor branch/commit.
2. Disable incremental polling usage by removing `since_version` polling param on frontend if needed.
3. Disable all new cache TTLs by setting values to `0` or reverting service changes.
4. Re-run:

```bash
conda run -n base python -m pytest -q tests
```

5. Validate startup behavior again:

```bash
conda run -n base uvicorn app.main:app --host 0.0.0.0 --port 8000
```
