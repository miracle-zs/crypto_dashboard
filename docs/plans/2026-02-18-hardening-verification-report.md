# Hardening Verification Report (2026-02-18)

## Scope
- Plan source: `docs/plans/2026-02-18-one-week-hardening-plan.md`
- Branch: `codex/one-week-hardening`
- Verification date: 2026-02-18

## Verification Results

### 1) Test suite
- Command: `conda run -n base python -m pytest -q`
- Result: PASS
- Summary: `10 passed, 6 warnings in 1.13s`

### 2) App smoke check
- Planned command: `uvicorn app.main:app --reload --host 0.0.0.0 --port 8000`
- Environment outcome:
  - `--reload` mode failed with `Operation not permitted` (sandbox restriction on file watching).
  - Non-reload mode started app lifecycle, but bind failed with `operation not permitted` for `127.0.0.1:8000` (sandbox blocks listening socket).
- Conclusion: runtime route/static HTTP smoke could not be completed in this sandbox.

### 3) Scheduler startup behavior (log-level smoke evidence)
- Observed startup/shutdown logs during uvicorn attempt:
  - Scheduler startup log present when guard allows start.
  - Scheduler shutdown log present on app shutdown.
- Multi-worker and missing-key startup guards are covered by unit tests:
  - `tests/test_scheduler_guards.py` passing.

## Implemented Hardening Items
1. Baseline test harness and API/static smoke tests.
2. Static assets mounted at `/static`.
3. DB dependency moved to generator lifecycle and service injection improved.
4. Routes split into domain routers (`system`, `trades`, `leaderboard`) with regression tests.
5. Scheduler startup guards for missing API keys and multi-worker deployments.
6. Write endpoint token guard (`DASHBOARD_ADMIN_TOKEN` + `X-Admin-Token`).

## Known Risks
1. FastAPI `on_event` deprecation warnings remain (no lifespan migration in this cycle).
2. Sandbox constraints prevented full local HTTP smoke verification.
3. Test runs still emit scheduler/network side effects in startup logs when API keys exist.

## Rollback Notes
- Commits are split by task/day and can be reverted independently:
  - `a7ddf41` Task 1
  - `30f42a5` Task 2
  - `0e8cf76` Task 3
  - `9fd5201` Task 4
  - `0d2581f` Task 5
  - `c33c5ca` Task 6
- Revert only the newest problematic task commit to minimize blast radius.
