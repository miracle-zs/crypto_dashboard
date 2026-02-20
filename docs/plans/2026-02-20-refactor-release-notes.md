# Release Notes: Zero-Behavior Refactor

Date: 2026-02-20  
Branch: `main`

## Summary

This release refactors the application into layered modules without changing external API/page/scheduler behavior.  
Core goal: improve maintainability while preserving runtime behavior.

## What Changed

1. Test runtime stabilization
- Ensured local test runtime dependency path is explicit.
- Added TDD runtime readiness check.

2. Shared core utilities
- Added `app/core/deps.py` for common dependencies.
- Added `app/core/time.py` for shared UTC+8 time utility.

3. Leaderboard service extraction
- Moved leaderboard aggregation logic from `main.py` into `app/services/leaderboard_service.py`.
- Added dedicated router: `app/api/leaderboard_api.py`.

4. Open positions service extraction
- Moved open-positions aggregation logic into `app/services/positions_service.py`.
- Added dedicated router: `app/api/positions_api.py`.

5. Repository facade introduction
- Added compatibility facades under `app/repositories/` over existing `Database` methods.
- Updated services to call repositories instead of direct database method usage.

6. Scheduler job extraction
- Added `app/jobs/noon_loss_job.py` and `app/jobs/sync_jobs.py`.
- Kept scheduler registration and job IDs stable in `app/scheduler.py`.

7. API router migration completion
- Moved remaining APIs out of `main.py` into:
  - `app/api/system_api.py`
  - `app/api/trades_api.py`
  - `app/api/watchnotes_api.py`
- Slimmed app entrypoint while preserving routes.

8. Final verification artifacts
- Added verification report: `docs/plans/2026-02-20-refactor-verification-report.md`.
- Updated README with new layered architecture section.

## Compatibility Statement

- API endpoint paths: unchanged.
- Core response contract keys for leaderboard/open-positions: unchanged.
- Write endpoint token protections: unchanged.
- Scheduler job IDs used by runtime/ops checks: unchanged.

## Verification

- Full suite passed on `main`:
  - `conda run -n base python -m pytest -q`
  - Result: `23 passed`

- Detailed evidence and parity checklist:
  - `docs/plans/2026-02-20-refactor-verification-report.md`
