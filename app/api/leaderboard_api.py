import asyncio
from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.deps import get_db
from app.repositories import SnapshotRepository
from app.services import LeaderboardService

router = APIRouter()
service = LeaderboardService()


@router.get("/api/leaderboard")
async def get_leaderboard_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db=Depends(get_db),
):
    return await service.build_snapshot_response(db=db, date=date)


@router.get("/api/leaderboard/dates")
async def get_leaderboard_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db=Depends(get_db),
):
    loop = asyncio.get_event_loop()
    snapshot_repo = SnapshotRepository(db)
    dates = await loop.run_in_executor(None, snapshot_repo.list_leaderboard_snapshot_dates, limit)
    return {"dates": dates}


@router.get("/api/leaderboard/metrics-history")
async def get_leaderboard_metrics_history(
    limit: int = Query(60, ge=1, le=365),
    db=Depends(get_db),
):
    loop = asyncio.get_event_loop()
    snapshot_repo = SnapshotRepository(db)
    dates = await loop.run_in_executor(None, snapshot_repo.list_leaderboard_snapshot_dates, limit)
    if not dates:
        return {"rows": []}

    metrics_map = await loop.run_in_executor(None, snapshot_repo.get_leaderboard_daily_metrics_by_dates, dates)
    missing_dates = [d for d in dates if d not in metrics_map]
    for d in missing_dates:
        payload = await loop.run_in_executor(None, snapshot_repo.upsert_leaderboard_daily_metrics_for_date, d)
        if payload:
            metrics_map[d] = payload

    rows = []
    for snapshot_date in dates:
        row = metrics_map.get(snapshot_date)
        if not row:
            continue
        metric1 = row.get("metric1", {}) or {}
        metric2 = row.get("metric2", {}) or {}
        metric3 = row.get("metric3", {}) or {}
        eval3 = int(metric3.get("evaluated_count") or 0)
        dist3 = metric3.get("distribution", {}) or {}
        lt_neg10 = int(dist3.get("lt_neg10") or 0)
        gt_pos10 = int(dist3.get("gt_pos10") or 0)
        lt_neg10_pct = round(lt_neg10 * 100.0 / eval3, 2) if eval3 > 0 else None
        gt_pos10_pct = round(gt_pos10 * 100.0 / eval3, 2) if eval3 > 0 else None

        rows.append(
            {
                "snapshot_date": snapshot_date,
                "metric1": metric1,
                "metric2": metric2,
                "metric3": metric3,
                "m1_prob_pct": metric1.get("probability_pct"),
                "m1_hits": metric1.get("hits"),
                "m2_prob_pct": metric2.get("probability_pct"),
                "m2_hits": metric2.get("hits"),
                "m3_eval_count": eval3,
                "m3_lt_neg10_pct": lt_neg10_pct,
                "m3_gt_pos10_pct": gt_pos10_pct,
            }
        )
    return {"rows": rows}
