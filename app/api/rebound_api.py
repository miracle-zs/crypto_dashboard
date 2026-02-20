import asyncio
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.deps import get_db
from app.core.time import UTC8
from app.repositories import SnapshotRepository, TradeRepository

router = APIRouter()


def _normalize_symbol(symbol: str) -> str:
    return symbol if symbol.endswith("USDT") else f"{symbol}USDT"


async def _get_rebound_snapshot_response(
    *,
    date: Optional[str],
    snapshot_repo: SnapshotRepository,
    trade_repo: TradeRepository,
    getter_by_date,
    getter_latest,
    empty_message: str,
):
    loop = asyncio.get_event_loop()
    if date:
        try:
            requested_date = datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError:
            return {
                "ok": False,
                "reason": "invalid_date",
                "message": f"日期格式错误: {date}，请使用 YYYY-MM-DD",
            }
        today_utc8 = datetime.now(UTC8).date()
        if requested_date > today_utc8:
            return {
                "ok": False,
                "reason": "future_date",
                "message": f"请求日期 {date} 超过今天 {today_utc8.strftime('%Y-%m-%d')}",
            }
        snapshot = await loop.run_in_executor(None, getter_by_date, date)
    else:
        snapshot = await loop.run_in_executor(None, getter_latest)

    if not snapshot:
        return {
            "ok": False,
            "reason": "no_snapshot",
            "message": empty_message,
        }

    open_positions = await loop.run_in_executor(None, trade_repo.get_open_positions)
    held_symbols = set()
    for pos in open_positions:
        sym = str(pos.get("symbol", "")).upper().strip()
        if not sym:
            continue
        held_symbols.add(sym)
        held_symbols.add(_normalize_symbol(sym))

    enriched_rows = []
    for idx, row in enumerate(snapshot.get("rows", []), start=1):
        symbol = str(row.get("symbol", "")).upper()
        enriched_rows.append(
            {
                **row,
                "rank": idx,
                "is_held": symbol in held_symbols,
            }
        )

    snapshot["rows"] = enriched_rows
    snapshot["top_count"] = len(enriched_rows)
    snapshot.pop("all_rows", None)
    return {"ok": True, **snapshot}


@router.get("/api/rebound-7d")
async def get_rebound_7d_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db=Depends(get_db),
):
    snapshot_repo = SnapshotRepository(db)
    trade_repo = TradeRepository(db)
    return await _get_rebound_snapshot_response(
        date=date,
        snapshot_repo=snapshot_repo,
        trade_repo=trade_repo,
        getter_by_date=snapshot_repo.get_rebound_7d_snapshot_by_date,
        getter_latest=snapshot_repo.get_latest_rebound_7d_snapshot,
        empty_message="暂无快照数据，请等待下一次07:30定时任务生成（14D）",
    )


@router.get("/api/rebound-7d/dates")
async def get_rebound_7d_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db=Depends(get_db),
):
    loop = asyncio.get_event_loop()
    snapshot_repo = SnapshotRepository(db)
    dates = await loop.run_in_executor(None, snapshot_repo.list_rebound_7d_snapshot_dates, limit)
    return {"dates": dates}


@router.get("/api/rebound-30d")
async def get_rebound_30d_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db=Depends(get_db),
):
    snapshot_repo = SnapshotRepository(db)
    trade_repo = TradeRepository(db)
    return await _get_rebound_snapshot_response(
        date=date,
        snapshot_repo=snapshot_repo,
        trade_repo=trade_repo,
        getter_by_date=snapshot_repo.get_rebound_30d_snapshot_by_date,
        getter_latest=snapshot_repo.get_latest_rebound_30d_snapshot,
        empty_message="暂无快照数据，请等待下一次07:30定时任务生成（30D）",
    )


@router.get("/api/rebound-30d/dates")
async def get_rebound_30d_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db=Depends(get_db),
):
    loop = asyncio.get_event_loop()
    snapshot_repo = SnapshotRepository(db)
    dates = await loop.run_in_executor(None, snapshot_repo.list_rebound_30d_snapshot_dates, limit)
    return {"dates": dates}


@router.get("/api/rebound-60d")
async def get_rebound_60d_snapshot(
    date: Optional[str] = Query(None, description="Snapshot date in YYYY-MM-DD"),
    db=Depends(get_db),
):
    snapshot_repo = SnapshotRepository(db)
    trade_repo = TradeRepository(db)
    return await _get_rebound_snapshot_response(
        date=date,
        snapshot_repo=snapshot_repo,
        trade_repo=trade_repo,
        getter_by_date=snapshot_repo.get_rebound_60d_snapshot_by_date,
        getter_latest=snapshot_repo.get_latest_rebound_60d_snapshot,
        empty_message="暂无快照数据，请等待下一次07:30定时任务生成（60D）",
    )


@router.get("/api/rebound-60d/dates")
async def get_rebound_60d_snapshot_dates(
    limit: int = Query(90, ge=1, le=365),
    db=Depends(get_db),
):
    loop = asyncio.get_event_loop()
    snapshot_repo = SnapshotRepository(db)
    dates = await loop.run_in_executor(None, snapshot_repo.list_rebound_60d_snapshot_dates, limit)
    return {"dates": dates}
