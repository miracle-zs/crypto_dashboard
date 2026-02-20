import asyncio

from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.deps import get_db
from app.security import require_admin_token

router = APIRouter()


@router.get("/api/watch-notes")
async def get_watch_notes(
    limit: int = Query(200, description="最大返回记录数"),
    db=Depends(get_db)
):
    loop = asyncio.get_event_loop()
    items = await loop.run_in_executor(None, db.get_watch_notes, limit)
    return {"items": items}


@router.post("/api/watch-notes", dependencies=[Depends(require_admin_token)])
async def create_watch_note(
    symbol: str = Query(..., description="观察币种"),
    db=Depends(get_db)
):
    normalized_symbol = (symbol or "").strip().upper()
    if not normalized_symbol:
        raise HTTPException(status_code=400, detail="symbol 不能为空")

    loop = asyncio.get_event_loop()
    try:
        item = await loop.run_in_executor(None, db.add_watch_note, normalized_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    exists_today = bool(item.get("exists_today"))
    return {
        "message": "已存在" if exists_today else "已记录",
        "item": item,
        "exists_today": exists_today
    }


@router.delete("/api/watch-notes/{note_id}", dependencies=[Depends(require_admin_token)])
async def remove_watch_note(
    note_id: int,
    db=Depends(get_db)
):
    loop = asyncio.get_event_loop()
    deleted = await loop.run_in_executor(None, db.delete_watch_note, note_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"message": "已删除", "id": note_id}
