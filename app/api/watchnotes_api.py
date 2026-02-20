from fastapi import APIRouter, Depends, HTTPException, Query

from app.core.deps import get_db
from app.security import require_admin_token
from app.services import WatchNotesService

router = APIRouter()
service = WatchNotesService()


@router.get("/api/watch-notes")
async def get_watch_notes(
    limit: int = Query(200, description="最大返回记录数"),
    db=Depends(get_db)
):
    return await service.get_notes(db=db, limit=limit)


@router.post("/api/watch-notes", dependencies=[Depends(require_admin_token)])
async def create_watch_note(
    symbol: str = Query(..., description="观察币种"),
    db=Depends(get_db)
):
    normalized_symbol = (symbol or "").strip().upper()
    if not normalized_symbol:
        raise HTTPException(status_code=400, detail="symbol 不能为空")

    try:
        return await service.create_note(db=db, symbol=normalized_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete("/api/watch-notes/{note_id}", dependencies=[Depends(require_admin_token)])
async def remove_watch_note(
    note_id: int,
    db=Depends(get_db)
):
    deleted = await service.delete_note(db=db, note_id=note_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="记录不存在")
    return {"message": "已删除", "id": note_id}
