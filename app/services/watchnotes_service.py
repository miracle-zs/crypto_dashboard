from app.core.async_utils import run_in_thread
from app.repositories import WatchNotesRepository


class WatchNotesService:
    async def get_notes(self, *, db, limit: int):
        repo = WatchNotesRepository(db)
        items = await run_in_thread(repo.get_watch_notes, limit)
        return {"items": items}

    async def create_note(self, *, db, symbol: str):
        repo = WatchNotesRepository(db)
        item = await run_in_thread(repo.add_watch_note, symbol)
        exists_today = bool(item.get("exists_today"))
        return {
            "message": "已存在" if exists_today else "已记录",
            "item": item,
            "exists_today": exists_today,
        }

    async def delete_note(self, *, db, note_id: int):
        repo = WatchNotesRepository(db)
        deleted = await run_in_thread(repo.delete_watch_note, note_id)
        return deleted
