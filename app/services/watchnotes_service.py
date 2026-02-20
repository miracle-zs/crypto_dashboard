import asyncio


class WatchNotesService:
    async def get_notes(self, *, db, limit: int):
        loop = asyncio.get_event_loop()
        items = await loop.run_in_executor(None, db.get_watch_notes, limit)
        return {"items": items}

    async def create_note(self, *, db, symbol: str):
        loop = asyncio.get_event_loop()
        item = await loop.run_in_executor(None, db.add_watch_note, symbol)
        exists_today = bool(item.get("exists_today"))
        return {
            "message": "已存在" if exists_today else "已记录",
            "item": item,
            "exists_today": exists_today,
        }

    async def delete_note(self, *, db, note_id: int):
        loop = asyncio.get_event_loop()
        deleted = await loop.run_in_executor(None, db.delete_watch_note, note_id)
        return deleted
