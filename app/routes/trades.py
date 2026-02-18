import asyncio

from fastapi import APIRouter, Depends

from app.database import Database

router = APIRouter()


def get_db():
    db = Database()
    try:
        yield db
    finally:
        close = getattr(db, "close", None)
        if callable(close):
            close()


@router.get("/api/database/stats")
async def get_database_stats(db: Database = Depends(get_db)):
    loop = asyncio.get_event_loop()
    stats, sync_status = await asyncio.gather(
        loop.run_in_executor(None, db.get_statistics),
        loop.run_in_executor(None, db.get_sync_status),
    )
    return {"statistics": stats, "sync_status": sync_status}
