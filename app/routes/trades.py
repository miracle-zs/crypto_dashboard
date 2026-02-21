import asyncio

from fastapi import APIRouter, Depends

from app.core.async_utils import run_in_thread
from app.core.deps import get_db
from app.database import Database
from app.repositories import SyncRepository, TradeRepository

router = APIRouter()


@router.get("/api/database/stats")
async def get_database_stats(db: Database = Depends(get_db)):
    trade_repo = TradeRepository(db)
    sync_repo = SyncRepository(db)
    stats, sync_status = await asyncio.gather(
        run_in_thread(trade_repo.get_statistics),
        run_in_thread(sync_repo.get_sync_status),
    )
    return {"statistics": stats, "sync_status": sync_status}
