from typing import Optional

from fastapi import APIRouter, Depends, Query

from app.core.deps import get_db
from app.models import OpenPositionsResponse
from app.services import PositionsService

router = APIRouter()
service = PositionsService()


@router.get("/api/open-positions", response_model=OpenPositionsResponse)
async def get_open_positions(
    since_version: Optional[int] = Query(default=None),
    db=Depends(get_db),
):
    return await service.build_open_positions_response(db=db, since_version=since_version)
