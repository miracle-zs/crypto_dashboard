from fastapi import APIRouter, Depends

from app.binance_client import BinanceFuturesRestClient
from app.core.deps import get_db, get_public_rest
from app.models import OpenPositionsResponse
from app.services import PositionsService

router = APIRouter()
service = PositionsService()


@router.get("/api/open-positions", response_model=OpenPositionsResponse)
async def get_open_positions(
    db=Depends(get_db),
    client: BinanceFuturesRestClient = Depends(get_public_rest),
):
    return await service.build_open_positions_response(db=db, client=client)
