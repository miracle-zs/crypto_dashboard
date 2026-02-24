import os

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/leaderboard", response_class=HTMLResponse)
async def read_leaderboard_page(request: Request):
    leaderboard_hour = int(os.getenv("LEADERBOARD_ALERT_HOUR", "7"))
    leaderboard_minute = int(os.getenv("LEADERBOARD_ALERT_MINUTE", "40"))
    rebound_hour = int(os.getenv("REBOUND_7D_HOUR", "7"))
    rebound_minute = int(os.getenv("REBOUND_7D_MINUTE", "30"))

    return templates.TemplateResponse(
        request,
        "leaderboard.html",
        {
            "leaderboard_snapshot_time_label": f"{leaderboard_hour:02d}:{leaderboard_minute:02d}",
            "rebound_snapshot_time_label": f"{rebound_hour:02d}:{rebound_minute:02d}",
        },
    )
