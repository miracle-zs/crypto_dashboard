from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.core.scheduler_config import _env_int
from app.static_assets import static_asset_url

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/leaderboard", response_class=HTMLResponse)
async def read_leaderboard_page(request: Request):
    leaderboard_hour = _env_int("LEADERBOARD_ALERT_HOUR", 7, minimum=0) % 24
    leaderboard_minute = _env_int("LEADERBOARD_ALERT_MINUTE", 30, minimum=0) % 60
    rebound_hour = _env_int("MARKET_SNAPSHOT_HOUR", 7, minimum=0) % 24
    rebound_minute = _env_int("MARKET_SNAPSHOT_MINUTE", 0, minimum=0) % 60

    return templates.TemplateResponse(
        request,
        "leaderboard.html",
        {
            "leaderboard_snapshot_time_label": f"{leaderboard_hour:02d}:{leaderboard_minute:02d}",
            "rebound_snapshot_time_label": f"{rebound_hour:02d}:{rebound_minute:02d}",
            "leaderboard_css_url": static_asset_url("/static/dark-unified.css"),
            "leaderboard_js_url": static_asset_url("/static/js/leaderboard.js"),
        },
    )
