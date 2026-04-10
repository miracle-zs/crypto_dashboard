from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.static_assets import static_asset_url

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/crash-risk", response_class=HTMLResponse)
async def read_crash_risk_page(request: Request):
    return templates.TemplateResponse(
        request,
        "crash_risk.html",
        {
            "dark_css_url": static_asset_url("/static/dark-unified.css"),
            "page_js_url": static_asset_url("/static/js/crash-risk.js"),
        },
    )
