from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.static_assets import static_asset_url

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "dark_css_url": static_asset_url("/static/dark-unified.css"),
            "page_js_url": static_asset_url("/static/js/index-dashboard.js"),
        },
    )

