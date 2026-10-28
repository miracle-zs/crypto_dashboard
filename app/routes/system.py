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
            "tailwind_css_url": static_asset_url("/static/css/tailwind.css"),
            "lucide_js_url": static_asset_url("/static/vendor/lucide.min.js"),
            "apexcharts_js_url": static_asset_url("/static/vendor/apexcharts.min.js"),
            "dark_css_url": static_asset_url("/static/dark-unified.css"),
            "page_js_url": static_asset_url("/static/js/index-dashboard.js"),
        },
    )

