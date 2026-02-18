from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="templates")


@router.get("/leaderboard", response_class=HTMLResponse)
async def read_leaderboard_page(request: Request):
    return templates.TemplateResponse("leaderboard.html", {"request": request})
