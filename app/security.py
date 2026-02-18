import os
import secrets
from typing import Optional

from fastapi import Header, HTTPException


def require_admin_token(x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token")) -> None:
    expected = os.getenv("DASHBOARD_ADMIN_TOKEN", "")
    if not expected:
        return

    provided = x_admin_token or ""
    if not secrets.compare_digest(provided, expected):
        raise HTTPException(status_code=401, detail="Invalid admin token")
