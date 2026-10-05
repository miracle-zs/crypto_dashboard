from collections.abc import Generator

from fastapi import Request

from app.database import Database


def get_db(request: Request) -> Generator[Database, None, None]:
    """Expose the database owned by this FastAPI application's lifespan."""
    context = getattr(request.app.state, "context", None)
    if context is None:
        raise RuntimeError("application context is not initialized")
    yield context.db
