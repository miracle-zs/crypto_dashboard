from dataclasses import dataclass

from app.database import Database


@dataclass(frozen=True)
class ApplicationContext:
    """Explicit process dependencies assembled by the FastAPI lifespan."""

    db: Database

    @classmethod
    def create(cls) -> "ApplicationContext":
        return cls(db=Database())
