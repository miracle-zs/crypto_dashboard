from app.database import Database


def get_db():
    db = Database()
    try:
        yield db
    finally:
        close = getattr(db, "close", None)
        if callable(close):
            close()
