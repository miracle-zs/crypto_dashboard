from app.binance_client import BinanceFuturesRestClient
from app.database import Database

_db_singleton = None


def get_db_singleton() -> Database:
    global _db_singleton
    if _db_singleton is None:
        _db_singleton = Database()
    return _db_singleton


def get_db():
    yield get_db_singleton()


def get_public_rest():
    return BinanceFuturesRestClient()
