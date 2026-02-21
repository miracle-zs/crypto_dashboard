from app.binance_client import BinanceFuturesRestClient
from app.database import Database


def get_db():
    db = Database()
    yield db


def get_public_rest():
    return BinanceFuturesRestClient()
