from app.database import Database


def test_daily_kline_repository_upserts_and_loads_market_view(tmp_path):
    from app.repositories.daily_kline_repository import DailyKlineRepository

    db = Database(db_path=str(tmp_path / "daily_klines.db"))
    repo = DailyKlineRepository(db)
    repo.upsert(
        [
            {
                "symbol": "BTCUSDT",
                "open_time": 1_000,
                "open": 90,
                "high": 110,
                "low": 80,
                "close": 100,
                "is_closed": False,
            },
            {
                "symbol": "ETHUSDT",
                "open_time": 1_000,
                "open": 9,
                "high": 11,
                "low": 8,
                "close": 10,
                "is_closed": True,
            },
        ]
    )
    repo.upsert(
        [
            {
                "symbol": "BTCUSDT",
                "open_time": 1_000,
                "open": 90,
                "high": 120,
                "low": 80,
                "close": 115,
                "is_closed": True,
            },
            {
                "symbol": "BTCUSDT",
                "open_time": 2_000,
                "open": 115,
                "high": 130,
                "low": 100,
                "close": 125,
                "is_closed": False,
            },
        ]
    )

    view = repo.load(["BTCUSDT", "ETHUSDT"], since_open_time=1_000)

    assert [row["open_time"] for row in view["BTCUSDT"]] == [1_000, 2_000]
    assert view["BTCUSDT"][0]["high"] == 120.0
    assert view["BTCUSDT"][0]["is_closed"] is True
    assert repo.latest_open_times(["BTCUSDT", "ETHUSDT", "XRPUSDT"]) == {
        "BTCUSDT": 2_000,
        "ETHUSDT": 1_000,
        "XRPUSDT": None,
    }

