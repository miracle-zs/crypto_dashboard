from app.database import Database
from app.repositories.trade_repository import TradeRepository


def test_get_trade_aggregates_returns_expected_shape_and_values(tmp_path):
    db = Database(db_path=str(tmp_path / "trade_aggregates.db"))
    repo = TradeRepository(db)

    conn = db._get_connection()
    cur = conn.cursor()
    rows = [
        (
            1, "20260224", "2026-02-24 01:00:00", "2026-02-24 01:03:00", "3m",
            "BTC", "LONG", 0.1, 100.0, 100.0, 101.0, 1.0,
            0.1, 10.0, "tp", "10.00%", 99.0, 10.1, 101, "201",
        ),
        (
            2, "20260224", "2026-02-24 02:00:00", "2026-02-24 02:20:00", "20m",
            "ETH", "SHORT", -0.2, 120.0, 120.0, 119.0, 1.0,
            0.2, -5.0, "sl", "-4.17%", 121.0, -4.8, 102, "202",
        ),
        (
            3, "20260224", "2026-02-24 01:30:00", "2026-02-24 04:00:00", "150m",
            "BTC", "LONG", 0.05, 80.0, 80.0, 80.5, 1.0,
            0.05, 2.0, "tp", "2.50%", 79.5, 2.05, 103, "203",
        ),
    ]
    cur.executemany(
        """
        INSERT INTO trades (
            no, date, entry_time, exit_time, holding_time, symbol, side,
            price_change_pct, entry_amount, entry_price, exit_price, qty,
            fees, pnl_net, close_type, return_rate, open_price,
            pnl_before_fees, entry_order_id, exit_order_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.commit()
    conn.close()

    payload = repo.get_trade_aggregates()

    assert set(payload.keys()) == {"duration_buckets", "duration_points", "hourly_pnl", "symbol_rank"}
    assert len(payload["hourly_pnl"]) == 24
    assert payload["hourly_pnl"][1] == 12.0
    assert payload["hourly_pnl"][2] == -5.0

    bucket_map = {item["label"]: item for item in payload["duration_buckets"]}
    assert bucket_map["0-5m"]["trade_count"] == 1
    assert bucket_map["0-5m"]["win_pnl"] == 10.0
    assert bucket_map["15-30m"]["trade_count"] == 1
    assert bucket_map["15-30m"]["loss_pnl"] == -5.0
    assert bucket_map["2h+"]["trade_count"] == 1

    duration_points = payload["duration_points"]
    assert len(duration_points) == 3
    assert all(set(p.keys()) == {"x", "y", "symbol", "time"} for p in duration_points)

    winners = payload["symbol_rank"]["winners"]
    losers = payload["symbol_rank"]["losers"]
    assert winners[0]["symbol"] == "BTC"
    assert winners[0]["pnl"] == 12.0
    assert losers[0]["symbol"] == "ETH"
    assert losers[0]["pnl"] == -5.0
