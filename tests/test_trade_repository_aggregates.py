from datetime import datetime, timedelta, timezone

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


def test_get_trade_aggregates_uses_cache_and_invalidates_on_data_change(tmp_path):
    db = Database(db_path=str(tmp_path / "trade_aggregates_cache.db"))
    repo = TradeRepository(db)

    conn = db._get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO trades (
            no, date, entry_time, exit_time, holding_time, symbol, side,
            price_change_pct, entry_amount, entry_price, exit_price, qty,
            fees, pnl_net, close_type, return_rate, open_price,
            pnl_before_fees, entry_order_id, exit_order_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1, "20260224", "2026-02-24 01:00:00", "2026-02-24 01:03:00", "3m",
            "BTC", "LONG", 0.1, 100.0, 100.0, 101.0, 1.0,
            0.1, 10.0, "tp", "10.00%", 99.0, 10.1, 101, "201",
        ),
    )
    conn.commit()
    conn.close()

    payload = repo.get_trade_aggregates()
    assert payload["hourly_pnl"][1] == 10.0

    conn = db._get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE trade_aggregates_cache
        SET payload_json = ?
        WHERE id = 1
        """,
        ('{"hourly_pnl":[999],"duration_buckets":[],"duration_points":[],"symbol_rank":{"winners":[],"losers":[]}}',),
    )
    conn.commit()
    conn.close()

    cached_payload = repo.get_trade_aggregates()
    assert cached_payload["hourly_pnl"] == [999]

    conn = db._get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO trades (
            no, date, entry_time, exit_time, holding_time, symbol, side,
            price_change_pct, entry_amount, entry_price, exit_price, qty,
            fees, pnl_net, close_type, return_rate, open_price,
            pnl_before_fees, entry_order_id, exit_order_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            2, "20260224", "2026-02-24 02:00:00", "2026-02-24 02:10:00", "10m",
            "ETH", "SHORT", -0.1, 120.0, 120.0, 118.0, 1.0,
            0.1, -3.0, "sl", "-2.50%", 121.0, -2.9, 102, "202",
        ),
    )
    conn.commit()
    conn.close()

    refreshed_payload = repo.get_trade_aggregates()
    assert len(refreshed_payload["hourly_pnl"]) == 24
    assert refreshed_payload["hourly_pnl"][1] == 10.0
    assert refreshed_payload["hourly_pnl"][2] == -3.0


def test_get_trade_aggregates_window_filters_recent_data(tmp_path):
    db = Database(db_path=str(tmp_path / "trade_aggregates_window.db"))
    repo = TradeRepository(db)

    utc8 = timezone(timedelta(hours=8))
    now = datetime.now(utc8)
    recent_entry = (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
    recent_exit = (now - timedelta(days=1) + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    old_entry = (now - timedelta(days=40)).strftime("%Y-%m-%d %H:%M:%S")
    old_exit = (now - timedelta(days=40) + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")

    conn = db._get_connection()
    cur = conn.cursor()
    rows = [
        (
            1, now.strftime("%Y%m%d"), recent_entry, recent_exit, "5m",
            "BTC", "LONG", 0.1, 100.0, 100.0, 101.0, 1.0,
            0.1, 10.0, "tp", "10.00%", 99.0, 10.1, 101, "201",
        ),
        (
            2, (now - timedelta(days=40)).strftime("%Y%m%d"), old_entry, old_exit, "5m",
            "ETH", "SHORT", -0.2, 120.0, 120.0, 118.0, 1.0,
            0.2, -5.0, "sl", "-4.17%", 121.0, -4.8, 102, "202",
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

    all_payload = repo.get_trade_aggregates(window="all")
    d7_payload = repo.get_trade_aggregates(window="7d")

    assert all_payload["hourly_pnl"] != d7_payload["hourly_pnl"]
    all_symbols = {item["symbol"] for item in all_payload["symbol_rank"]["winners"] + all_payload["symbol_rank"]["losers"]}
    d7_symbols = {item["symbol"] for item in d7_payload["symbol_rank"]["winners"] + d7_payload["symbol_rank"]["losers"]}
    assert {"BTC", "ETH"}.issubset(all_symbols)
    assert "BTC" in d7_symbols
    assert "ETH" not in d7_symbols
