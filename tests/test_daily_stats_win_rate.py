from app.database import Database
from app.repositories.trade_repository import TradeRepository
from app.repositories.sync_repository import SyncRepository


def _insert_closed_trade(conn, *, day, pnl, order_id):
    conn.execute(
        """
        INSERT INTO trades (
            no, date, entry_time, exit_time, holding_time, symbol, side,
            price_change_pct, entry_amount, entry_price, exit_price, qty,
            fees, pnl_net, close_type, return_rate, open_price,
            pnl_before_fees, entry_order_id, exit_order_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            order_id,
            day,
            f"{day[:4]}-{day[4:6]}-{day[6:]} 10:00:00",
            f"{day[:4]}-{day[4:6]}-{day[6:]} 10:05:00",
            "5m",
            "BTC",
            "LONG",
            0.0,
            100.0,
            100.0,
            100.0 + pnl,
            1.0,
            0.0,
            pnl,
            "test",
            "0%",
            100.0,
            pnl,
            order_id,
            str(order_id),
        ),
    )


def test_daily_win_rate_uses_closed_trades_and_reports_open_positions_separately(tmp_path):
    db = Database(db_path=str(tmp_path / "daily_stats.db"))
    repo = TradeRepository(db)
    conn = db._get_connection()
    _insert_closed_trade(conn, day="20261027", pnl=5.0, order_id=1)
    _insert_closed_trade(conn, day="20261027", pnl=-2.0, order_id=2)
    conn.commit()
    conn.close()
    SyncRepository(db).save_open_positions(
        [{
            "date": "20261027",
            "symbol": "ETH",
            "side": "LONG",
            "entry_time": "2026-10-27 11:00:00",
            "entry_price": 10.0,
            "qty": 1.0,
            "entry_amount": 10.0,
            "order_id": 3,
        }]
    )

    stats = {row["date"]: row for row in repo.get_daily_stats()}

    assert stats["20261027"]["trade_count"] == 3
    assert stats["20261027"]["closed_trade_count"] == 2
    assert stats["20261027"]["open_position_count"] == 1
    assert stats["20261027"]["win_count"] == 1
    assert stats["20261027"]["win_rate"] == 50.0


def test_daily_win_rate_is_unknown_when_a_day_has_only_open_positions(tmp_path):
    db = Database(db_path=str(tmp_path / "daily_stats_open_only.db"))
    repo = TradeRepository(db)
    SyncRepository(db).save_open_positions(
        [{
            "date": "20261027",
            "symbol": "ETH",
            "side": "LONG",
            "entry_time": "2026-10-27 11:00:00",
            "entry_price": 10.0,
            "qty": 1.0,
            "entry_amount": 10.0,
            "order_id": 3,
        }]
    )

    stats = repo.get_daily_stats()

    assert stats == [{
        "date": "20261027",
        "trade_count": 1,
        "closed_trade_count": 0,
        "open_position_count": 1,
        "total_amount": 10.0,
        "total_pnl": 0.0,
        "win_count": 0,
        "loss_count": 0,
        "win_rate": None,
    }]
