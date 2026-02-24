import pytest

from app.database import Database
from app.repositories.trade_repository import TradeRepository


def test_recompute_trade_summary_matches_expected_metrics(tmp_path):
    db = Database(db_path=str(tmp_path / "trade_summary_recompute.db"))
    repo = TradeRepository(db)

    conn = db._get_connection()
    cur = conn.cursor()
    rows = [
        (
            1, "20260221", "2026-02-21 10:00:00", "2026-02-21 10:05:00", "5m",
            "BTC", "LONG", 0.1, 100.0, 100.0, 101.0, 1.0,
            0.1, 10.0, "tp", "10.00%", 99.0, 10.1, 101, "201",
        ),
        (
            2, "20260221", "2026-02-21 11:00:00", "2026-02-21 11:15:00", "15m",
            "ETH", "SHORT", -0.2, 120.0, 120.0, 119.0, 1.0,
            0.2, -5.0, "sl", "-4.17%", 121.0, -4.8, 102, "202",
        ),
        (
            3, "20260221", "2026-02-21 12:00:00", "2026-02-21 12:10:00", "10m",
            "XRP", "LONG", 0.05, 80.0, 80.0, 80.5, 1.0,
            0.05, 0.0, "be", "0.00%", 79.5, 0.05, 103, "203",
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

    summary = repo.recompute_trade_summary()

    assert summary["total_trades"] == 3
    assert summary["win_count"] == 1
    assert summary["loss_count"] == 1
    assert summary["total_pnl"] == 5.0
    assert summary["total_fees"] == pytest.approx(0.35)
    assert summary["win_rate"] == (1 / 3) * 100
    assert summary["equity_curve"] == [10.0, 5.0, 5.0]
    assert summary["current_streak"] == 0
    assert summary["best_win_streak"] == 1
    assert summary["worst_loss_streak"] == -1
    assert summary["max_single_loss"] == -5.0
    assert summary["max_drawdown"] == -5.0


def test_recompute_trade_summary_drawdown_differs_from_single_loss(tmp_path):
    db = Database(db_path=str(tmp_path / "trade_summary_drawdown.db"))
    repo = TradeRepository(db)

    conn = db._get_connection()
    cur = conn.cursor()
    pnls = [4.0, -2.0, -2.0, -2.0]
    rows = []
    for idx, pnl in enumerate(pnls, start=1):
        hour = 9 + idx
        rows.append(
            (
                idx,
                "20260221",
                f"2026-02-21 {hour:02d}:00:00",
                f"2026-02-21 {hour:02d}:05:00",
                "5m",
                f"S{idx}",
                "LONG",
                0.0,
                100.0,
                100.0,
                100.0,
                1.0,
                0.0,
                pnl,
                "tp",
                "0.00%",
                100.0,
                pnl,
                1000 + idx,
                str(2000 + idx),
            )
        )

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

    summary = repo.recompute_trade_summary()

    assert summary["max_single_loss"] == -2.0
    assert summary["max_drawdown"] == -6.0
