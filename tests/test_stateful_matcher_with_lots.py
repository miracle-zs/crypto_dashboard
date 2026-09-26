from pathlib import Path
from tempfile import TemporaryDirectory

from app.core.trade_matching import match_orders_to_positions
from app.database import Database
from app.repositories.sync_write_repository import SyncWriteRepository


def _make_order(order_id, side, price, qty, time_ms, position_side="BOTH", fee=0.0):
    return {
        "orderId": order_id,
        "side": side,
        "positionSide": position_side,
        "executedQty": str(qty),
        "avgPrice": str(price),
        "updateTime": time_ms,
        "type": "MARKET",
        "fee": fee,
    }


def test_matcher_resumes_from_exit_only_with_initial_open_lots():
    # Order 1: BUY 1 BTC @ 50,000 at t=1000
    # Order 2: SELL 1 BTC @ 55,000 at t=2000
    buy_order = _make_order(1, "BUY", 50000.0, 1.0, 1000, fee=-5.0)
    sell_order = _make_order(2, "SELL", 55000.0, 1.0, 2000, fee=-5.5)

    # 1. First batch only has BUY order
    positions_1, open_lots_1 = match_orders_to_positions(
        [buy_order], "BTCUSDT", return_open_lots=True
    )
    assert len(positions_1) == 0
    assert len(open_lots_1) == 1
    assert open_lots_1[0]["order_id"] == 1
    assert open_lots_1[0]["side"] == "LONG"
    assert open_lots_1[0]["price"] == 50000.0
    assert open_lots_1[0]["remaining_qty"] == 1.0

    # 2. Second batch only has SELL order (exit-only!), but gets prior open_lots_1
    positions_2, open_lots_2 = match_orders_to_positions(
        [sell_order],
        "BTCUSDT",
        initial_open_lots=open_lots_1,
        return_open_lots=True,
    )
    assert len(positions_2) == 1
    assert len(open_lots_2) == 0
    pos = positions_2[0]
    assert pos["symbol"] == "BTCUSDT"
    assert pos["side"] == "LONG"
    assert pos["entry_price"] == 50000.0
    assert pos["exit_price"] == 55000.0
    assert pos["qty"] == 1.0
    assert pos["pnl_before_fees"] == 5000.0
    assert pos["entry_order_id"] == 1
    assert pos["exit_order_id"] == 2
    assert pos["fees"] == -10.5
    assert pos["pnl"] == 4989.5


def test_partial_close_updates_remaining_lot_qty():
    # BUY 2 BTC @ 60,000
    # SELL 0.5 BTC @ 62,000
    buy_order = _make_order(101, "BUY", 60000.0, 2.0, 1000)
    sell_order = _make_order(102, "SELL", 62000.0, 0.5, 2000)

    _, lots = match_orders_to_positions([buy_order], "BTCUSDT", return_open_lots=True)
    assert len(lots) == 1
    assert lots[0]["remaining_qty"] == 2.0

    positions, remaining_lots = match_orders_to_positions(
        [sell_order], "BTCUSDT", initial_open_lots=lots, return_open_lots=True
    )
    assert len(positions) == 1
    assert positions[0]["qty"] == 0.5
    assert positions[0]["pnl_before_fees"] == (62000 - 60000) * 0.5

    assert len(remaining_lots) == 1
    assert remaining_lots[0]["order_id"] == 101
    assert abs(remaining_lots[0]["remaining_qty"] - 1.5) < 1e-6


def test_hedge_mode_separate_long_short_lots():
    # BUY LONG 1 BTC, SELL SHORT 1 BTC
    long_entry = _make_order(201, "BUY", 50000.0, 1.0, 1000, position_side="LONG")
    short_entry = _make_order(202, "SELL", 52000.0, 1.0, 1050, position_side="SHORT")

    _, lots = match_orders_to_positions(
        [long_entry, short_entry], "BTCUSDT", return_open_lots=True
    )
    assert len(lots) == 2
    pos_sides = {lot["position_side"] for lot in lots}
    assert pos_sides == {"LONG", "SHORT"}

    # Close only the SHORT position
    short_exit = _make_order(203, "BUY", 51000.0, 1.0, 2000, position_side="SHORT")
    positions, remaining_lots = match_orders_to_positions(
        [short_exit], "BTCUSDT", initial_open_lots=lots, return_open_lots=True
    )
    assert len(positions) == 1
    assert positions[0]["side"] == "SHORT"
    assert positions[0]["entry_price"] == 52000.0
    assert positions[0]["exit_price"] == 51000.0
    assert positions[0]["pnl_before_fees"] == 1000.0

    # Remaining lots should only be the LONG lot
    assert len(remaining_lots) == 1
    assert remaining_lots[0]["position_side"] == "LONG"
    assert remaining_lots[0]["order_id"] == 201


def test_open_lots_persistence_in_sync_write_repository():
    with TemporaryDirectory() as tmp_dir:
        db = Database(str(Path(tmp_dir) / "test_lots.db"))
        repo = SyncWriteRepository(db)

        test_lots = [
            {
                "symbol": "BTCUSDT",
                "position_side": "BOTH",
                "side": "LONG",
                "order_id": 1001,
                "price": 60000.0,
                "qty": 1.5,
                "remaining_qty": 1.5,
                "time_ms": 1700000000000,
                "fee_rate": 0.0004,
            },
            {
                "symbol": "BTCUSDT",
                "position_side": "BOTH",
                "side": "LONG",
                "order_id": 1002,
                "price": 61000.0,
                "qty": 2.0,
                "remaining_qty": 2.0,
                "time_ms": 1700000050000,
                "fee_rate": 0.0004,
            },
        ]

        saved_count = repo.save_open_lots("BTCUSDT", test_lots)
        assert saved_count == 2

        loaded = repo.get_open_lots("BTCUSDT")
        assert len(loaded) == 2
        assert loaded[0]["order_id"] == 1001
        assert loaded[0]["remaining_qty"] == 1.5
        assert loaded[1]["order_id"] == 1002
        assert loaded[1]["remaining_qty"] == 2.0

        # Now simulate partial fill on lot 1 and total fill on lot 2
        updated_lots = [
            {
                "symbol": "BTCUSDT",
                "position_side": "BOTH",
                "side": "LONG",
                "order_id": 1002,
                "price": 61000.0,
                "qty": 2.0,
                "remaining_qty": 0.8,
                "time_ms": 1700000050000,
                "fee_rate": 0.0004,
            }
        ]
        repo.save_open_lots("BTCUSDT", updated_lots)

        loaded_updated = repo.get_open_lots("BTCUSDT")
        assert len(loaded_updated) == 1
        assert loaded_updated[0]["order_id"] == 1002
        assert loaded_updated[0]["remaining_qty"] == 0.8
