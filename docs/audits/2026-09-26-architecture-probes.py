"""Offline characterization probes; no exchange calls or production DB writes.

Run from repository root with PYTHONPATH=. python <this-file>.
Verifies first-principles architectural invariants and contracts.
"""
from pathlib import Path
from tempfile import TemporaryDirectory

from app.core.trade_matching import match_orders_to_positions
from app.database import Database
from app.repositories.sync_write_repository import SyncWriteRepository


def order(oid, side, time, price, fee=None):
    o = dict(orderId=oid, side=side, positionSide="BOTH", executedQty="1",
             avgPrice=str(price), updateTime=time, type="MARKET")
    if fee is not None:
        o["commission"] = fee
    return o


def main():
    # 1. Deterministic fee invariance across batch boundaries
    # Each order has its fill-level fee recorded from execution facts
    fees_map = {1: -0.5, 2: -0.5, 3: -1.5, 4: -1.5}
    orders = [order(1, "BUY", 1000, 100), order(2, "SELL", 2000, 110),
              order(3, "BUY", 3000, 100), order(4, "SELL", 6000, 110)]
    whole = match_orders_to_positions(orders, "BTCUSDT", fees_map)
    split = (match_orders_to_positions(orders[:2], "BTCUSDT", fees_map)
             + match_orders_to_positions(orders[2:], "BTCUSDT", fees_map))
    assert [x["fees"] for x in whole] == [-1.0, -3.0]
    assert [x["fees"] for x in split] == [-1.0, -3.0]
    assert [x["fees"] for x in whole] == [x["fees"] for x in split]
    print("Fee allocation invariant verified: whole == split (deterministic fees across batch slices)")

    # 2. Stateful resumable matching with prior open lots
    # Batch 1: Only contains entry (BUY) -> emits 0 closed positions and 1 open lot
    pos_batch1, open_lots = match_orders_to_positions(orders[:1], "BTCUSDT", fees_map, return_open_lots=True)
    assert len(pos_batch1) == 0
    assert len(open_lots) == 1 and open_lots[0]["order_id"] == 1

    # Batch 2: Only contains exit (SELL) -> without lot state it fails, with lot state it resumes!
    assert match_orders_to_positions(orders[1:2], "BTCUSDT", fees_map) == []
    resumed_pos, remaining_lots = match_orders_to_positions(
        orders[1:2], "BTCUSDT", fees_map, initial_open_lots=open_lots, return_open_lots=True
    )
    assert len(resumed_pos) == 1
    assert len(remaining_lots) == 0
    assert resumed_pos[0]["entry_order_id"] == 1
    assert resumed_pos[0]["exit_order_id"] == 2
    assert resumed_pos[0]["entry_price"] == 100.0
    assert resumed_pos[0]["exit_price"] == 110.0
    assert resumed_pos[0]["fees"] == -1.0
    print("Matcher successfully resumed from exit-only input using prior lot state")

    # 3. Persistent lot ledger and hedge positions in database
    with TemporaryDirectory() as tmp:
        db = Database(str(Path(tmp) / "probe.db"))
        repo = SyncWriteRepository(db)

        # Verify open_lots persistence
        repo.save_open_lots("BTCUSDT", open_lots)
        saved_lots = repo.get_open_lots("BTCUSDT")
        assert len(saved_lots) == 1
        assert saved_lots[0]["order_id"] == 1
        assert saved_lots[0]["price"] == 100.0
        print("Open lots ledger correctly persisted and reloaded across transactions")

        # Verify hedge mode open positions and incomplete flag
        base = dict(date="20260926", symbol="BTC", entry_time="2026-09-20 00:00:00",
                    entry_price=0, qty=1, entry_amount=0, order_id=0, is_incomplete=True)
        repo.save_open_positions([{**base, "side": "LONG"}, {**base, "side": "SHORT"}])
        conn = db._get_connection()
        try:
            rows = [dict(row) for row in conn.execute("SELECT * FROM open_positions")]
        finally:
            conn.close()
        assert len(rows) == 2 and {r["side"] for r in rows} == {"LONG", "SHORT"}
        assert all(r.get("is_incomplete") == 1 for r in rows)
        print("Hedge positions preserved and incomplete flag recorded correctly")


if __name__ == "__main__":
    main()
