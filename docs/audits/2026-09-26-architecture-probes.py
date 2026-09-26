"""Offline characterization probes; no exchange calls or production DB writes.

Run from repository root with PYTHONPATH=. python <this-file>.
Assertions document current defects, not desired future contracts.
"""
from pathlib import Path
from tempfile import TemporaryDirectory

from app.core.trade_matching import match_orders_to_positions
from app.database import Database
from app.repositories.sync_write_repository import SyncWriteRepository


def order(oid, side, time, price):
    return dict(orderId=oid, side=side, positionSide="BOTH", executedQty="1",
                avgPrice=str(price), updateTime=time, type="MARKET")


def main():
    orders = [order(1, "BUY", 1000, 100), order(2, "SELL", 2000, 110),
              order(3, "BUY", 3000, 100), order(4, "SELL", 6000, 110)]
    whole = match_orders_to_positions(orders, "BTCUSDT", {0: -4})
    split = (match_orders_to_positions(orders[:2], "BTCUSDT", {0: -2})
             + match_orders_to_positions(orders[2:], "BTCUSDT", {0: -2}))
    assert [x["fees"] for x in whole] == [-1, -3]
    assert [x["fees"] for x in split] == [-2, -2]
    print("Fee allocation depends on batch: whole=[-1,-3], split=[-2,-2]")
    assert len(match_orders_to_positions(orders[:2], "BTCUSDT")) == 1
    assert match_orders_to_positions(orders[1:2], "BTCUSDT") == []
    print("Matcher cannot resume from exit-only input without prior position state")

    with TemporaryDirectory() as tmp:
        db = Database(str(Path(tmp) / "probe.db"))
        repo = SyncWriteRepository(db)
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
