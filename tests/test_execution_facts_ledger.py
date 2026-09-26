from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from app.database import Database
from app.repositories.sync_write_repository import SyncWriteRepository


def test_execution_facts_idempotency_and_retrieval():
    """Verify execution facts are stored idempotently without duplicates on re-insertion."""
    with TemporaryDirectory() as tmp:
        db = Database(str(Path(tmp) / "probe.db"))
        repo = SyncWriteRepository(db)

        raw_trades = [
            {
                "symbol": "BTCUSDT",
                "id": 1001,
                "orderId": 5001,
                "side": "BUY",
                "positionSide": "BOTH",
                "price": "60000.0",
                "qty": "0.1",
                "realizedPnl": "0.0",
                "quoteQty": "6000.0",
                "commission": "1.2",
                "commissionAsset": "USDT",
                "time": 1780000000000,
                "buyer": True,
                "maker": False,
            },
            {
                "symbol": "BTCUSDT",
                "id": 1002,
                "orderId": 5002,
                "side": "SELL",
                "positionSide": "BOTH",
                "price": "61000.0",
                "qty": "0.1",
                "realizedPnl": "100.0",
                "quoteQty": "6100.0",
                "commission": "1.22",
                "commissionAsset": "USDT",
                "time": 1780000060000,
                "buyer": False,
                "maker": True,
            },
        ]

        # First insert
        count1 = repo.save_execution_facts(raw_trades)
        assert count1 == 2

        # Re-insert the same trades (idempotency check)
        count2 = repo.save_execution_facts(raw_trades)
        assert count2 == 0  # no new rows inserted

        # Query back
        facts = repo.get_execution_facts(symbol="BTCUSDT")
        assert len(facts) == 2
        f1, f2 = facts[0], facts[1]
        assert f1["trade_id"] == 1001
        assert f1["order_id"] == 5001
        assert f1["commission"] == 1.2
        assert f2["trade_id"] == 1002
        assert f2["realized_pnl"] == 100.0


def test_income_facts_idempotency_and_retrieval():
    """Verify income facts (funding fees, commissions, transfers) are stored idempotently."""
    with TemporaryDirectory() as tmp:
        db = Database(str(Path(tmp) / "probe.db"))
        repo = SyncWriteRepository(db)

        income_records = [
            {
                "symbol": "BTCUSDT",
                "incomeType": "FUNDING_FEE",
                "income": "-2.50",
                "asset": "USDT",
                "time": 1780000000000,
                "tranId": 9001,
                "tradeId": None,
                "info": "Funding fee",
            },
            {
                "symbol": "BTCUSDT",
                "incomeType": "FUNDING_FEE",
                "income": "-2.75",
                "asset": "USDT",
                "time": 1780028800000,
                "tranId": 9002,
                "tradeId": None,
                "info": "Funding fee",
            },
        ]

        count1 = repo.save_income_facts(income_records)
        assert count1 == 2

        # Re-insert idempotency
        count2 = repo.save_income_facts(income_records)
        assert count2 == 0

        facts = repo.get_income_facts(symbol="BTCUSDT", income_type="FUNDING_FEE")
        assert len(facts) == 2
        assert facts[0]["tran_id"] == 9001
        assert facts[0]["income"] == -2.50
        assert facts[1]["tran_id"] == 9002
