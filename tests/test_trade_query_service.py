import pandas as pd

from app.services.trade_query_service import TradeQueryService


def test_get_trades_list_parses_rows_and_duration():
    class FakeRepo:
        def get_all_trades(self):
            return pd.DataFrame(
                [
                    {
                        "No": 1,
                        "Date": "20260221",
                        "Entry_Time": "2026-02-21 10:00:00",
                        "Exit_Time": "2026-02-21 10:15:00",
                        "Holding_Time": "15分0秒",
                        "Symbol": "BTC",
                        "Side": "LONG",
                        "Price_Change_Pct": 0.1,
                        "Entry_Amount": 1000,
                        "Entry_Price": 100,
                        "Exit_Price": 110,
                        "Qty": 10,
                        "Fees": 1,
                        "PNL_Net": 99,
                        "Close_Type": "止盈",
                        "Return_Rate": "9.90%",
                        "Open_Price": 95,
                        "PNL_Before_Fees": 100,
                        "Entry_Order_ID": 11,
                        "Exit_Order_ID": "22",
                    },
                    {
                        "No": 2,
                        "Date": "20260221",
                        "Entry_Time": "bad-time",
                        "Exit_Time": "2026-02-21 10:30:00",
                        "Holding_Time": "0秒",
                        "Symbol": "ETH",
                        "Side": "SHORT",
                        "Price_Change_Pct": -0.1,
                        "Entry_Amount": 2000,
                        "Entry_Price": 200,
                        "Exit_Price": 180,
                        "Qty": 10,
                        "Fees": 2,
                        "PNL_Net": 198,
                        "Close_Type": "止盈",
                        "Return_Rate": "9.90%",
                        "Open_Price": 210,
                        "PNL_Before_Fees": 200,
                        "Entry_Order_ID": 33,
                        "Exit_Order_ID": "44",
                    },
                ]
            )

    service = TradeQueryService.__new__(TradeQueryService)
    service.repo = FakeRepo()

    trades = service.get_trades_list()

    assert len(trades) == 2
    assert trades[0].duration_minutes == 15.0
    assert trades[1].duration_minutes == 0.0


def test_get_summary_prefers_cached_total_count_fast_path():
    class FakeRepo:
        def get_trade_summary(self):
            return {
                "total_pnl": 1.0,
                "total_fees": 0.1,
                "win_rate": 50.0,
                "win_count": 1,
                "loss_count": 1,
                "total_trades": 2,
                "equity_curve": [1.0],
                "current_streak": 1,
                "best_win_streak": 1,
                "worst_loss_streak": -1,
                "max_single_loss": -1.0,
                "max_drawdown": -1.0,
                "profit_factor": 1.0,
                "kelly_criterion": 0.1,
                "sqn": 0.1,
                "expected_value": 0.1,
                "risk_reward_ratio": 1.0,
            }

        def get_cached_total_trades(self):
            return 2

        def get_statistics(self):
            raise AssertionError("should not call slow stats path")

        def recompute_trade_summary(self):
            raise AssertionError("should not recompute when counts match")

    service = TradeQueryService.__new__(TradeQueryService)
    service.repo = FakeRepo()

    summary = service.get_summary()
    assert summary.total_trades == 2
