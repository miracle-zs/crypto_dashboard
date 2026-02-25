from app.services.trade_income_aggregation import (
    aggregate_fee_totals_by_symbol,
    summarize_income_records,
)


def test_summarize_income_records_collects_symbols_and_tracked_costs():
    records = [
        {"symbol": "BTCUSDT", "incomeType": "COMMISSION", "income": "-1.0"},
        {"symbol": "BTCUSDT", "incomeType": "INSURANCE_CLEAR", "income": "-9.5"},
        {"symbol": "BTCUSDT", "incomeType": "REALIZED_PNL", "income": "-30.0"},
        {"symbol": "ETHUSDT", "incomeType": "FUNDING_FEE", "income": "-0.2"},
        {"symbol": "", "incomeType": "COMMISSION", "income": "-99"},
    ]

    symbols, totals = summarize_income_records(
        records,
        extra_loss_income_types={"insurance_clear"},
    )

    assert set(symbols) == {"BTCUSDT", "ETHUSDT"}
    assert totals["BTCUSDT"] == -10.5
    assert totals["ETHUSDT"] == -0.2


def test_aggregate_fee_totals_by_symbol_ignores_untracked_income_types():
    records = [
        {"symbol": "BTCUSDT", "incomeType": "COMMISSION", "income": "-1.0"},
        {"symbol": "BTCUSDT", "incomeType": "REALIZED_PNL", "income": "8.0"},
        {"symbol": "ETHUSDT", "incomeType": "FUNDING_FEE", "income": "-0.5"},
        {"symbol": "ETHUSDT", "incomeType": "INSURANCE_CLEAR", "income": "-0.3"},
        {"symbol": None, "incomeType": "COMMISSION", "income": "-1.1"},
    ]

    totals = aggregate_fee_totals_by_symbol(
        records,
        extra_loss_income_types={"INSURANCE_CLEAR"},
    )

    assert totals == {"BTCUSDT": -1.0, "ETHUSDT": -0.8}
