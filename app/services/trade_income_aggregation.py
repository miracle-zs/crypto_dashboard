from typing import Dict, List, Optional, Tuple


def _normalize_extra_loss_income_types(extra_loss_income_types: Optional[set[str]]) -> set[str]:
    return {item.strip().upper() for item in (extra_loss_income_types or set()) if item}


def _build_tracked_cost_types(extra_loss_income_types: Optional[set[str]]) -> set[str]:
    extra_types = _normalize_extra_loss_income_types(extra_loss_income_types)
    return {"COMMISSION", "FUNDING_FEE", *extra_types}


def summarize_income_records(
    records: List[Dict], extra_loss_income_types: Optional[set[str]] = None
) -> Tuple[List[str], Dict[str, float]]:
    symbols = set()
    fee_totals: Dict[str, float] = {}
    tracked_cost_types = _build_tracked_cost_types(extra_loss_income_types)

    for record in records:
        symbol = record.get("symbol")
        if not symbol:
            continue
        symbols.add(symbol)
        income_type = str(record.get("incomeType") or "").upper()
        if income_type in tracked_cost_types:
            fee_totals[symbol] = fee_totals.get(symbol, 0.0) + float(record.get("income", 0.0))

    return list(symbols), fee_totals


def summarize_income_records_with_ranges(
    records: List[Dict], extra_loss_income_types: Optional[set[str]] = None
) -> Tuple[List[str], Dict[str, float], Dict[str, tuple[int, int]]]:
    symbols, fee_totals = summarize_income_records(
        records,
        extra_loss_income_types=extra_loss_income_types,
    )
    activity_ranges: Dict[str, tuple[int, int]] = {}
    for record in records:
        symbol = record.get("symbol")
        if not symbol:
            continue
        try:
            event_time = int(record.get("time"))
        except (TypeError, ValueError):
            continue
        current = activity_ranges.get(symbol)
        if current is None:
            activity_ranges[symbol] = (event_time, event_time)
        else:
            activity_ranges[symbol] = (min(current[0], event_time), max(current[1], event_time))
    return symbols, fee_totals, activity_ranges


def aggregate_fee_totals_by_symbol(
    records: List[Dict], extra_loss_income_types: Optional[set[str]] = None
) -> Dict[str, float]:
    tracked_cost_types = _build_tracked_cost_types(extra_loss_income_types)
    fee_totals: Dict[str, float] = {}

    for record in records:
        symbol = record.get("symbol")
        if not symbol:
            continue
        income_type = str(record.get("incomeType") or "").upper()
        if income_type not in tracked_cost_types:
            continue
        fee_totals[symbol] = fee_totals.get(symbol, 0.0) + float(record.get("income", 0.0))

    return fee_totals
