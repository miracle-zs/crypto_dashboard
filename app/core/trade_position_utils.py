from datetime import datetime
from typing import Callable, Dict, List, Optional


def format_holding_time_cn(duration_seconds: float) -> str:
    if duration_seconds < 60:
        return f"{int(duration_seconds)}秒"
    if duration_seconds < 3600:
        minutes = int(duration_seconds // 60)
        seconds = int(duration_seconds % 60)
        return f"{minutes}分{seconds}秒"
    if duration_seconds < 86400:
        hours = int(duration_seconds // 3600)
        minutes = int((duration_seconds % 3600) // 60)
        return f"{hours}小时{minutes}分"
    days = int(duration_seconds // 86400)
    hours = int((duration_seconds % 86400) // 3600)
    return f"{days}天{hours}小时"


def position_to_trade_row(
    pos: Dict,
    idx: int,
    open_price_cache: Dict[tuple[str, int], Optional[float]],
    *,
    utc8,
    get_utc_day_start: Callable[[int], int],
) -> Dict:
    symbol = pos["symbol"]
    entry_time = pos["entry_time"]
    exit_time = pos["exit_time"]
    day_start_ms = get_utc_day_start(entry_time)
    open_price = open_price_cache.get((symbol, day_start_ms))

    entry_dt = datetime.fromtimestamp(entry_time / 1000, tz=utc8)
    exit_dt = datetime.fromtimestamp(exit_time / 1000, tz=utc8)
    duration_seconds = (exit_time - entry_time) / 1000
    hold_time = format_holding_time_cn(duration_seconds)

    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    entry_price = pos["entry_price"]
    qty = pos["qty"]
    pnl_net = round(pos["pnl"], 2)
    entry_amount = round(entry_price * qty, 2)

    if open_price and entry_price:
        price_change_pct = (entry_price - open_price) / open_price
    else:
        price_change_pct = 0

    if bool(pos.get("is_liquidation")):
        close_type = "爆仓"
    else:
        close_type = "止盈" if pnl_net > 0 else "止损"
    return_rate_raw = (pnl_net / entry_amount * 100) if entry_amount != 0 else 0

    return {
        "No": idx,
        "Date": entry_dt.strftime("%Y%m%d"),
        "Entry_Time": entry_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "Exit_Time": exit_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "Holding_Time": hold_time,
        "Symbol": base,
        "Side": pos["side"],
        "Price_Change_Pct": price_change_pct,
        "Entry_Amount": entry_amount,
        "Entry_Price": entry_price,
        "Exit_Price": pos["exit_price"],
        "Qty": qty,
        "Fees": round(pos.get("fees", 0), 2),
        "PNL_Net": pnl_net,
        "Close_Type": close_type,
        "Return_Rate": f"{return_rate_raw:.2f}%",
        "Open_Price": open_price if open_price else 0,
        "PNL_Before_Fees": round(pos.get("pnl_before_fees", pos["pnl"]), 2),
        "Entry_Order_ID": pos["entry_order_id"],
        "Exit_Order_ID": pos["exit_order_id"],
    }


def consume_fifo_entries(entries: List[Dict], qty_to_close: float):
    remaining = qty_to_close
    while remaining > 0 and entries:
        entry = entries[0]
        close_qty = min(remaining, entry["qty"])
        entry["qty"] -= close_qty
        remaining -= close_qty
        if entry["qty"] <= 0.0001:
            entries.pop(0)


def trim_entries_to_target_qty(entries: List[Dict], target_qty: float):
    calculated_qty = sum(e["qty"] for e in entries)
    if calculated_qty <= target_qty:
        return
    diff = calculated_qty - target_qty
    while diff > 0.0001 and entries:
        entry = entries[0]
        if entry["qty"] > diff:
            entry["qty"] -= diff
            diff = 0
        else:
            diff -= entry["qty"]
            entries.pop(0)


def build_open_position_output(symbol: str, side_str: str, entries: List[Dict], *, utc8) -> List[Dict]:
    base = symbol[:-4] if symbol.endswith("USDT") else symbol
    output = []
    for entry in entries:
        if entry["qty"] > 0.0001:
            dt = datetime.fromtimestamp(entry["time"] / 1000, tz=utc8)
            output.append(
                {
                    "date": dt.strftime("%Y%m%d"),
                    "symbol": base,
                    "side": side_str,
                    "entry_time": dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "entry_price": entry["price"],
                    "qty": entry["qty"],
                    "entry_amount": round(entry["price"] * entry["qty"]),
                    "order_id": entry["order_id"],
                }
            )
    return output
