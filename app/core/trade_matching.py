from typing import Dict, List, Optional


def is_liquidation_order(order: Dict) -> bool:
    client_order_id = str(order.get("clientOrderId") or "").lower()
    order_type = str(order.get("type") or "").upper()
    return (
        client_order_id.startswith("autoclose-")
        or "adl_autoclose" in client_order_id
        or order_type == "LIQUIDATION"
    )


def match_position_side(orders: List[Dict], symbol: str, side: str) -> List[Dict]:
    """Match entry and exit orders for one position side."""
    positions = []
    open_positions = []

    for order in orders:
        if order["type"] == "entry":
            open_positions.append(
                {
                    "price": order["price"],
                    "qty": order["qty"],
                    "time": order["time"],
                    "order_id": order["order_id"],
                }
            )
        elif order["type"] == "exit" and open_positions:
            exit_qty_remaining = order["qty"]

            while exit_qty_remaining > 0 and open_positions:
                entry = open_positions[0]
                close_qty = min(exit_qty_remaining, entry["qty"])

                if side == "LONG":
                    pnl_before_fees = (order["price"] - entry["price"]) * close_qty
                else:  # SHORT
                    pnl_before_fees = (entry["price"] - order["price"]) * close_qty

                time_held = order["time"] - entry["time"]
                weight = close_qty * time_held

                positions.append(
                    {
                        "symbol": symbol,
                        "side": side,
                        "entry_price": entry["price"],
                        "exit_price": order["price"],
                        "entry_time": entry["time"],
                        "exit_time": order["time"],
                        "qty": close_qty,
                        "pnl_before_fees": pnl_before_fees,
                        "weight": weight,
                        "entry_order_id": entry["order_id"],
                        "exit_order_id": order["order_id"],
                        "is_liquidation": bool(order.get("is_liquidation", False)),
                    }
                )

                entry["qty"] -= close_qty
                exit_qty_remaining -= close_qty
                if entry["qty"] <= 0.0001:
                    open_positions.pop(0)

    return positions


def match_orders_to_positions(
    orders: List[Dict],
    symbol: str,
    fees_map: Optional[Dict[int, float]] = None,
    presorted: bool = False,
) -> List[Dict]:
    """Match orders to closed positions (handles partial fills)."""
    if fees_map is None:
        fees_map = {}

    total_fees = sum(fees_map.values())
    long_positions = []
    short_positions = []

    iterable_orders = orders if presorted else sorted(orders, key=lambda x: x["updateTime"])
    for order in iterable_orders:
        if float(order["executedQty"]) <= 0:
            continue

        side = order["side"]
        position_side = order["positionSide"]
        qty = float(order["executedQty"])
        price = float(order["avgPrice"])
        order_time = order["updateTime"]
        liquidation = is_liquidation_order(order)

        if (position_side == "LONG" and side == "BUY") or (position_side == "BOTH" and side == "BUY"):
            long_positions.append(
                {"type": "entry", "price": price, "qty": qty, "time": order_time, "order_id": order["orderId"]}
            )
        elif (position_side == "LONG" and side == "SELL") or (position_side == "BOTH" and side == "SELL"):
            long_positions.append(
                {
                    "type": "exit",
                    "price": price,
                    "qty": qty,
                    "time": order_time,
                    "order_id": order["orderId"],
                    "is_liquidation": liquidation,
                }
            )

        if position_side == "SHORT" and side == "SELL":
            short_positions.append(
                {"type": "entry", "price": price, "qty": qty, "time": order_time, "order_id": order["orderId"]}
            )
        elif position_side == "SHORT" and side == "BUY":
            short_positions.append(
                {
                    "type": "exit",
                    "price": price,
                    "qty": qty,
                    "time": order_time,
                    "order_id": order["orderId"],
                    "is_liquidation": liquidation,
                }
            )

    all_positions = match_position_side(long_positions, symbol, "LONG") + match_position_side(
        short_positions, symbol, "SHORT"
    )

    if all_positions:
        total_weight = sum(pos["weight"] for pos in all_positions)
        if total_weight > 0:
            for pos in all_positions:
                fee_allocation = (pos["weight"] / total_weight) * total_fees
                pos["fees"] = fee_allocation
                pos["pnl"] = pos["pnl_before_fees"] + fee_allocation

    return all_positions
