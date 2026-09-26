import time
from typing import Dict, List, Optional

from app.logger import logger


def fetch_income_history(
    *,
    client,
    since: int,
    until: int,
    income_type: Optional[str] = None,
    fail_on_error: bool = False,
) -> List[Dict]:
    endpoint = "/fapi/v1/income"
    records: List[Dict] = []
    seen_keys = set()
    max_iterations = 10000  # Safety limit to prevent infinite loops
    iterations = 0
    current_page = 1

    while iterations < max_iterations:
        iterations += 1
        params = {
            "startTime": since,
            "endTime": until,
            "limit": 1000,
            "page": current_page,
        }
        if income_type:
            params["incomeType"] = income_type
        batch = client.signed_get(endpoint, params)
        if batch is None:
            if fail_on_error:
                raise RuntimeError(
                    f"income request failed, window=[{since},{until}], income_type={income_type or 'ALL'}"
                )
            break
        if not batch:
            break

        # Deduplicate by (incomeType, tranId) to handle same-timestamp and different incomeType records
        new_records = []
        for record in batch:
            tran_id = record.get("tranId")
            if tran_id is not None:
                key = (record.get("incomeType"), str(tran_id))
                if key not in seen_keys:
                    seen_keys.add(key)
                    new_records.append(record)
            else:
                new_records.append(record)

        records.extend(new_records)
        if len(batch) < 1000:
            break

        current_page += 1
        time.sleep(0.2)

    if iterations >= max_iterations:
        logger.warning(
            f"Income history hit max iterations ({max_iterations}), "
            f"may have incomplete data"
        )

    return records


def fetch_account_balance(*, client) -> Optional[Dict[str, float]]:
    # v3 endpoint returns same top-level balance fields with better performance
    endpoint = "/fapi/v3/account"
    account_info = client.signed_get(endpoint)
    if account_info:
        return {
            "margin_balance": float(account_info.get("totalMarginBalance", 0)),
            "wallet_balance": float(account_info.get("totalWalletBalance", 0)),
        }
    logger.warning("Could not retrieve account balance.")
    return None


def fetch_all_orders(
    *,
    client,
    symbol: str,
    limit: int = 1000,
    start_time: int = None,
    end_time: int = None,
    fail_on_error: bool = False,
) -> List[Dict]:
    endpoint = "/fapi/v1/allOrders"
    params = {
        "symbol": symbol,
        "limit": limit,
    }

    if start_time is None and end_time is None:
        result = client.signed_get(endpoint, params)
        if result is None:
            if fail_on_error:
                raise RuntimeError(f"allOrders request failed for {symbol}")
            logger.warning(f"API request failed for {symbol}")
        elif isinstance(result, list) and len(result) == 0:
            logger.debug(f"No orders in time range for {symbol}")
        return result if result else []

    if start_time is None and end_time is not None:
        start_time = end_time - (7 * 24 * 60 * 60 * 1000) + 1
    if end_time is None and start_time is not None:
        end_time = int(time.time() * 1000)

    max_window_ms = (7 * 24 * 60 * 60 * 1000) - 1
    current_start = start_time
    all_orders: List[Dict] = []
    seen_order_ids = set()

    while current_start <= end_time:
        current_end = min(end_time, current_start + max_window_ms)
        window_start = current_start
        next_order_id = None

        while True:
            params = {
                "symbol": symbol,
                "limit": limit,
                "startTime": window_start,
                "endTime": current_end,
            }
            if next_order_id is not None:
                params["orderId"] = next_order_id

            batch = client.signed_get(endpoint, params)
            if batch is None:
                if fail_on_error:
                    raise RuntimeError(f"allOrders request failed for {symbol}, window=[{window_start},{current_end}]")
                batch = []
            if not batch:
                break

            for order in batch:
                order_id = order.get("orderId")
                if order_id in seen_order_ids:
                    continue
                seen_order_ids.add(order_id)
                all_orders.append(order)

            if len(batch) < limit:
                break

            last_order_id = batch[-1].get("orderId")
            if last_order_id is None:
                break
            candidate_order_id = int(last_order_id) + 1
            if next_order_id is not None and candidate_order_id <= next_order_id:
                break
            next_order_id = candidate_order_id

        current_start = current_end + 1

    if not all_orders:
        logger.debug(f"No orders in time range for {symbol}")

    return all_orders


def fetch_user_trades(
    *,
    client,
    symbol: str,
    limit: int = 1000,
    start_time: int = None,
    end_time: int = None,
    from_trade_id: int = None,
    fail_on_error: bool = False,
) -> List[Dict]:
    """Fetch account trades for one active symbol and deduplicate by trade ID."""
    endpoint = "/fapi/v1/userTrades"
    base_params = {"symbol": symbol, "limit": int(limit)}
    if start_time is None and end_time is None:
        if from_trade_id is not None:
            base_params["fromId"] = int(from_trade_id)
        result = client.signed_get(endpoint, base_params)
        if result is None:
            if fail_on_error:
                raise RuntimeError(f"userTrades request failed for {symbol}")
            return []
        seen = set()
        output = []
        for trade in result or []:
            trade_id = trade.get("id")
            if trade_id is not None and trade_id in seen:
                continue
            if trade_id is not None:
                seen.add(trade_id)
            output.append(trade)
        return output

    if start_time is None:
        start_time = max(0, int(end_time) - (7 * 24 * 60 * 60 * 1000) + 1)
    if end_time is None:
        end_time = int(time.time() * 1000)

    max_window_ms = (7 * 24 * 60 * 60 * 1000) - 1
    current_start = int(start_time)
    until = int(end_time)
    seen_trade_ids = set()
    trades: List[Dict] = []
    while current_start <= until:
        current_end = min(until, current_start + max_window_ms)
        next_trade_id = None
        while True:
            if next_trade_id is None:
                params = {
                    **base_params,
                    "startTime": current_start,
                    "endTime": current_end,
                }
            else:
                # Binance forbids combining fromId with startTime/endTime.
                params = {**base_params, "fromId": next_trade_id}
            batch = client.signed_get(endpoint, params)
            if batch is None:
                if fail_on_error:
                    raise RuntimeError(
                        f"userTrades request failed for {symbol}, window=[{current_start},{current_end}]"
                    )
                break
            for trade in batch or []:
                try:
                    trade_time = int(trade.get("time"))
                except (TypeError, ValueError):
                    trade_time = current_start
                if trade_time < current_start or trade_time > current_end:
                    continue
                trade_id = trade.get("id")
                if trade_id is not None and trade_id in seen_trade_ids:
                    continue
                if trade_id is not None:
                    seen_trade_ids.add(trade_id)
                trades.append(trade)
            if not batch or len(batch) < int(limit):
                break
            last_trade_id = batch[-1].get("id")
            if last_trade_id is None:
                break
            candidate_trade_id = int(last_trade_id) + 1
            if next_trade_id is not None and candidate_trade_id <= next_trade_id:
                break
            next_trade_id = candidate_trade_id
            try:
                if int(batch[-1].get("time")) > current_end:
                    break
            except (TypeError, ValueError):
                pass
        current_start = current_end + 1
    return trades


def fetch_real_positions(*, client) -> Optional[Dict[str, float]]:
    # v3 endpoint returns positions with open orders/positions only, better performance
    endpoint = "/fapi/v3/positionRisk"
    try:
        positions = client.signed_get(endpoint)
        if positions is None:
            logger.warning("PositionRisk request failed, returning None")
            return None

        real_pos = {}
        mark_prices = {}
        if positions:
            for position in positions:
                amt = float(position.get("positionAmt", 0))
                symbol = position.get("symbol")
                raw_mp = position.get("markPrice")
                if symbol and raw_mp is not None:
                    try:
                        mp = float(raw_mp)
                        if mp > 0:
                            mark_prices[symbol] = mp
                    except (TypeError, ValueError):
                        pass
                if abs(amt) > 0 and symbol:
                    pos_side = position.get("positionSide")
                    if pos_side and pos_side in ("LONG", "SHORT"):
                        key = (symbol, pos_side)
                    else:
                        key = symbol
                    real_pos[key] = amt
            active_risk_list = [p for p in positions if abs(float(p.get("positionAmt", 0))) > 0]
            risk_map = {}
            for p in active_risk_list:
                s = p.get("symbol")
                ps = p.get("positionSide")
                if s and ps in ("LONG", "SHORT"):
                    risk_map[(s, ps)] = p
                if s:
                    risk_map[s] = p
            setattr(client, "_latest_position_risk", active_risk_list)
            setattr(client, "_latest_position_risk_map", risk_map)
            if mark_prices:
                try:
                    from app.services.market_price_service import MarketPriceService
                    MarketPriceService.set_cached_prices(mark_prices)
                except Exception:
                    pass

        return real_pos
    except Exception as exc:
        logger.error(f"Failed to fetch position risk: {exc}")
        return None
