import time
from typing import Dict, List, Optional

from app.logger import logger


def fetch_income_history(
    *,
    client,
    since: int,
    until: int,
    income_type: Optional[str] = None,
) -> List[Dict]:
    endpoint = "/fapi/v1/income"
    records: List[Dict] = []
    current_start = since

    while True:
        params = {
            "startTime": current_start,
            "endTime": until,
            "limit": 1000,
        }
        if income_type:
            params["incomeType"] = income_type
        batch = client.signed_get(endpoint, params)
        if not batch:
            break

        records.extend(batch)
        if len(batch) < 1000:
            break

        last_time = int(batch[-1]["time"])
        current_start = last_time + 1
        time.sleep(0.2)

    return records


def fetch_account_balance(*, client) -> Optional[Dict[str, float]]:
    endpoint = "/fapi/v2/account"
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

        while True:
            params = {
                "symbol": symbol,
                "limit": limit,
                "startTime": window_start,
                "endTime": current_end,
            }

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

            last_update = int(batch[-1].get("updateTime", window_start))
            if last_update <= window_start:
                window_start += 1
            else:
                window_start = last_update + 1

            if window_start > current_end:
                break

        current_start = current_end + 1

    if not all_orders:
        logger.debug(f"No orders in time range for {symbol}")

    return all_orders


def fetch_real_positions(*, client) -> Optional[Dict[str, float]]:
    endpoint = "/fapi/v2/positionRisk"
    try:
        positions = client.signed_get(endpoint)
        if positions is None:
            logger.warning("PositionRisk request failed, returning None")
            return None

        real_pos = {}
        if positions:
            for position in positions:
                amt = float(position.get("positionAmt", 0))
                if abs(amt) > 0:
                    real_pos[position["symbol"]] = amt
        return real_pos
    except Exception as exc:
        logger.error(f"Failed to fetch position risk: {exc}")
        return None
