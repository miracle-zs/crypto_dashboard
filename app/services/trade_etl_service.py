import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

import pandas as pd

from app.logger import logger
from app.services.trade_income_aggregation import summarize_income_records


def extract_symbol_closed_positions(
    processor,
    *,
    symbol: str,
    since: int,
    until: int,
    use_time_filter: bool = True,
    fee_totals_by_symbol: Optional[Dict[str, float]] = None,
) -> tuple[List[Dict], float]:
    started_at = time.perf_counter()
    worker_client = processor._create_worker_client()

    if use_time_filter:
        orders = processor.get_all_orders(
            symbol,
            limit=1000,
            start_time=since,
            end_time=until,
            client=worker_client,
            fail_on_error=True,
        )
    else:
        orders = processor.get_all_orders(
            symbol,
            limit=1000,
            start_time=None,
            end_time=None,
            client=worker_client,
        )

    if not orders:
        return [], time.perf_counter() - started_at

    filled_orders = [order for order in orders if float(order["executedQty"]) > 0 and order["updateTime"] >= since]
    if len(filled_orders) < 1:
        return [], time.perf_counter() - started_at
    filled_orders.sort(key=lambda item: item["updateTime"])

    if fee_totals_by_symbol is not None:
        symbol_total_fees = fee_totals_by_symbol.get(symbol, 0.0)
        fees_map = {0: symbol_total_fees} if symbol_total_fees != 0 else {}
    else:
        fees_map = processor.get_fees_for_symbol(symbol, since, until, client=worker_client)

    positions = processor.match_orders_to_positions(filled_orders, symbol, fees_map, presorted=True)
    return positions, time.perf_counter() - started_at


def analyze_orders(
    processor,
    *,
    since: int,
    until: int,
    traded_symbols: Optional[List[str]] = None,
    use_time_filter: bool = True,
    symbol_since_map: Optional[Dict[str, int]] = None,
    prefetched_fee_totals: Optional[Dict[str, float]] = None,
    return_symbol_status: bool = False,
) -> pd.DataFrame | Tuple[pd.DataFrame, List[str], Dict[str, str]]:
    local_prefetched_fee_totals = prefetched_fee_totals
    if traded_symbols is None:
        income_records = processor._fetch_income_history(since=since, until=until)
        traded_symbols, local_prefetched_fee_totals = summarize_income_records(
            income_records,
            extra_loss_income_types=getattr(processor, "extra_loss_income_types", None),
        )
        logger.info(f"Income prefetch: records={len(income_records)}, symbols={len(traded_symbols)}")

    if not traded_symbols:
        logger.warning("No trading history found in the specified period")
        empty_df = pd.DataFrame()
        if return_symbol_status:
            return empty_df, [], {}
        return empty_df

    symbols = sorted(set(traded_symbols))
    logger.info(f"Analyzing {len(symbols)} symbols...")

    all_positions: List[Dict] = []
    worker_count = processor._resolve_workers(len(symbols))
    logger.info(f"ETL worker count: {worker_count}")
    success_count = 0
    failure_count = 0
    symbol_timings: List[tuple[str, float, int]] = []
    success_symbols: List[str] = []
    failure_symbols: Dict[str, str] = {}

    fee_prefetch_started = time.perf_counter()
    fee_totals_by_symbol = (
        local_prefetched_fee_totals
        if local_prefetched_fee_totals is not None
        else processor.get_fee_totals_by_symbol(since=since, until=until)
    )
    fee_prefetch_elapsed = time.perf_counter() - fee_prefetch_started
    logger.info(f"Income fee cache: symbols={len(fee_totals_by_symbol)}, elapsed={fee_prefetch_elapsed:.2f}s")

    if worker_count == 1:
        for idx, symbol in enumerate(symbols, 1):
            logger.info(f"[{idx}/{len(symbols)}] Processing {symbol}...")
            try:
                symbol_since = symbol_since_map.get(symbol, since) if symbol_since_map else since
                positions, elapsed = processor._extract_symbol_closed_positions(
                    symbol=symbol,
                    since=symbol_since,
                    until=until,
                    use_time_filter=use_time_filter,
                    fee_totals_by_symbol=fee_totals_by_symbol,
                )
                symbol_timings.append((symbol, elapsed, len(positions)))
                success_count += 1
                success_symbols.append(symbol)
                if positions:
                    all_positions.extend(positions)
                    logger.debug(f"Found {len(positions)} closed positions for {symbol}")
            except Exception as exc:
                failure_count += 1
                failure_symbols[symbol] = str(exc)
                logger.error(f"Processing {symbol} failed: {exc}")
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(
                    processor._extract_symbol_closed_positions,
                    symbol,
                    symbol_since_map.get(symbol, since) if symbol_since_map else since,
                    until,
                    use_time_filter,
                    fee_totals_by_symbol,
                ): symbol
                for symbol in symbols
            }

            for idx, future in enumerate(as_completed(future_map), 1):
                symbol = future_map[future]
                try:
                    positions, elapsed = future.result()
                    symbol_timings.append((symbol, elapsed, len(positions)))
                    success_count += 1
                    success_symbols.append(symbol)
                    if positions:
                        all_positions.extend(positions)
                        logger.debug(f"Found {len(positions)} closed positions for {symbol}")
                    logger.info(f"[{idx}/{len(symbols)}] Processed {symbol}")
                except Exception as exc:
                    failure_count += 1
                    failure_symbols[symbol] = str(exc)
                    logger.error(f"[{idx}/{len(symbols)}] Processing {symbol} failed: {exc}")

    if symbol_timings:
        slowest = sorted(symbol_timings, key=lambda item: item[1], reverse=True)[:5]
        slowest_str = ", ".join(f"{symbol}:{elapsed:.2f}s/{count}" for symbol, elapsed, count in slowest)
        logger.info(
            f"Closed ETL stats: success={success_count}, failed={failure_count}, "
            f"workers={worker_count}, slowest=[{slowest_str}]"
        )
    elif symbols:
        logger.info(f"Closed ETL stats: success={success_count}, failed={failure_count}, workers={worker_count}")

    logger.info(f"Found {len(all_positions)} closed positions total")

    if not all_positions:
        logger.warning("No closed positions found")
        empty_df = pd.DataFrame()
        if return_symbol_status:
            return empty_df, success_symbols, failure_symbols
        return empty_df

    price_prefetch_started = time.perf_counter()
    price_keys = sorted({(pos["symbol"], processor.get_utc_day_start(pos["entry_time"])) for pos in all_positions})
    open_price_cache: Dict[tuple[str, int], Optional[float]] = {}
    price_timings: List[tuple[str, float, bool]] = []
    price_worker_count = processor._resolve_workers(len(price_keys), max_workers=processor.max_price_workers)

    if price_worker_count == 1:
        for symbol, day_start_ms in price_keys:
            fetched_symbol, fetched_day_start, price, elapsed = processor._fetch_open_price_for_key(
                symbol=symbol,
                utc_day_start_ms=day_start_ms,
            )
            open_price_cache[(fetched_symbol, fetched_day_start)] = price
            price_timings.append((fetched_symbol, elapsed, price is not None))
    else:
        with ThreadPoolExecutor(max_workers=price_worker_count) as executor:
            future_map = {
                executor.submit(processor._fetch_open_price_for_key, symbol, day_start_ms): (symbol, day_start_ms)
                for symbol, day_start_ms in price_keys
            }
            for future in as_completed(future_map):
                symbol, day_start_ms = future_map[future]
                try:
                    fetched_symbol, fetched_day_start, price, elapsed = future.result()
                    open_price_cache[(fetched_symbol, fetched_day_start)] = price
                    price_timings.append((fetched_symbol, elapsed, price is not None))
                except Exception:
                    open_price_cache[(symbol, day_start_ms)] = None
                    price_timings.append((symbol, 0.0, False))

    price_prefetch_elapsed = time.perf_counter() - price_prefetch_started
    if price_keys:
        hit_count = sum(1 for _symbol, _elapsed, ok in price_timings if ok)
        slowest_price = sorted(price_timings, key=lambda item: item[1], reverse=True)[:5]
        slowest_price_str = ", ".join(f"{symbol}:{elapsed:.2f}s" for symbol, elapsed, _ok in slowest_price)
        logger.info(
            f"Open price cache: keys={len(price_keys)}, hits={hit_count}, workers={price_worker_count}, "
            f"elapsed={price_prefetch_elapsed:.2f}s, slowest=[{slowest_price_str}]"
        )

    transform_started = time.perf_counter()
    df = processor._build_trades_dataframe(all_positions=all_positions, open_price_cache=open_price_cache)
    df = df.sort_values("Entry_Time", ascending=True)

    logger.info(f"Successfully parsed {len(df)} positions")
    logger.info("Post-processing: Merging positions with same entry time...")
    df = processor.merge_same_entry_positions(df)
    logger.info(f"After merging: {len(df)} positions")
    transform_elapsed = time.perf_counter() - transform_started
    logger.info(
        f"Closed ETL transform stats: prefetch={price_prefetch_elapsed:.2f}s, transform={transform_elapsed:.2f}s"
    )

    if return_symbol_status:
        return df, success_symbols, failure_symbols
    return df


def extract_open_positions_for_symbol(
    processor,
    *,
    symbol: str,
    real_net_qty: float,
    since: int,
    until: int,
) -> tuple[List[Dict], float]:
    started_at = time.perf_counter()
    worker_client = processor._create_worker_client()
    orders = processor.get_all_orders(
        symbol,
        limit=1000,
        start_time=since,
        end_time=until,
        client=worker_client,
    )
    if not orders:
        return [], time.perf_counter() - started_at

    filled_orders = [order for order in orders if float(order["executedQty"]) > 0 and order["updateTime"] >= since]
    long_entries = []
    short_entries = []

    for order in sorted(filled_orders, key=lambda item: item["updateTime"]):
        side = order["side"]
        position_side = order["positionSide"]
        qty = float(order["executedQty"])
        price = float(order["avgPrice"])
        order_time = order["updateTime"]
        order_id = order["orderId"]

        if (position_side == "LONG" and side == "BUY") or (position_side == "BOTH" and side == "BUY"):
            long_entries.append({"price": price, "qty": qty, "time": order_time, "order_id": order_id})
        elif (position_side == "LONG" and side == "SELL") or (position_side == "BOTH" and side == "SELL"):
            processor._consume_fifo_entries(long_entries, qty)

        if position_side == "SHORT" and side == "SELL":
            short_entries.append({"price": price, "qty": qty, "time": order_time, "order_id": order_id})
        elif position_side == "SHORT" and side == "BUY":
            processor._consume_fifo_entries(short_entries, qty)

    final_entries = []
    if real_net_qty > 0:
        calculated_qty = sum(entry["qty"] for entry in long_entries)
        if abs(calculated_qty - real_net_qty) > 0.0001:
            logger.warning(f"{symbol} Qty Mismatch: Real={real_net_qty}, Calc={calculated_qty}. Adjusting to Real.")
            if calculated_qty > real_net_qty:
                processor._trim_entries_to_target_qty(long_entries, real_net_qty)
        final_entries = long_entries
    elif real_net_qty < 0:
        abs_real_qty = abs(real_net_qty)
        calculated_qty = sum(entry["qty"] for entry in short_entries)
        if abs(calculated_qty - abs_real_qty) > 0.0001:
            logger.warning(f"{symbol} Qty Mismatch: Real={real_net_qty}, Calc=-{calculated_qty}. Adjusting to Real.")
            if calculated_qty > abs_real_qty:
                processor._trim_entries_to_target_qty(short_entries, abs_real_qty)
        final_entries = short_entries

    side_str = "LONG" if real_net_qty > 0 else "SHORT"
    output = processor._build_open_position_output(symbol, side_str, final_entries)
    return output, time.perf_counter() - started_at


def get_open_positions(
    processor,
    *,
    since: int,
    until: int,
    traded_symbols: Optional[List[str]] = None,
) -> Optional[List[Dict]]:
    real_positions_map = processor.get_real_positions()
    if real_positions_map is None:
        logger.warning("Skip open positions refresh because PositionRisk failed")
        return None

    traded_symbols = traded_symbols or []
    all_target_symbols = set(traded_symbols) if traded_symbols else set()
    all_target_symbols.update(real_positions_map.keys())
    if not all_target_symbols:
        return []

    active_symbols = sorted(symbol for symbol in all_target_symbols if real_positions_map.get(symbol, 0.0) != 0)
    if not active_symbols:
        return []

    open_positions_workers = getattr(
        processor,
        "max_open_positions_workers",
        getattr(processor, "max_etl_workers", 1),
    )
    worker_count = processor._resolve_workers(len(active_symbols), max_workers=open_positions_workers)
    open_positions: List[Dict] = []
    success_count = 0
    failure_count = 0
    symbol_timings: List[tuple[str, float, int]] = []

    if worker_count == 1:
        for symbol in active_symbols:
            try:
                positions, elapsed = processor._extract_open_positions_for_symbol(
                    symbol=symbol,
                    real_net_qty=real_positions_map[symbol],
                    since=since,
                    until=until,
                )
                symbol_timings.append((symbol, elapsed, len(positions)))
                success_count += 1
                open_positions.extend(positions)
            except Exception as exc:
                failure_count += 1
                logger.error(f"Failed to extract open positions for {symbol}: {exc}")
        if symbol_timings:
            slowest = sorted(symbol_timings, key=lambda item: item[1], reverse=True)[:5]
            slowest_str = ", ".join(f"{symbol}:{elapsed:.2f}s/{count}" for symbol, elapsed, count in slowest)
            logger.info(
                f"Open ETL stats: success={success_count}, failed={failure_count}, "
                f"workers={worker_count}, slowest=[{slowest_str}]"
            )
        return open_positions

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_map = {
            executor.submit(
                processor._extract_open_positions_for_symbol,
                symbol,
                real_positions_map[symbol],
                since,
                until,
            ): symbol
            for symbol in active_symbols
        }
        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                positions, elapsed = future.result()
                symbol_timings.append((symbol, elapsed, len(positions)))
                success_count += 1
                open_positions.extend(positions)
            except Exception as exc:
                failure_count += 1
                logger.error(f"Failed to extract open positions for {symbol}: {exc}")

    if symbol_timings:
        slowest = sorted(symbol_timings, key=lambda item: item[1], reverse=True)[:5]
        slowest_str = ", ".join(f"{symbol}:{elapsed:.2f}s/{count}" for symbol, elapsed, count in slowest)
        logger.info(
            f"Open ETL stats: success={success_count}, failed={failure_count}, "
            f"workers={worker_count}, slowest=[{slowest_str}]"
        )
    return open_positions
