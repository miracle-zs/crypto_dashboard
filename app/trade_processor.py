#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance Futures Order Analyzer
Analyze trading history based on orders (not individual trades)
"""

import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from app.logger import logger
from app.binance_client import BinanceFuturesRestClient

# 定义北京时区 UTC+8
UTC8 = timezone(timedelta(hours=8))


class TradeDataProcessor:
    """Binance Order Analyzer"""

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = BinanceFuturesRestClient(api_key=api_key, api_secret=api_secret)
        self.max_etl_workers = self._load_max_workers()
        self.max_price_workers = self._load_price_workers()

    def _load_max_workers(self) -> int:
        raw = os.getenv('ETL_MAX_WORKERS', '2')
        try:
            workers = int(raw)
            if workers < 1:
                raise ValueError("must be >= 1")
            return workers
        except Exception:
            logger.warning(f"Invalid ETL_MAX_WORKERS={raw}, fallback to 2")
            return 2

    def _load_price_workers(self) -> int:
        raw = os.getenv('ETL_PRICE_WORKERS', str(self.max_etl_workers))
        try:
            workers = int(raw)
            if workers < 1:
                raise ValueError("must be >= 1")
            return workers
        except Exception:
            logger.warning(f"Invalid ETL_PRICE_WORKERS={raw}, fallback to {self.max_etl_workers}")
            return self.max_etl_workers

    def _resolve_workers(self, task_count: int, max_workers: Optional[int] = None) -> int:
        cap = self.max_etl_workers if max_workers is None else max_workers
        if task_count <= 1:
            return 1
        return min(cap, task_count)

    def _create_worker_client(self) -> BinanceFuturesRestClient:
        return BinanceFuturesRestClient(api_key=self.api_key, api_secret=self.api_secret)

    def _fetch_open_price_for_key(
        self,
        symbol: str,
        utc_day_start_ms: int
    ) -> tuple[str, int, Optional[float], float]:
        started_at = time.perf_counter()
        worker_client = self._create_worker_client()
        try:
            kline_start = self.get_kline_data(
                symbol=symbol,
                interval='1h',
                start_time=utc_day_start_ms,
                limit=1,
                client=worker_client
            )
            if not kline_start:
                return symbol, utc_day_start_ms, None, time.perf_counter() - started_at
            open_price = float(kline_start[0][1])
            return symbol, utc_day_start_ms, open_price, time.perf_counter() - started_at
        except Exception:
            return symbol, utc_day_start_ms, None, time.perf_counter() - started_at

    def get_recent_financial_flow(self, start_time: int) -> float:
        """
        Calculate the sum of Realized PnL, Commission, and Funding Fees since start_time.
        Used to distinguish between trading PnL and Deposit/Withdrawal.
        """
        endpoint = '/fapi/v1/income'
        params = {
            'startTime': start_time,
            'limit': 1000
        }

        income_data = self.client.signed_get(endpoint, params)
        if not income_data:
            return 0.0

        flow = 0.0
        for item in income_data:
            # TRANSFER is explicitly excluded here because we want to detect it via balance diff
            # (or we could rely on this if it's reliable, but user asked for balance diff)
            # We want to know how much balance change is EXPLAINED by trading.
            if item['incomeType'] in ['REALIZED_PNL', 'COMMISSION', 'FUNDING_FEE']:
                flow += float(item['income'])

        return flow

    def _fetch_income_history(
        self,
        since: int,
        until: int,
        client: Optional[BinanceFuturesRestClient] = None,
        income_type: Optional[str] = None,
    ) -> List[Dict]:
        """
        Fetch income history once in a paginated way for a time window.
        """
        client = client or self.client
        endpoint = '/fapi/v1/income'
        records: List[Dict] = []
        current_start = since

        while True:
            params = {
                'startTime': current_start,
                'endTime': until,
                'limit': 1000
            }
            if income_type:
                params['incomeType'] = income_type
            batch = client.signed_get(endpoint, params)
            if not batch:
                break

            records.extend(batch)
            if len(batch) < 1000:
                break

            last_time = int(batch[-1]['time'])
            current_start = last_time + 1
            time.sleep(0.2)

        return records

    def get_transfer_income_records(self, start_time: int, end_time: Optional[int] = None) -> List[Dict]:
        """
        直接从 Binance income 接口拉取 TRANSFER 出入金记录。

        Returns:
            List[Dict]: 每条包含 amount/event_time_ms/asset/income_type/source_uid/description
        """
        if start_time is None:
            return []

        now_ms = int(time.time() * 1000)
        until = int(end_time if end_time is not None else now_ms)
        if start_time > until:
            return []

        records = self._fetch_income_history(
            since=int(start_time),
            until=until,
            income_type='TRANSFER'
        )
        if not records:
            return []

        normalized = []
        seen = set()

        for record in records:
            income_type = str(record.get('incomeType') or '').upper()
            if income_type != 'TRANSFER':
                continue

            try:
                event_time_ms = int(record.get('time'))
                amount = float(record.get('income', 0.0))
            except (TypeError, ValueError):
                continue

            asset = str(record.get('asset') or 'USDT').upper()
            tran_id = str(record.get('tranId') or '').strip()
            info = str(record.get('info') or '').strip()
            symbol = str(record.get('symbol') or '').strip().upper()
            source_uid = (
                f"TRANSFER:{tran_id}"
                if tran_id
                else f"TRANSFER:{event_time_ms}:{asset}:{amount:.8f}:{symbol}:{info}"
            )
            if source_uid in seen:
                continue
            seen.add(source_uid)

            normalized.append({
                "amount": amount,
                "event_time_ms": event_time_ms,
                "asset": asset,
                "income_type": income_type,
                "source_uid": source_uid,
                "description": f"Binance income {income_type}{f' ({info})' if info else ''}".strip()
            })

        normalized.sort(key=lambda x: x["event_time_ms"])
        return normalized

    def get_account_balance(self) -> Dict[str, float]:
        """Get USD-M Futures account balance (Margin & Wallet)."""
        endpoint = '/fapi/v2/account'
        account_info = self.client.signed_get(endpoint)

        if account_info:
            return {
                'margin_balance': float(account_info.get('totalMarginBalance', 0)),
                'wallet_balance': float(account_info.get('totalWalletBalance', 0))
            }
        else:
            logger.warning("Could not retrieve account balance.")
            return None

    def get_all_orders(
        self,
        symbol: str,
        limit: int = 1000,
        start_time: int = None,
        end_time: int = None,
        client: Optional[BinanceFuturesRestClient] = None,
    ) -> List[Dict]:
        """Get all orders for a symbol"""
        client = client or self.client
        endpoint = '/fapi/v1/allOrders'
        params = {
            'symbol': symbol,
            'limit': limit
        }

        # No time filter: one-shot fetch (Binance default window)
        if start_time is None and end_time is None:
            result = client.signed_get(endpoint, params)
            if result is None:
                logger.warning(f"API request failed for {symbol}")
            elif isinstance(result, list) and len(result) == 0:
                logger.debug(f"No orders in time range for {symbol}")
            return result if result else []

        # Ensure time range
        if start_time is None and end_time is not None:
            # fallback to last 7 days window
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
                    'symbol': symbol,
                    'limit': limit,
                    'startTime': window_start,
                    'endTime': current_end
                }

                batch = client.signed_get(endpoint, params) or []
                if not batch:
                    break

                for order in batch:
                    order_id = order.get('orderId')
                    if order_id in seen_order_ids:
                        continue
                    seen_order_ids.add(order_id)
                    all_orders.append(order)

                if len(batch) < limit:
                    break

                last_update = int(batch[-1].get('updateTime', window_start))
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

    def get_exchange_info(self, client: Optional[BinanceFuturesRestClient] = None) -> dict:
        """Get exchange information"""
        client = client or self.client
        endpoint = '/fapi/v1/exchangeInfo'
        result = client.public_get(endpoint)
        return result or {}

    def get_all_symbols(self, client: Optional[BinanceFuturesRestClient] = None) -> List[str]:
        """Get all USDT perpetual contract symbols"""
        info = self.get_exchange_info(client=client)
        if not info or 'symbols' not in info:
            return []

        symbols = []
        for s in info['symbols']:
            if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT':
                symbols.append(s['symbol'])

        return symbols

    def get_kline_data(
        self,
        symbol: str,
        interval: str,
        start_time: int,
        limit: int = 1,
        client: Optional[BinanceFuturesRestClient] = None
    ) -> List:
        """Get kline/candlestick data"""
        client = client or self.client
        endpoint = '/fapi/v1/klines'
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_time,
            'limit': limit
        }

        result = client.public_get(endpoint, params)
        return result or []

    def get_utc_day_start(self, timestamp: int) -> int:
        """Get UTC+0 00:00 timestamp for given timestamp"""
        dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(day_start.timestamp() * 1000)

    def get_price_change_from_utc_start(
        self,
        symbol: str,
        timestamp: int,
        client: Optional[BinanceFuturesRestClient] = None
    ) -> Optional[float]:
        """Get the opening price for the UTC day of the given timestamp
        
        Returns:
            float: opening price or None if failed
        """
        try:
            day_start = self.get_utc_day_start(timestamp)

            kline_start = self.get_kline_data(symbol, '1h', day_start, 1, client=client)
            if not kline_start:
                return None

            open_price = float(kline_start[0][1])
            return open_price

        except Exception as e:
            return None

    def get_fees_for_symbol(
        self,
        symbol: str,
        since: int,
        until: int,
        client: Optional[BinanceFuturesRestClient] = None
    ) -> Dict[int, float]:
        """Get commission and funding fees for a symbol (fallback helper)."""
        records = self._fetch_income_history(since=since, until=until, client=client)
        fees_map = {}

        if records:
            for record in records:
                if record.get('symbol') != symbol:
                    continue
                if record.get('incomeType') in ['COMMISSION', 'FUNDING_FEE']:
                    time_ts = int(record['time'])
                    income = float(record['income'])
                    fees_map[time_ts] = fees_map.get(time_ts, 0) + income

        return fees_map

    def get_fee_totals_by_symbol(
        self,
        since: int,
        until: int,
        client: Optional[BinanceFuturesRestClient] = None
    ) -> Dict[str, float]:
        """
        Batch fetch income once and aggregate commission/funding by symbol.
        """
        records = self._fetch_income_history(since=since, until=until, client=client)
        fee_totals: Dict[str, float] = {}

        for record in records:
            symbol = record.get('symbol')
            if not symbol:
                continue
            if record.get('incomeType') not in ['COMMISSION', 'FUNDING_FEE']:
                continue
            income = float(record.get('income', 0.0))
            fee_totals[symbol] = fee_totals.get(symbol, 0.0) + income

        return fee_totals

    def match_orders_to_positions(self, orders: List[Dict], symbol: str, fees_map: Dict[int, float] = None) -> List[Dict]:
        """Match orders to closed positions (handles partial fills)"""

        if fees_map is None:
            fees_map = {}

        total_fees = sum(fees_map.values())

        # Separate LONG and SHORT positions
        long_positions = []
        short_positions = []

        for order in sorted(orders, key=lambda x: x['updateTime']):
            if float(order['executedQty']) <= 0:
                continue

            side = order['side']
            position_side = order['positionSide']
            qty = float(order['executedQty'])
            price = float(order['avgPrice'])
            order_time = order['updateTime']

            # Handle LONG positions
            if (position_side == 'LONG' and side == 'BUY') or \
               (position_side == 'BOTH' and side == 'BUY'):
                long_positions.append({
                    'type': 'entry',
                    'price': price,
                    'qty': qty,
                    'time': order_time,
                    'order_id': order['orderId']
                })
            elif (position_side == 'LONG' and side == 'SELL') or \
                 (position_side == 'BOTH' and side == 'SELL'):
                long_positions.append({
                    'type': 'exit',
                    'price': price,
                    'qty': qty,
                    'time': order_time,
                    'order_id': order['orderId']
                })

            # Handle SHORT positions
            if position_side == 'SHORT' and side == 'SELL':
                short_positions.append({
                    'type': 'entry',
                    'price': price,
                    'qty': qty,
                    'time': order_time,
                    'order_id': order['orderId']
                })
            elif position_side == 'SHORT' and side == 'BUY':
                short_positions.append({
                    'type': 'exit',
                    'price': price,
                    'qty': qty,
                    'time': order_time,
                    'order_id': order['orderId']
                })

        # Match LONG positions
        long_positions_matched = self._match_position_side(long_positions, symbol, 'LONG')

        # Match SHORT positions
        short_positions_matched = self._match_position_side(short_positions, symbol, 'SHORT')

        all_positions = long_positions_matched + short_positions_matched

        # Allocate fees proportionally based on position weights
        if all_positions:
            # Calculate total weight
            total_weight = sum(pos['weight'] for pos in all_positions)

            if total_weight > 0:
                for pos in all_positions:
                    # Allocate fees proportionally
                    fee_allocation = (pos['weight'] / total_weight) * total_fees
                    pos['fees'] = fee_allocation
                    pos['pnl'] = pos['pnl_before_fees'] + fee_allocation

        return all_positions

    def _match_position_side(self, orders: List[Dict], symbol: str, side: str) -> List[Dict]:
        """Match entry and exit orders for one position side"""
        positions = []
        open_positions = []  # Track multiple open positions with their entry prices

        for order in orders:
            if order['type'] == 'entry':
                # Record each entry separately with its own price and quantity
                open_positions.append({
                    'price': order['price'],
                    'qty': order['qty'],
                    'time': order['time'],
                    'order_id': order['order_id']
                })

            elif order['type'] == 'exit' and open_positions:
                # Close position FIFO (First In First Out)
                exit_qty_remaining = order['qty']

                while exit_qty_remaining > 0 and open_positions:
                    entry = open_positions[0]
                    close_qty = min(exit_qty_remaining, entry['qty'])

                    # Calculate PNL for this piece
                    if side == 'LONG':
                        pnl_before_fees = (order['price'] - entry['price']) * close_qty
                    else:  # SHORT
                        pnl_before_fees = (entry['price'] - order['price']) * close_qty

                    # Calculate weight for fee allocation (qty * time_held)
                    time_held = order['time'] - entry['time']
                    weight = close_qty * time_held

                    positions.append({
                        'symbol': symbol,
                        'side': side,
                        'entry_price': entry['price'],  # Use actual entry price, not average
                        'exit_price': order['price'],
                        'entry_time': entry['time'],
                        'exit_time': order['time'],
                        'qty': close_qty,
                        'pnl_before_fees': pnl_before_fees,
                        'weight': weight,  # Weight for proportional fee allocation
                        'entry_order_id': entry['order_id'],
                        'exit_order_id': order['order_id']
                    })

                    # Update remaining quantities
                    entry['qty'] -= close_qty
                    exit_qty_remaining -= close_qty

                    # Remove fully closed entry
                    if entry['qty'] <= 0.0001:
                        open_positions.pop(0)

        return positions

    def get_traded_symbols(
        self,
        since: int,
        until: int,
        client: Optional[BinanceFuturesRestClient] = None
    ) -> List[str]:
        """Get symbols that have actual trades using income history"""
        client = client or self.client
        logger.info("Fetching traded symbols from income history...")
        result = self._fetch_income_history(since=since, until=until, client=client)
        symbols = {
            record['symbol']
            for record in result
            if 'symbol' in record and record['symbol']
        }

        symbols_list = list(symbols)

        if symbols_list:
            logger.info(f"Found {len(symbols_list)} symbols with activity: {symbols_list}")
        else:
            logger.warning("No symbols found in income history")

        return symbols_list

    def _extract_symbol_closed_positions(
        self,
        symbol: str,
        since: int,
        until: int,
        use_time_filter: bool = True,
        fee_totals_by_symbol: Optional[Dict[str, float]] = None,
    ) -> tuple[List[Dict], float]:
        """
        Extract and transform closed positions for one symbol.
        Designed for parallel ETL workers.
        """
        started_at = time.perf_counter()
        worker_client = self._create_worker_client()

        if use_time_filter:
            orders = self.get_all_orders(
                symbol,
                limit=1000,
                start_time=since,
                end_time=until,
                client=worker_client
            )
        else:
            # Get all orders without time filter (API limit issue)
            orders = self.get_all_orders(
                symbol,
                limit=1000,
                start_time=None,
                end_time=None,
                client=worker_client
            )

        if not orders:
            return [], time.perf_counter() - started_at

        # Filter filled or partially filled orders (executedQty > 0)
        filled_orders = [
            o for o in orders
            if float(o['executedQty']) > 0 and o['updateTime'] >= since
        ]
        if len(filled_orders) < 1:
            return [], time.perf_counter() - started_at

        symbol_total_fees = 0.0
        if fee_totals_by_symbol is not None:
            symbol_total_fees = fee_totals_by_symbol.get(symbol, 0.0)
            fees_map = {0: symbol_total_fees} if symbol_total_fees != 0 else {}
        else:
            fees_map = self.get_fees_for_symbol(symbol, since, until, client=worker_client)
        positions = self.match_orders_to_positions(filled_orders, symbol, fees_map)
        return positions, time.perf_counter() - started_at

    def analyze_orders(
        self,
        since: int,
        until: int,
        traded_symbols: Optional[List[str]] = None,
        use_time_filter: bool = True
    ) -> pd.DataFrame:
        """Analyze orders and convert to DataFrame"""

        # 获取有交易记录的币种
        if traded_symbols is None:
            traded_symbols = self.get_traded_symbols(since, until)

        if not traded_symbols:
            logger.warning("No trading history found in the specified period")
            return pd.DataFrame()

        symbols = sorted(set(traded_symbols))
        logger.info(f"Analyzing {len(symbols)} symbols...")

        all_positions: List[Dict] = []
        worker_count = self._resolve_workers(len(symbols))
        logger.info(f"ETL worker count: {worker_count}")
        success_count = 0
        failure_count = 0
        symbol_timings: List[tuple[str, float, int]] = []
        fee_prefetch_started = time.perf_counter()
        fee_totals_by_symbol = self.get_fee_totals_by_symbol(since=since, until=until)
        fee_prefetch_elapsed = time.perf_counter() - fee_prefetch_started
        logger.info(
            f"Income fee cache: symbols={len(fee_totals_by_symbol)}, elapsed={fee_prefetch_elapsed:.2f}s"
        )

        if worker_count == 1:
            for idx, symbol in enumerate(symbols, 1):
                logger.info(f"[{idx}/{len(symbols)}] Processing {symbol}...")
                try:
                    positions, elapsed = self._extract_symbol_closed_positions(
                        symbol=symbol,
                        since=since,
                        until=until,
                        use_time_filter=use_time_filter,
                        fee_totals_by_symbol=fee_totals_by_symbol
                    )
                    symbol_timings.append((symbol, elapsed, len(positions)))
                    success_count += 1
                    if positions:
                        all_positions.extend(positions)
                        logger.debug(f"Found {len(positions)} closed positions for {symbol}")
                except Exception as exc:
                    failure_count += 1
                    logger.error(f"Processing {symbol} failed: {exc}")
        else:
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                future_map = {
                    executor.submit(
                        self._extract_symbol_closed_positions,
                        symbol,
                        since,
                        until,
                        use_time_filter,
                        fee_totals_by_symbol
                    ): symbol
                    for symbol in symbols
                }

                for idx, future in enumerate(as_completed(future_map), 1):
                    symbol = future_map[future]
                    try:
                        positions, elapsed = future.result()
                        symbol_timings.append((symbol, elapsed, len(positions)))
                        success_count += 1
                        if positions:
                            all_positions.extend(positions)
                            logger.debug(f"Found {len(positions)} closed positions for {symbol}")
                        logger.info(f"[{idx}/{len(symbols)}] Processed {symbol}")
                    except Exception as exc:
                        failure_count += 1
                        logger.error(f"[{idx}/{len(symbols)}] Processing {symbol} failed: {exc}")

        if symbol_timings:
            slowest = sorted(symbol_timings, key=lambda item: item[1], reverse=True)[:5]
            slowest_str = ", ".join(
                f"{symbol}:{elapsed:.2f}s/{count}"
                for symbol, elapsed, count in slowest
            )
            logger.info(
                f"Closed ETL stats: success={success_count}, failed={failure_count}, "
                f"workers={worker_count}, slowest=[{slowest_str}]"
            )
        elif symbols:
            logger.info(
                f"Closed ETL stats: success={success_count}, failed={failure_count}, workers={worker_count}"
            )

        logger.info(f"Found {len(all_positions)} closed positions total")

        if not all_positions:
            logger.warning("No closed positions found")
            return pd.DataFrame()

        # Prefetch daily open price by unique (symbol, UTC day) to avoid per-trade network call.
        price_prefetch_started = time.perf_counter()
        price_keys = sorted(
            {
                (pos['symbol'], self.get_utc_day_start(pos['entry_time']))
                for pos in all_positions
            }
        )
        open_price_cache: Dict[tuple[str, int], Optional[float]] = {}
        price_timings: List[tuple[str, float, bool]] = []
        price_worker_count = self._resolve_workers(len(price_keys), max_workers=self.max_price_workers)

        if price_worker_count == 1:
            for symbol, day_start_ms in price_keys:
                fetched_symbol, fetched_day_start, price, elapsed = self._fetch_open_price_for_key(
                    symbol=symbol,
                    utc_day_start_ms=day_start_ms
                )
                open_price_cache[(fetched_symbol, fetched_day_start)] = price
                price_timings.append((fetched_symbol, elapsed, price is not None))
        else:
            with ThreadPoolExecutor(max_workers=price_worker_count) as executor:
                future_map = {
                    executor.submit(self._fetch_open_price_for_key, symbol, day_start_ms): (symbol, day_start_ms)
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
            slowest_price_str = ", ".join(
                f"{symbol}:{elapsed:.2f}s"
                for symbol, elapsed, _ok in slowest_price
            )
            logger.info(
                f"Open price cache: keys={len(price_keys)}, hits={hit_count}, workers={price_worker_count}, "
                f"elapsed={price_prefetch_elapsed:.2f}s, slowest=[{slowest_price_str}]"
            )

        trades_data = []
        transform_started = time.perf_counter()
        for idx, pos in enumerate(all_positions, 1):
            try:
                symbol = pos['symbol']
                entry_time = pos['entry_time']
                exit_time = pos['exit_time']

                day_start_ms = self.get_utc_day_start(entry_time)
                open_price = open_price_cache.get((symbol, day_start_ms))

                dt = datetime.fromtimestamp(entry_time / 1000, tz=UTC8)
                date_str = dt.strftime('%Y-%m-%d %H:%M:%S')
                utc_day = dt.strftime('%Y%m%d')

                # 计算出场时间
                exit_dt = datetime.fromtimestamp(exit_time / 1000, tz=UTC8)
                exit_time_str = exit_dt.strftime('%Y-%m-%d %H:%M:%S')

                # 计算持仓时间
                duration_ms = exit_time - entry_time
                duration_seconds = duration_ms / 1000

                if duration_seconds < 60:
                    hold_time = f"{int(duration_seconds)}秒"
                elif duration_seconds < 3600:
                    minutes = int(duration_seconds // 60)
                    seconds = int(duration_seconds % 60)
                    hold_time = f"{minutes}分{seconds}秒"
                elif duration_seconds < 86400:
                    hours = int(duration_seconds // 3600)
                    minutes = int((duration_seconds % 3600) // 60)
                    hold_time = f"{hours}小时{minutes}分"
                else:
                    days = int(duration_seconds // 86400)
                    hours = int((duration_seconds % 86400) // 3600)
                    hold_time = f"{days}天{hours}小时"

                if symbol.endswith('USDT'):
                    base = symbol[:-4]
                else:
                    base = symbol

                # 用实际入场价格计算相对开盘价格的变化百分比
                if open_price and pos['entry_price']:
                    price_change_pct = (pos['entry_price'] - open_price) / open_price
                else:
                    price_change_pct = 0

                pnl_net = round(pos['pnl'])
                entry_amount = round(pos['entry_price'] * pos['qty'])

                # 判断平仓类型：不管LONG还是SHORT，只要PNL_Net为正就是止盈
                close_type = '止盈' if pnl_net > 0 else '止损'

                # 计算收益率并格式化为保留2位小数的百分比字符串
                return_rate_raw = (pnl_net / entry_amount * 100) if entry_amount != 0 else 0
                return_rate = f"{return_rate_raw:.2f}%"

                trades_data.append({
                    'No': idx,
                    'Date': utc_day,
                    'Entry_Time': date_str,
                    'Exit_Time': exit_time_str,
                    'Holding_Time': hold_time,
                    'Symbol': base,
                    'Side': pos['side'],
                    'Price_Change_Pct': price_change_pct,
                    'Entry_Amount': entry_amount,
                    'Entry_Price': pos['entry_price'],
                    'Exit_Price': pos['exit_price'],
                    'Qty': pos['qty'],
                    'Fees': round(pos.get('fees', 0)),
                    'PNL_Net': pnl_net,
                    'Close_Type': close_type,
                    'Return_Rate': return_rate,
                    'Open_Price': open_price if open_price else 0,
                    'PNL_Before_Fees': round(pos.get('pnl_before_fees', pos['pnl'])),
                    'Entry_Order_ID': pos['entry_order_id'],
                    'Exit_Order_ID': pos['exit_order_id']
                })

                if idx % 20 == 0:
                    logger.info(f"Processed {idx}/{len(all_positions)} positions...")

            except Exception as e:
                logger.error(f"Parse position failed: {e}")
                continue

        df = pd.DataFrame(trades_data)
        df = df.sort_values('Entry_Time', ascending=True)

        logger.info(f"Successfully parsed {len(df)} positions")

        # Post-processing: Merge positions with same symbol and entry time
        logger.info("Post-processing: Merging positions with same entry time...")
        df = self.merge_same_entry_positions(df)
        logger.info(f"After merging: {len(df)} positions")
        transform_elapsed = time.perf_counter() - transform_started
        logger.info(
            f"Closed ETL transform stats: prefetch={price_prefetch_elapsed:.2f}s, transform={transform_elapsed:.2f}s"
        )

        return df

    def merge_same_entry_positions(self, df: pd.DataFrame) -> pd.DataFrame:
        """Merge positions with same symbol and entry time"""
        if df.empty:
            return df

        # Group by Symbol, Side, and Entry Time (round to minute)
        df['Entry_Time_Key'] = pd.to_datetime(df['Entry_Time']).dt.floor('min')  # Use 'min' instead of 'T' for pandas 3.0+

        merged_data = []

        for (symbol, side, entry_time_key), group in df.groupby(['Symbol', 'Side', 'Entry_Time_Key']):
            if len(group) == 1:
                # No need to merge, keep as is
                row = group.iloc[0].to_dict()
                merged_data.append(row)
            else:
                # Merge multiple positions
                total_qty = group['Qty'].sum()
                weighted_entry_price = (group['Entry_Price'] * group['Qty']).sum() / total_qty
                weighted_exit_price = (group['Exit_Price'] * group['Qty']).sum() / total_qty

                pnl_net_merged = round(group['PNL_Net'].sum())
                entry_amount_merged = round(weighted_entry_price * total_qty)

                # 判断合并后记录的平仓类型：不管LONG还是SHORT，只要PNL_Net为正就是止盈
                close_type_merged = '止盈' if pnl_net_merged > 0 else '止损'

                # 计算合并后记录的收益率并格式化为保留2位小数的百分比字符串
                return_rate_raw_merged = (pnl_net_merged / entry_amount_merged * 100) if entry_amount_merged != 0 else 0
                return_rate_merged = f"{return_rate_raw_merged:.2f}%"

                merged_row = {
                    'No': group.iloc[0]['No'],  # Keep first number
                    'Date': group.iloc[0]['Date'],
                    'Entry_Time': group.iloc[0]['Entry_Time'],
                    'Exit_Time': group.iloc[0]['Exit_Time'],
                    'Holding_Time': group.iloc[0]['Holding_Time'],
                    'Symbol': symbol,
                    'Side': side,
                    'Price_Change_Pct': group.iloc[0]['Price_Change_Pct'],  # Keep first
                    'Entry_Amount': entry_amount_merged,
                    'Entry_Price': weighted_entry_price,
                    'Exit_Price': weighted_exit_price,
                    'Qty': total_qty,
                    'Fees': round(group['Fees'].sum()),
                    'PNL_Net': pnl_net_merged,
                    'Close_Type': close_type_merged,
                    'Return_Rate': return_rate_merged,
                    'Open_Price': group.iloc[0]['Open_Price'],  # Keep first (same for all)
                    'PNL_Before_Fees': round(group['PNL_Before_Fees'].sum()),
                    'Entry_Order_ID': group.iloc[0]['Entry_Order_ID'],  # Keep first
                    'Exit_Order_ID': ','.join(group['Exit_Order_ID'].astype(str).unique()),  # Combine all
                    'Entry_Time_Key': entry_time_key
                }
                merged_data.append(merged_row)

        # Create new dataframe
        merged_df = pd.DataFrame(merged_data)

        # Drop the temporary key column
        if 'Entry_Time_Key' in merged_df.columns:
            merged_df = merged_df.drop('Entry_Time_Key', axis=1)

        # Re-number
        merged_df = merged_df.sort_values('Entry_Time', ascending=True).reset_index(drop=True)
        merged_df['No'] = range(1, len(merged_df) + 1)

        return merged_df

    def export_to_excel(self, df: pd.DataFrame, filename: str = None):
        """Export to Excel file"""
        if filename is None:
            filename = f"order_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='Orders', index=False)

                workbook = writer.book
                worksheet = writer.sheets['Orders']

                # Format Price_Change_Pct column as percentage
                if 'Price_Change_Pct' in df.columns:
                    col_idx = df.columns.get_loc('Price_Change_Pct')
                    col_letter = chr(ord('A') + col_idx)
                    for row in range(2, len(df) + 2):  # Data rows start from row 2 (1-indexed, header is row 1)
                        cell = worksheet[f'{col_letter}{row}']
                        cell.number_format = '0%'

                last_row = len(df) + 2
                worksheet[f'A{last_row}'] = 'Summary'

                # Add summary for PNL columns
                if 'PNL_Before_Fees' in df.columns:
                    col_letter = chr(ord('A') + df.columns.get_loc('PNL_Before_Fees'))
                    worksheet[f'{col_letter}{last_row}'] = df['PNL_Before_Fees'].sum()

                if 'Fees' in df.columns:
                    col_letter = chr(ord('A') + df.columns.get_loc('Fees'))
                    worksheet[f'{col_letter}{last_row}'] = df['Fees'].sum()

                if 'PNL_Net' in df.columns:
                    col_letter = chr(ord('A') + df.columns.get_loc('PNL_Net'))
                    worksheet[f'{col_letter}{last_row}'] = df['PNL_Net'].sum()

            logger.info(f"Exported to: {filename}")
            logger.info(f"Total records: {len(df)}")

            if 'PNL_Net' in df.columns:
                pnl_before_fees = df['PNL_Before_Fees'].sum() if 'PNL_Before_Fees' in df.columns else 0
                total_fees = df['Fees'].sum() if 'Fees' in df.columns else 0
                pnl_net = df['PNL_Net'].sum()
                win_count = len(df[df['PNL_Net'] > 0])
                loss_count = len(df[df['PNL_Net'] < 0])
                win_rate = win_count / len(df) * 100 if len(df) > 0 else 0

                logger.info(f"PNL Before Fees: {pnl_before_fees:.2f} USDT")
                logger.info(f"Total Fees (Commission + Funding): {total_fees:.2f} USDT")
                logger.info(f"Net PNL: {pnl_net:.2f} USDT")
                logger.info(f"Win Rate: {win_rate:.2f}% ({win_count} wins / {loss_count} losses)")

        except Exception as e:
            logger.error(f"Export to Excel failed: {e}")
            import traceback
            traceback.print_exc()


    def get_real_positions(self, client: Optional[BinanceFuturesRestClient] = None) -> Optional[Dict[str, float]]:
        """Get actual open positions from Binance (Symbol -> NetQty)"""
        client = client or self.client
        endpoint = '/fapi/v2/positionRisk'
        try:
            positions = client.signed_get(endpoint)
            if positions is None:
                logger.warning("PositionRisk request failed, returning None")
                return None
            # Filter for non-zero positions
            real_pos = {}
            if positions:
                for p in positions:
                    amt = float(p.get('positionAmt', 0))
                    if abs(amt) > 0:
                        real_pos[p['symbol']] = amt
            return real_pos
        except Exception as e:
            logger.error(f"Failed to fetch position risk: {e}")
            return None

    def _extract_open_positions_for_symbol(
        self,
        symbol: str,
        real_net_qty: float,
        since: int,
        until: int
    ) -> tuple[List[Dict], float]:
        started_at = time.perf_counter()
        worker_client = self._create_worker_client()
        orders = self.get_all_orders(
            symbol,
            limit=1000,
            start_time=since,
            end_time=until,
            client=worker_client
        )
        if not orders:
            return [], time.perf_counter() - started_at

        # Filter filled orders
        filled_orders = [
            o for o in orders
            if float(o['executedQty']) > 0 and o['updateTime'] >= since
        ]

        # Track positions locally
        long_entries = []
        short_entries = []

        for order in sorted(filled_orders, key=lambda x: x['updateTime']):
            side = order['side']
            position_side = order['positionSide']
            qty = float(order['executedQty'])
            price = float(order['avgPrice'])
            order_time = order['updateTime']
            order_id = order['orderId']

            # LONG
            if (position_side == 'LONG' and side == 'BUY') or \
               (position_side == 'BOTH' and side == 'BUY'):
                long_entries.append({
                    'price': price, 'qty': qty, 'time': order_time, 'order_id': order_id
                })
            elif (position_side == 'LONG' and side == 'SELL') or \
                    (position_side == 'BOTH' and side == 'SELL'):
                # FIFO Close
                remaining = qty
                while remaining > 0 and long_entries:
                    entry = long_entries[0]
                    close_qty = min(remaining, entry['qty'])
                    entry['qty'] -= close_qty
                    remaining -= close_qty
                    if entry['qty'] <= 0.0001:
                        long_entries.pop(0)

            # SHORT
            if position_side == 'SHORT' and side == 'SELL':
                short_entries.append({
                    'price': price, 'qty': qty, 'time': order_time, 'order_id': order_id
                })
            elif position_side == 'SHORT' and side == 'BUY':
                remaining = qty
                while remaining > 0 and short_entries:
                    entry = short_entries[0]
                    close_qty = min(remaining, entry['qty'])
                    entry['qty'] -= close_qty
                    remaining -= close_qty
                    if entry['qty'] <= 0.0001:
                        short_entries.pop(0)

        # Sync with Real Position
        # We trust real_net_qty. If local calculation differs, we adjust.
        # real_net_qty > 0 means Net Long, < 0 means Net Short.
        final_entries = []

        if real_net_qty > 0:
            # We should only have Long entries
            calculated_qty = sum(e['qty'] for e in long_entries)
            # Allow small float diff
            if abs(calculated_qty - real_net_qty) > 0.0001:
                logger.warning(f"{symbol} Qty Mismatch: Real={real_net_qty}, Calc={calculated_qty}. Adjusting to Real.")

                # If we have TOO MANY locals, it means we missed some sells.
                # Trim from HEAD (FIFO) to match real qty.
                if calculated_qty > real_net_qty:
                    diff = calculated_qty - real_net_qty
                    while diff > 0.0001 and long_entries:
                        entry = long_entries[0]  # Oldest entry
                        if entry['qty'] > diff:
                            entry['qty'] -= diff
                            diff = 0
                        else:
                            diff -= entry['qty']
                            long_entries.pop(0)
                else:
                    # Calculated < Real: We missed some BUYS or started monitoring late.
                    # Can't invent history, so we just show what we have.
                    pass

            final_entries = long_entries

        elif real_net_qty < 0:
            # We should only have Short entries
            abs_real_qty = abs(real_net_qty)
            calculated_qty = sum(e['qty'] for e in short_entries)
            if abs(calculated_qty - abs_real_qty) > 0.0001:
                logger.warning(f"{symbol} Qty Mismatch: Real={real_net_qty}, Calc=-{calculated_qty}. Adjusting to Real.")

                if calculated_qty > abs_real_qty:
                    diff = calculated_qty - abs_real_qty
                    while diff > 0.0001 and short_entries:
                        entry = short_entries[0]
                        if entry['qty'] > diff:
                            entry['qty'] -= diff
                            diff = 0
                        else:
                            diff -= entry['qty']
                            short_entries.pop(0)

            final_entries = short_entries

        # Add to results
        base = symbol[:-4] if symbol.endswith('USDT') else symbol
        side_str = 'LONG' if real_net_qty > 0 else 'SHORT'

        output = []
        for entry in final_entries:
            if entry['qty'] > 0.0001:
                dt = datetime.fromtimestamp(entry['time'] / 1000, tz=UTC8)
                output.append({
                    'date': dt.strftime('%Y%m%d'),
                    'symbol': base,
                    'side': side_str,
                    'entry_time': dt.strftime('%Y-%m-%d %H:%M:%S'),
                    'entry_price': entry['price'],
                    'qty': entry['qty'],
                    'entry_amount': round(entry['price'] * entry['qty']),
                    'order_id': entry['order_id']
                })
        return output, time.perf_counter() - started_at

    def get_open_positions(self, since: int, until: int, traded_symbols: Optional[List[str]] = None) -> Optional[List[Dict]]:
        """
        Get open entry orders (Opened but not closed)
        Uses PositionRisk as source of truth to filter out closed/liquidated positions.
        """
        # 1. Get Real Positions (Source of Truth)
        real_positions_map = self.get_real_positions()
        if real_positions_map is None:
            logger.warning("Skip open positions refresh because PositionRisk failed")
            return None

        if traded_symbols is None:
            traded_symbols = self.get_traded_symbols(since, until)

        # Add any symbols that have real positions but might be missed (unlikely but safe)
        all_target_symbols = set(traded_symbols) if traded_symbols else set()
        all_target_symbols.update(real_positions_map.keys())

        if not all_target_symbols:
            return []

        active_symbols = sorted(
            symbol for symbol in all_target_symbols
            if real_positions_map.get(symbol, 0.0) != 0
        )

        if not active_symbols:
            return []

        worker_count = self._resolve_workers(len(active_symbols))
        open_positions: List[Dict] = []
        success_count = 0
        failure_count = 0
        symbol_timings: List[tuple[str, float, int]] = []

        if worker_count == 1:
            for symbol in active_symbols:
                try:
                    positions, elapsed = self._extract_open_positions_for_symbol(
                        symbol=symbol,
                        real_net_qty=real_positions_map[symbol],
                        since=since,
                        until=until
                    )
                    symbol_timings.append((symbol, elapsed, len(positions)))
                    success_count += 1
                    open_positions.extend(positions)
                except Exception as exc:
                    failure_count += 1
                    logger.error(f"Failed to extract open positions for {symbol}: {exc}")
            if symbol_timings:
                slowest = sorted(symbol_timings, key=lambda item: item[1], reverse=True)[:5]
                slowest_str = ", ".join(
                    f"{symbol}:{elapsed:.2f}s/{count}"
                    for symbol, elapsed, count in slowest
                )
                logger.info(
                    f"Open ETL stats: success={success_count}, failed={failure_count}, "
                    f"workers={worker_count}, slowest=[{slowest_str}]"
                )
            return open_positions

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {
                executor.submit(
                    self._extract_open_positions_for_symbol,
                    symbol,
                    real_positions_map[symbol],
                    since,
                    until
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
            slowest_str = ", ".join(
                f"{symbol}:{elapsed:.2f}s/{count}"
                for symbol, elapsed, count in slowest
            )
            logger.info(
                f"Open ETL stats: success={success_count}, failed={failure_count}, "
                f"workers={worker_count}, slowest=[{slowest_str}]"
            )

        return open_positions


def main():
    """Main function"""
    load_dotenv()

    api_key = os.getenv('BINANCE_API_KEY')
    api_secret = os.getenv('BINANCE_API_SECRET')

    # Read time range configuration
    start_date_str = os.getenv('START_DATE')
    end_date_str = os.getenv('END_DATE')
    days_to_fetch = int(os.getenv('DAYS_TO_FETCH', 30))

    # Calculate time range
    if start_date_str:
        # Parse start date
        try:
            start_dt = datetime.strptime(start_date_str, '%Y-%m-%d')
            start_dt = start_dt.replace(hour=23, minute=0, second=0, microsecond=0)
            since = int(start_dt.timestamp() * 1000)

            if end_date_str:
                # Parse end date
                end_dt = datetime.strptime(end_date_str, '%Y-%m-%d')
                end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999000)
                until = int(end_dt.timestamp() * 1000)
                time_range_desc = f"{start_date_str} to {end_date_str}"
            else:
                until = int(datetime.now().timestamp() * 1000)
                time_range_desc = f"from {start_date_str} to now"
        except ValueError as e:
            logger.error(f"Error parsing date: {e}")
            logger.error("Date format should be YYYY-MM-DD")
            return
    else:
        # Use days_to_fetch
        since = int((datetime.now() - timedelta(days=days_to_fetch)).timestamp() * 1000)
        until = int(datetime.now().timestamp() * 1000)
        time_range_desc = f"Last {days_to_fetch} days"

    if not api_key or not api_secret:
        logger.error("Please configure API keys first!")
        logger.error("1. Copy .env.template to .env")
        logger.error("2. Fill in your Binance API keys in .env file")
        return

    logger.info("=" * 60)
    logger.info("Binance Futures Order Analyzer")
    logger.info("Analyzing based on ORDER level (not individual trades)")
    logger.info("=" * 60)
    logger.info(f"Query range: {time_range_desc}")
    logger.info(f"Start timestamp: {since} ({datetime.fromtimestamp(since/1000).strftime('%Y-%m-%d %H:%M:%S')})")
    logger.info(f"End timestamp: {until} ({datetime.fromtimestamp(until/1000).strftime('%Y-%m-%d %H:%M:%S')})")

    analyzer = TradeDataProcessor(api_key, api_secret)

    logger.info("Note: This may take several minutes depending on trading history...")

    df = analyzer.analyze_orders(since=since, until=until)

    if df.empty:
        logger.warning("No order data found")
        return

    analyzer.export_to_excel(df)

    logger.info("Done!")


if __name__ == "__main__":
    main()
