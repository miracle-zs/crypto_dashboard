#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance Futures Order Analyzer
Analyze trading history based on orders (not individual trades)
"""

import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
import os
import hashlib
import hmac
from urllib.parse import urlencode
from dotenv import load_dotenv
import time
from app.logger import logger

# 定义北京时区 UTC+8
UTC8 = timezone(timedelta(hours=8))


class BinanceOrderAnalyzer:
    """Binance Order Analyzer"""

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = 'https://fapi.binance.com'
        self.headers = {
            'X-MBX-APIKEY': api_key
        }

    def _generate_signature(self, params: dict) -> str:
        """Generate signature for authenticated requests"""
        query_string = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature

    def _request(self, method: str, endpoint: str, params: dict = None) -> dict:
        """Make authenticated request to Binance API"""
        if params is None:
            params = {}

        url = f"{self.base_url}{endpoint}"

        max_retries = 4
        backoff_seconds = 1

        for attempt in range(1, max_retries + 1):
            params['timestamp'] = int(time.time() * 1000)
            params['signature'] = self._generate_signature(params)

            try:
                if method == 'GET':
                    response = requests.get(url, headers=self.headers, params=params, timeout=30)
                else:
                    response = requests.post(url, headers=self.headers, params=params, timeout=30)

                response.raise_for_status()
                return response.json()
            except requests.exceptions.HTTPError as e:
                status_code = e.response.status_code if e.response is not None else None
                if status_code == 429 and attempt < max_retries:
                    logger.warning(
                        f"Rate limited (429) on {endpoint}. Backing off {backoff_seconds}s (attempt {attempt}/{max_retries})"
                    )
                    time.sleep(backoff_seconds)
                    backoff_seconds *= 2
                    continue

                logger.error(f"API request failed: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Response: {e.response.text}")
                return None
            except requests.exceptions.RequestException as e:
                logger.error(f"API request failed: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    logger.error(f"Response: {e.response.text}")
                return None

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

        income_data = self._request('GET', endpoint, params)
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

    def get_account_balance(self) -> Dict[str, float]:
        """Get USD-M Futures account balance (Margin & Wallet)."""
        endpoint = '/fapi/v2/account'
        account_info = self._request('GET', endpoint)

        if account_info:
            return {
                'margin_balance': float(account_info.get('totalMarginBalance', 0)),
                'wallet_balance': float(account_info.get('totalWalletBalance', 0))
            }
        else:
            logger.warning("Could not retrieve account balance.")
            return None

    def get_all_orders(self, symbol: str, limit: int = 1000, start_time: int = None) -> List[Dict]:
        """Get all orders for a symbol"""
        endpoint = '/fapi/v1/allOrders'
        params = {
            'symbol': symbol,
            'limit': limit
        }

        if start_time:
            params['startTime'] = start_time

        result = self._request('GET', endpoint, params)

        if result is None:
            logger.warning(f"API request failed for {symbol}")
        elif isinstance(result, list) and len(result) == 0:
            logger.debug(f"No orders in time range for {symbol}")

        return result if result else []

    def get_exchange_info(self) -> dict:
        """Get exchange information"""
        endpoint = '/fapi/v1/exchangeInfo'
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Get exchange info failed: {e}")
            return {}

    def get_all_symbols(self) -> List[str]:
        """Get all USDT perpetual contract symbols"""
        info = self.get_exchange_info()
        if not info or 'symbols' not in info:
            return []

        symbols = []
        for s in info['symbols']:
            if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT':
                symbols.append(s['symbol'])

        return symbols

    def get_kline_data(self, symbol: str, interval: str, start_time: int, limit: int = 1) -> List:
        """Get kline/candlestick data"""
        endpoint = '/fapi/v1/klines'
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_time,
            'limit': limit
        }

        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            return []

    def get_utc_day_start(self, timestamp: int) -> int:
        """Get UTC+0 00:00 timestamp for given timestamp"""
        dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
        day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return int(day_start.timestamp() * 1000)

    def get_price_change_from_utc_start(self, symbol: str, timestamp: int) -> Optional[float]:
        """Get the opening price for the UTC day of the given timestamp
        
        Returns:
            float: opening price or None if failed
        """
        try:
            day_start = self.get_utc_day_start(timestamp)

            kline_start = self.get_kline_data(symbol, '1h', day_start, 1)
            if not kline_start:
                return None

            open_price = float(kline_start[0][1])
            return open_price

        except Exception as e:
            return None

    def get_fees_for_symbol(self, symbol: str, since: int, until: int) -> Dict[int, float]:
        """Get commission and funding fees for a symbol"""
        endpoint = '/fapi/v1/income'

        # Get commission fees
        commission_params = {
            'symbol': symbol,
            'incomeType': 'COMMISSION',
            'startTime': since,
            'endTime': until,
            'limit': 1000
        }
        commission_records = self._request('GET', endpoint, commission_params)

        # Get funding fees
        funding_params = {
            'symbol': symbol,
            'incomeType': 'FUNDING_FEE',
            'startTime': since,
            'endTime': until,
            'limit': 1000
        }
        funding_records = self._request('GET', endpoint, funding_params)

        # Build a map of time -> total fees
        fees_map = {}

        if commission_records:
            for record in commission_records:
                time = int(record['time'])
                income = float(record['income'])
                fees_map[time] = fees_map.get(time, 0) + income

        if funding_records:
            for record in funding_records:
                time = int(record['time'])
                income = float(record['income'])
                fees_map[time] = fees_map.get(time, 0) + income

        return fees_map

    def match_orders_to_positions(self, orders: List[Dict], symbol: str, fees_map: Dict[int, float] = None) -> List[Dict]:
        """Match orders to closed positions (handles partial fills)"""

        if fees_map is None:
            fees_map = {}

        total_fees = sum(fees_map.values())

        # Separate LONG and SHORT positions
        long_positions = []
        short_positions = []

        for order in sorted(orders, key=lambda x: x['updateTime']):
            if order['status'] != 'FILLED':
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

    def get_traded_symbols(self, since: int, until: int) -> List[str]:
        """Get symbols that have actual trades using income history"""
        logger.info("Fetching traded symbols from income history...")

        endpoint = '/fapi/v1/income'
        symbols = set()
        current_start = since

        while True:
            params = {
                'startTime': current_start,
                'endTime': until,
                'limit': 1000
            }

            result = self._request('GET', endpoint, params)
            if not result or len(result) == 0:
                logger.warning("Failed to fetch income history")
                break

            # Extract unique symbols from income records
            batch_symbols = list(set([record['symbol'] for record in result if 'symbol' in record and record['symbol']]))
            symbols.update(batch_symbols)
            if len(result) < 1000:
                break
            # Move start time to the time of the last record + 1ms
            # This ensures we don't get duplicate records
            last_time = int(result[-1]['time'])
            current_start = last_time + 1
            
            # Add a small delay to avoid rate limiting
            time.sleep(0.1)            

        symbols_list = list(symbols)

        if symbols_list:
            logger.info(f"Found {len(symbols_list)} symbols with activity: {symbols_list}")
        else:
            logger.warning("No symbols found in income history")

        return symbols_list

    def analyze_orders(self, since: int, until: int) -> pd.DataFrame:
        """Analyze orders and convert to DataFrame"""

        # 获取有交易记录的币种
        traded_symbols = self.get_traded_symbols(since, until)

        if not traded_symbols:
            logger.warning("No trading history found in the specified period")
            return pd.DataFrame()

        logger.info(f"Analyzing {len(traded_symbols)} symbols...")

        all_positions = []
        processed = 0

        for symbol in traded_symbols:
            processed += 1
            logger.info(f"[{processed}/{len(traded_symbols)}] Processing {symbol}...")

            # Get all orders without time filter (API limit issue)
            orders = self.get_all_orders(symbol, limit=1000, start_time=None)
            if not orders:
                logger.debug(f"No orders found for {symbol}")
                continue

            # Filter by time range and status
            filled_orders = [o for o in orders if o['status'] == 'FILLED' and o['updateTime'] >= since]
            logger.debug(f"Found {len(filled_orders)} filled orders in time range for {symbol}")

            if len(filled_orders) < 1:
                continue

            # Get fees for this symbol
            logger.debug(f"Fetching fees for {symbol}...")
            fees_map = self.get_fees_for_symbol(symbol, since, until)
            total_fees = sum(fees_map.values())
            logger.debug(f"Total fees for {symbol}: {total_fees:.2f} USDT")

            positions = self.match_orders_to_positions(filled_orders, symbol, fees_map)
            all_positions.extend(positions)

            if positions:
                logger.debug(f"Found {len(positions)} closed positions for {symbol}")

            time.sleep(0.1)

        logger.info(f"Found {len(all_positions)} closed positions total")

        if not all_positions:
            logger.warning("No closed positions found")
            return pd.DataFrame()

        trades_data = []
        for idx, pos in enumerate(all_positions, 1):
            try:
                symbol = pos['symbol']
                entry_time = pos['entry_time']
                exit_time = pos['exit_time']

                open_price = self.get_price_change_from_utc_start(symbol, entry_time)

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


    def get_open_positions(self, since: int, until: int) -> List[Dict]:
        """
        获取未平仓的入场订单（已开仓但未平仓）

        Returns:
            List[Dict]: 未平仓订单列表
        """
        traded_symbols = self.get_traded_symbols(since, until)
        if not traded_symbols:
            return []

        open_positions = []

        for symbol in traded_symbols:
            orders = self.get_all_orders(symbol, limit=1000, start_time=since)
            if not orders:
                continue

            # 过滤已成交的订单
            filled_orders = [o for o in orders if o['status'] == 'FILLED' and o['updateTime'] >= since]

            # 分别追踪 LONG 和 SHORT 的持仓
            long_qty = 0.0
            long_entries = []
            short_qty = 0.0
            short_entries = []

            for order in sorted(filled_orders, key=lambda x: x['updateTime']):
                side = order['side']
                position_side = order['positionSide']
                qty = float(order['executedQty'])
                price = float(order['avgPrice'])
                order_time = order['updateTime']

                # LONG 仓位
                if (position_side == 'LONG' and side == 'BUY') or \
                   (position_side == 'BOTH' and side == 'BUY'):
                    long_qty += qty
                    long_entries.append({
                        'price': price,
                        'qty': qty,
                        'time': order_time,
                        'order_id': order['orderId']
                    })
                elif (position_side == 'LONG' and side == 'SELL') or \
                     (position_side == 'BOTH' and side == 'SELL'):
                    # 平仓，FIFO 扣减
                    remaining = qty
                    while remaining > 0 and long_entries:
                        entry = long_entries[0]
                        close_qty = min(remaining, entry['qty'])
                        entry['qty'] -= close_qty
                        remaining -= close_qty
                        long_qty -= close_qty
                        if entry['qty'] <= 0.0001:
                            long_entries.pop(0)

                # SHORT 仓位
                if position_side == 'SHORT' and side == 'SELL':
                    short_qty += qty
                    short_entries.append({
                        'price': price,
                        'qty': qty,
                        'time': order_time,
                        'order_id': order['orderId']
                    })
                elif position_side == 'SHORT' and side == 'BUY':
                    remaining = qty
                    while remaining > 0 and short_entries:
                        entry = short_entries[0]
                        close_qty = min(remaining, entry['qty'])
                        entry['qty'] -= close_qty
                        remaining -= close_qty
                        short_qty -= close_qty
                        if entry['qty'] <= 0.0001:
                            short_entries.pop(0)

            # 收集未平仓的入场订单
            base = symbol[:-4] if symbol.endswith('USDT') else symbol

            for entry in long_entries:
                if entry['qty'] > 0.0001:
                    dt = datetime.fromtimestamp(entry['time'] / 1000, tz=UTC8)
                    open_positions.append({
                        'date': dt.strftime('%Y%m%d'),
                        'symbol': base,
                        'side': 'LONG',
                        'entry_time': dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'entry_price': entry['price'],
                        'qty': entry['qty'],
                        'entry_amount': round(entry['price'] * entry['qty']),
                        'order_id': entry['order_id']
                    })

            for entry in short_entries:
                if entry['qty'] > 0.0001:
                    dt = datetime.fromtimestamp(entry['time'] / 1000, tz=UTC8)
                    open_positions.append({
                        'date': dt.strftime('%Y%m%d'),
                        'symbol': base,
                        'side': 'SHORT',
                        'entry_time': dt.strftime('%Y-%m-%d %H:%M:%S'),
                        'entry_price': entry['price'],
                        'qty': entry['qty'],
                        'entry_amount': round(entry['price'] * entry['qty']),
                        'order_id': entry['order_id']
                    })

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

    analyzer = BinanceOrderAnalyzer(api_key, api_secret)

    logger.info("Note: This may take several minutes depending on trading history...")

    df = analyzer.analyze_orders(since=since, until=until)

    if df.empty:
        logger.warning("No order data found")
        return

    analyzer.export_to_excel(df)

    logger.info("Done!")


if __name__ == "__main__":
    main()
