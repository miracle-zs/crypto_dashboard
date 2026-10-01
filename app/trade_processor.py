#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance Futures Order Analyzer & Trade Processing Facade
Coordinates trade analysis, order fetching, matching, and export.
"""

from datetime import datetime, timezone, timedelta
import os
import threading
import time
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
import pandas as pd

from app.logger import logger
from app.binance_client import BinanceFuturesRestClient
from app.core.trade_position_utils import (
    build_open_position_output,
    consume_fifo_entries,
    format_holding_time_cn,
    position_to_trade_row,
    trim_entries_to_target_qty,
)
from app.core.trade_matching import (
    is_liquidation_order as is_liquidation_order_core,
    match_orders_to_positions as match_orders_to_positions_core,
    match_position_side as match_position_side_core,
)
from app.core.trade_dataframe_utils import merge_same_entry_positions
from app.services.trade_api_gateway import (
    fetch_account_balance,
    fetch_all_orders,
    fetch_all_symbols,
    fetch_exchange_info,
    fetch_income_history,
    fetch_klines,
    fetch_real_positions,
    fetch_transfer_income_records,
    fetch_user_trades,
)
from app.services.trade_etl_service import (
    analyze_orders as analyze_orders_service,
    extract_open_positions_for_symbol as extract_open_positions_for_symbol_service,
    extract_symbol_closed_positions as extract_symbol_closed_positions_service,
    get_open_positions as get_open_positions_service,
)
from app.services.trade_income_aggregation import (
    aggregate_fee_totals_by_symbol,
    summarize_income_records,
    summarize_income_records_with_ranges,
)
from app.services.trade_price_service import (
    calc_utc_day_start,
    fetch_open_price_for_key as fetch_open_price_for_key_service,
    get_price_change_from_utc_start as get_price_change_from_utc_start_service,
)
from app.services.trade_export_service import export_to_excel_file

UTC8 = timezone(timedelta(hours=8))


class TradeDataProcessor:
    """Binance Futures Order Analyzer Facade"""

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.client = BinanceFuturesRestClient(api_key=api_key, api_secret=api_secret)
        self.max_etl_workers = self._load_max_workers()
        self.max_price_workers = self._load_price_workers()
        self.max_open_positions_workers = self._load_open_positions_workers()
        self._exchange_info_cache = None
        self._exchange_info_expires_at = 0.0
        self._exchange_info_lock = threading.Lock()
        self._exchange_info_cache_ttl_seconds = self._load_exchange_info_cache_ttl()
        self.extra_loss_income_types = self._load_extra_loss_income_types()

    def _load_max_workers(self) -> int:
        raw = os.getenv("ETL_MAX_WORKERS", "2")
        try:
            workers = int(raw)
            if workers < 1:
                raise ValueError("must be >= 1")
            return workers
        except Exception:
            logger.warning(f"Invalid ETL_MAX_WORKERS={raw}, fallback to 2")
            return 2

    def _load_price_workers(self) -> int:
        raw = os.getenv("ETL_PRICE_WORKERS", str(self.max_etl_workers))
        try:
            workers = int(raw)
            if workers < 1:
                raise ValueError("must be >= 1")
            return workers
        except Exception:
            logger.warning(f"Invalid ETL_PRICE_WORKERS={raw}, fallback to {self.max_etl_workers}")
            return self.max_etl_workers

    def _load_open_positions_workers(self) -> int:
        default_workers = max(1, self.max_etl_workers * 2)
        raw = os.getenv("ETL_OPEN_POSITIONS_WORKERS", str(default_workers))
        try:
            workers = int(raw)
            if workers < 1:
                raise ValueError("must be >= 1")
            return workers
        except Exception:
            logger.warning(f"Invalid ETL_OPEN_POSITIONS_WORKERS={raw}, fallback to {default_workers}")
            return default_workers

    def _load_exchange_info_cache_ttl(self) -> float:
        raw = os.getenv("ETL_EXCHANGE_INFO_CACHE_TTL_SECONDS", "300")
        try:
            ttl = float(raw)
            return max(0.0, ttl)
        except Exception:
            logger.warning(f"Invalid ETL_EXCHANGE_INFO_CACHE_TTL_SECONDS={raw}, fallback to 300")
            return 300.0

    def _load_extra_loss_income_types(self) -> set[str]:
        raw = os.getenv("EXTRA_LOSS_INCOME_TYPES", "INSURANCE_CLEAR")
        parsed = {item.strip().upper() for item in raw.split(",") if item.strip()}
        return parsed or {"INSURANCE_CLEAR"}

    def _resolve_workers(self, task_count: int, max_workers: Optional[int] = None) -> int:
        cap = self.max_etl_workers if max_workers is None else max_workers
        if task_count <= 1:
            return 1
        return min(cap, task_count)

    def _create_worker_client(self) -> BinanceFuturesRestClient:
        return BinanceFuturesRestClient(api_key=self.api_key, api_secret=self.api_secret)

    def _fetch_open_price_for_key(
        self, symbol: str, utc_day_start_ms: int
    ) -> tuple[str, int, Optional[float], float]:
        return fetch_open_price_for_key_service(self, symbol=symbol, utc_day_start_ms=utc_day_start_ms)

    def get_recent_financial_flow(self, start_time: int) -> float:
        """Calculate the sum of Realized PnL, Commission, and Funding Fees since start_time."""
        endpoint = "/fapi/v1/income"
        params = {"startTime": start_time, "limit": 1000}
        income_data = self.client.signed_get(endpoint, params)
        if not income_data:
            return 0.0

        flow = 0.0
        for item in income_data:
            if item.get("incomeType") in ("REALIZED_PNL", "COMMISSION", "FUNDING_FEE"):
                flow += float(item.get("income", 0.0))
        return flow

    def _fetch_income_history(
        self,
        since: int,
        until: int,
        client: Optional[BinanceFuturesRestClient] = None,
        income_type: Optional[str] = None,
        fail_on_error: bool = False,
    ) -> List[Dict]:
        """Fetch income history in a paginated way for a time window."""
        client = client or self.client
        records = fetch_income_history(
            client=client,
            since=since,
            until=until,
            income_type=income_type,
            fail_on_error=fail_on_error,
        )
        if records and getattr(self, "sync_repo", None):
            try:
                self.sync_repo.save_income_facts(records)
            except Exception as exc:
                logger.warning(f"Failed to persist income facts: {exc}")
        return records

    def get_transfer_income_records(self, start_time: int, end_time: Optional[int] = None) -> List[Dict]:
        """Fetch TRANSFER income records via trade_api_gateway."""
        return fetch_transfer_income_records(
            client=self.client,
            start_time=start_time,
            end_time=end_time,
            sync_repo=getattr(self, "sync_repo", None),
        )

    def get_account_balance(self) -> Dict[str, float]:
        """Get USD-M Futures account balance (Margin & Wallet)."""
        return fetch_account_balance(client=self.client)

    def get_all_orders(
        self,
        symbol: str,
        limit: int = 1000,
        start_time: int = None,
        end_time: int = None,
        client: Optional[BinanceFuturesRestClient] = None,
        fail_on_error: bool = False,
    ) -> List[Dict]:
        """Get all orders for a symbol."""
        return fetch_all_orders(
            client=client or self.client,
            symbol=symbol,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
            fail_on_error=fail_on_error,
        )

    def get_user_trades(
        self,
        symbol: str,
        limit: int = 1000,
        start_time: int = None,
        end_time: int = None,
        from_trade_id: int = None,
        client: Optional[BinanceFuturesRestClient] = None,
        fail_on_error: bool = False,
    ) -> List[Dict]:
        """Get account trades for a symbol."""
        return fetch_user_trades(
            client=client or self.client,
            symbol=symbol,
            limit=limit,
            start_time=start_time,
            end_time=end_time,
            from_trade_id=from_trade_id,
            fail_on_error=fail_on_error,
        )

    def get_exchange_info(self, client: Optional[BinanceFuturesRestClient] = None) -> dict:
        """Get exchange information with optional in-memory TTL cache."""
        active_client = client or self.client
        if active_client is self.client:
            now = time.time()
            with self._exchange_info_lock:
                if self._exchange_info_cache is not None and now < self._exchange_info_expires_at:
                    return self._exchange_info_cache

        payload = fetch_exchange_info(client=active_client)
        if active_client is self.client:
            with self._exchange_info_lock:
                self._exchange_info_cache = payload
                self._exchange_info_expires_at = time.time() + self._exchange_info_cache_ttl_seconds
        return payload

    def get_all_symbols(self, client: Optional[BinanceFuturesRestClient] = None) -> List[str]:
        """Get all USDT perpetual contract symbols."""
        info = self.get_exchange_info(client=client)
        return fetch_all_symbols(client=client or self.client, exchange_info=info)

    def get_kline_data(
        self,
        symbol: str,
        interval: str,
        start_time: int,
        limit: int = 1,
        client: Optional[BinanceFuturesRestClient] = None,
    ) -> List:
        """Get kline/candlestick data."""
        return fetch_klines(
            client=client or self.client,
            symbol=symbol,
            interval=interval,
            start_time=start_time,
            limit=limit,
        )

    @staticmethod
    def get_utc_day_start(timestamp: int) -> int:
        """Get UTC+0 00:00 timestamp for given timestamp."""
        return calc_utc_day_start(timestamp)

    def get_price_change_from_utc_start(
        self,
        symbol: str,
        timestamp: int,
        client: Optional[BinanceFuturesRestClient] = None,
    ) -> Optional[float]:
        """Get the opening price for the UTC day of the given timestamp."""
        return get_price_change_from_utc_start_service(
            self,
            symbol=symbol,
            timestamp=timestamp,
            client=client,
        )

    def get_fees_for_symbol(
        self,
        symbol: str,
        since: int,
        until: int,
        client: Optional[BinanceFuturesRestClient] = None,
    ) -> Dict[int, float]:
        """Get commission and funding fees for a symbol."""
        records = self._fetch_income_history(since=since, until=until, client=client)
        fees_map = {}
        if records:
            for record in records:
                if record.get("symbol") != symbol:
                    continue
                if record.get("incomeType") in ("COMMISSION", "FUNDING_FEE"):
                    time_ts = int(record["time"])
                    income = float(record["income"])
                    fees_map[time_ts] = fees_map.get(time_ts, 0.0) + income
        return fees_map

    def get_fee_totals_by_symbol(
        self,
        since: int,
        until: int,
        client: Optional[BinanceFuturesRestClient] = None,
    ) -> Dict[str, float]:
        """Batch fetch income once and aggregate fees by symbol."""
        records = self._fetch_income_history(since=since, until=until, client=client)
        extra_types = getattr(self, "extra_loss_income_types", {"INSURANCE_CLEAR"})
        return aggregate_fee_totals_by_symbol(records, extra_loss_income_types=extra_types)

    @staticmethod
    def _summarize_income_records(
        records: List[Dict], extra_loss_income_types: Optional[set[str]] = None
    ) -> Tuple[List[str], Dict[str, float]]:
        return summarize_income_records(records, extra_loss_income_types=extra_loss_income_types)

    @staticmethod
    def _is_liquidation_order(order: Dict) -> bool:
        return is_liquidation_order_core(order)

    @staticmethod
    def match_orders_to_positions(
        orders: List[Dict],
        symbol: str,
        fees_map: Dict[int, float] = None,
        presorted: bool = False,
        initial_open_lots: Optional[List[Dict]] = None,
        return_open_lots: bool = False,
    ):
        return match_orders_to_positions_core(
            orders=orders,
            symbol=symbol,
            fees_map=fees_map,
            presorted=presorted,
            initial_open_lots=initial_open_lots,
            return_open_lots=return_open_lots,
        )

    @staticmethod
    def _match_position_side(orders: List[Dict], symbol: str, side: str) -> List[Dict]:
        return match_position_side_core(orders=orders, symbol=symbol, side=side)

    def get_traded_symbols(
        self,
        since: int,
        until: int,
        client: Optional[BinanceFuturesRestClient] = None,
    ) -> List[str]:
        """Get symbols that have actual trades using income history."""
        symbols_list, _fee_totals = self.get_traded_symbols_and_fee_totals(
            since=since,
            until=until,
            client=client,
        )
        return symbols_list

    def get_traded_symbols_and_fee_totals(
        self,
        since: int,
        until: int,
        client: Optional[BinanceFuturesRestClient] = None,
    ) -> tuple[List[str], Dict[str, float]]:
        """Fetch traded symbols and fee totals in one income-history pass."""
        symbols_list, fee_totals, _ranges = self.get_traded_symbols_fee_totals_and_ranges(
            since=since,
            until=until,
            client=client,
        )
        return symbols_list, fee_totals

    def get_traded_symbols_fee_totals_and_ranges(
        self,
        since: int,
        until: int,
        client: Optional[BinanceFuturesRestClient] = None,
    ) -> tuple[List[str], Dict[str, float], Dict[str, tuple[int, int]]]:
        symbols_list, fee_totals, activity_ranges, _cursor = self.get_incremental_income_activity(
            since=since,
            until=until,
            client=client,
        )
        if symbols_list:
            logger.info(f"Found {len(symbols_list)} symbols with activity: {symbols_list}")
        else:
            logger.warning("No symbols found in income history")
        return symbols_list, fee_totals, activity_ranges

    def get_incremental_income_activity(
        self,
        since: int,
        until: int,
        client: Optional[BinanceFuturesRestClient] = None,
    ):
        """Fetch all income types once and return activity plus the income cursor."""
        client = client or self.client
        logger.info("Fetching incremental activity from one all-type income pass...")
        records = self._fetch_income_history(
            since=since,
            until=until,
            client=client,
            income_type=None,
            fail_on_error=True,
        )
        symbols_list, fee_totals, activity_ranges = summarize_income_records_with_ranges(
            records,
            extra_loss_income_types=self.extra_loss_income_types,
        )
        ids = []
        for record in records:
            try:
                ids.append(int(record.get("tranId")))
            except (TypeError, ValueError):
                pass
        cursor = {
            "last_id": max(ids) if ids else None,
            "last_time_ms": int(until),
        }
        return symbols_list, fee_totals, activity_ranges, cursor

    def _extract_symbol_closed_positions(
        self,
        symbol: str,
        since: int,
        until: int,
        use_time_filter: bool = True,
        fee_totals_by_symbol: Optional[Dict[str, float]] = None,
        order_cursor: Optional[Dict] = None,
        trade_cursor: Optional[Dict] = None,
        cursor_overlap_minutes: int = 30,
        return_cursor_update: bool = False,
    ):
        """Extract and transform closed positions for one symbol."""
        return extract_symbol_closed_positions_service(
            self,
            symbol=symbol,
            since=since,
            until=until,
            use_time_filter=use_time_filter,
            fee_totals_by_symbol=fee_totals_by_symbol,
            order_cursor=order_cursor,
            trade_cursor=trade_cursor,
            cursor_overlap_minutes=cursor_overlap_minutes,
            return_cursor_update=return_cursor_update,
        )

    def analyze_orders(
        self,
        since: int,
        until: int,
        traded_symbols: Optional[List[str]] = None,
        use_time_filter: bool = True,
        symbol_since_map: Optional[Dict[str, int]] = None,
        symbol_until_map: Optional[Dict[str, int]] = None,
        prefetched_fee_totals: Optional[Dict[str, float]] = None,
        return_symbol_status: bool = False,
        order_cursors: Optional[Dict[str, Dict]] = None,
        trade_cursors: Optional[Dict[str, Dict]] = None,
        cursor_overlap_minutes: int = 30,
        return_cursor_updates: bool = False,
    ):
        """Analyze orders and convert to DataFrame."""
        return analyze_orders_service(
            self,
            since=since,
            until=until,
            traded_symbols=traded_symbols,
            use_time_filter=use_time_filter,
            symbol_since_map=symbol_since_map,
            symbol_until_map=symbol_until_map,
            prefetched_fee_totals=prefetched_fee_totals,
            return_symbol_status=return_symbol_status,
            order_cursors=order_cursors,
            trade_cursors=trade_cursors,
            cursor_overlap_minutes=cursor_overlap_minutes,
            return_cursor_updates=return_cursor_updates,
        )

    @staticmethod
    def _format_holding_time_cn(duration_seconds: float) -> str:
        return format_holding_time_cn(duration_seconds)

    @staticmethod
    def _position_to_trade_row(
        pos: Dict, idx: int, open_price_cache: Dict[tuple[str, int], Optional[float]]
    ) -> Dict:
        return position_to_trade_row(
            pos=pos,
            idx=idx,
            open_price_cache=open_price_cache,
            utc8=UTC8,
            get_utc_day_start=calc_utc_day_start,
        )

    @staticmethod
    def _build_trades_dataframe(
        *,
        all_positions: List[Dict],
        open_price_cache: Dict[tuple[str, int], Optional[float]],
    ) -> pd.DataFrame:
        trades_data = []
        for idx, pos in enumerate(all_positions, 1):
            try:
                trades_data.append(
                    position_to_trade_row(
                        pos=pos,
                        idx=idx,
                        open_price_cache=open_price_cache,
                        utc8=UTC8,
                        get_utc_day_start=calc_utc_day_start,
                    )
                )
                if idx % 20 == 0:
                    logger.info(f"Processed {idx}/{len(all_positions)} positions...")
            except Exception as e:
                logger.error(f"Parse position failed: {e}")
        return pd.DataFrame(trades_data)

    @staticmethod
    def merge_same_entry_positions(df: pd.DataFrame) -> pd.DataFrame:
        return merge_same_entry_positions(df)

    def export_to_excel(self, df: pd.DataFrame, filename: str = None):
        """Export analyzed trades to Excel file."""
        try:
            output_path = export_to_excel_file(df, filename=filename)
            logger.info(f"Exported to: {output_path}")
            logger.info(f"Total records: {len(df)}")
            if "PNL_Net" in df.columns:
                pnl_before_fees = df["PNL_Before_Fees"].sum() if "PNL_Before_Fees" in df.columns else 0
                total_fees = df["Fees"].sum() if "Fees" in df.columns else 0
                pnl_net = df["PNL_Net"].sum()
                win_count = len(df[df["PNL_Net"] > 0])
                loss_count = len(df[df["PNL_Net"] < 0])
                win_rate = win_count / len(df) * 100 if len(df) > 0 else 0
                logger.info(f"PNL Before Fees: {pnl_before_fees:.2f} USDT")
                logger.info(f"Total Fees: {total_fees:.2f} USDT")
                logger.info(f"Net PNL: {pnl_net:.2f} USDT")
                logger.info(f"Win Rate: {win_rate:.2f}% ({win_count} wins / {loss_count} losses)")
        except Exception as e:
            logger.error(f"Export to Excel failed: {e}")

    def get_real_positions(
        self, client: Optional[BinanceFuturesRestClient] = None
    ) -> Optional[Dict[str, float]]:
        """Get actual open positions from Binance (Symbol -> NetQty)."""
        active_client = client or self.client
        res = fetch_real_positions(client=active_client)
        if hasattr(active_client, "_latest_position_risk"):
            self._latest_position_risk = getattr(active_client, "_latest_position_risk")
            self._latest_position_risk_map = getattr(active_client, "_latest_position_risk_map")
        return res

    @staticmethod
    def _consume_fifo_entries(entries: List[Dict], qty_to_close: float):
        consume_fifo_entries(entries, qty_to_close)

    @staticmethod
    def _trim_entries_to_target_qty(entries: List[Dict], target_qty: float):
        trim_entries_to_target_qty(entries, target_qty)

    @staticmethod
    def _build_open_position_output(symbol: str, side_str: str, entries: List[Dict]) -> List[Dict]:
        return build_open_position_output(symbol, side_str, entries, utc8=UTC8)

    def _extract_open_positions_for_symbol(
        self, symbol: str, real_net_qty: float, since: int, until: int
    ) -> tuple[List[Dict], float]:
        return extract_open_positions_for_symbol_service(
            self,
            symbol=symbol,
            real_net_qty=real_net_qty,
            since=since,
            until=until,
        )

    def get_open_positions(
        self, since: int, until: int, traded_symbols: Optional[List[str]] = None
    ) -> Optional[List[Dict]]:
        """Get open entry orders (opened but not closed)."""
        return get_open_positions_service(
            self,
            since=since,
            until=until,
            traded_symbols=traded_symbols,
        )


def main():
    """Main CLI entry point for standalone trade analysis"""
    load_dotenv()

    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")

    start_date_str = os.getenv("START_DATE")
    end_date_str = os.getenv("END_DATE")
    days_to_fetch = int(os.getenv("DAYS_TO_FETCH", 30))

    if start_date_str:
        try:
            start_dt = datetime.strptime(start_date_str, "%Y-%m-%d").replace(
                hour=23, minute=0, second=0, microsecond=0
            )
            since = int(start_dt.timestamp() * 1000)

            if end_date_str:
                end_dt = datetime.strptime(end_date_str, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59, microsecond=999000
                )
                until = int(end_dt.timestamp() * 1000)
                time_range_desc = f"{start_date_str} to {end_date_str}"
            else:
                until = int(datetime.now().timestamp() * 1000)
                time_range_desc = f"from {start_date_str} to now"
        except ValueError as e:
            logger.error(f"Error parsing date: {e}. Format should be YYYY-MM-DD")
            return
    else:
        since = int((datetime.now() - timedelta(days=days_to_fetch)).timestamp() * 1000)
        until = int(datetime.now().timestamp() * 1000)
        time_range_desc = f"Last {days_to_fetch} days"

    if not api_key or not api_secret:
        logger.error("Please configure API keys first!")
        return

    logger.info("=" * 60)
    logger.info("Binance Futures Order Analyzer")
    logger.info(f"Query range: {time_range_desc}")
    logger.info(f"Start: {datetime.fromtimestamp(since/1000).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"End: {datetime.fromtimestamp(until/1000).strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    analyzer = TradeDataProcessor(api_key, api_secret)
    df = analyzer.analyze_orders(since=since, until=until)

    if df.empty:
        logger.warning("No order data found")
        return

    analyzer.export_to_excel(df)
    logger.info("Done!")


if __name__ == "__main__":
    main()
