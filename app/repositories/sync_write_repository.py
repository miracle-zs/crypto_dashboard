from datetime import datetime

from app.logger import logger


class SyncWriteRepository:
    def __init__(self, db):
        self.db = db
        self._open_positions_state_columns = None

    def update_sync_status(self, *, status: str, error_message, last_entry_time, total_trades: int):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE sync_status
            SET last_sync_time = CURRENT_TIMESTAMP,
                last_entry_time = ?,
                total_trades = ?,
                status = ?,
                error_message = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
            """,
            (last_entry_time, int(total_trades), status, error_message),
        )
        conn.commit()
        conn.close()
        return None

    def update_symbol_sync_success_batch(self, symbols, end_ms: int):
        if not symbols:
            return 0

        unique_symbols = sorted({str(s).upper() for s in symbols if s})
        if not unique_symbols:
            return 0

        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT INTO symbol_sync_state (
                symbol, last_success_end_ms, last_attempt_end_ms, last_error, updated_at
            ) VALUES (?, ?, ?, NULL, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                last_success_end_ms = excluded.last_success_end_ms,
                last_attempt_end_ms = excluded.last_attempt_end_ms,
                last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            """,
            [(symbol, int(end_ms), int(end_ms)) for symbol in unique_symbols],
        )
        conn.commit()
        conn.close()
        return len(unique_symbols)

    def update_symbol_sync_failure_batch(self, failures, end_ms: int):
        if not failures:
            return 0

        rows = []
        for symbol, error_message in failures.items():
            if not symbol:
                continue
            rows.append(
                (
                    str(symbol).upper(),
                    int(end_ms),
                    str(error_message or "")[:500],
                )
            )
        if not rows:
            return 0

        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.executemany(
            """
            INSERT INTO symbol_sync_state (
                symbol, last_attempt_end_ms, last_error, updated_at
            ) VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(symbol) DO UPDATE SET
                last_attempt_end_ms = excluded.last_attempt_end_ms,
                last_error = excluded.last_error,
                updated_at = CURRENT_TIMESTAMP
            """,
            rows,
        )
        conn.commit()
        conn.close()
        return len(rows)

    def upsert_sync_cursors(self, rows):
        normalized = []
        for row in rows or []:
            stream = str(row.get("stream") or "").strip()
            if not stream:
                continue
            symbol = str(row.get("symbol") or "").upper()
            last_id = row.get("last_id")
            last_time_ms = row.get("last_time_ms")
            normalized.append(
                (
                    stream,
                    symbol,
                    int(last_id) if last_id is not None else None,
                    int(last_time_ms) if last_time_ms is not None else None,
                )
            )
        if not normalized:
            return 0

        conn = self.db._get_connection()
        try:
            conn.executemany(
                """
                INSERT INTO sync_cursors (
                    stream, symbol, last_id, last_time_ms, updated_at
                ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(stream, symbol) DO UPDATE SET
                    last_id = CASE
                        WHEN excluded.last_id IS NULL THEN sync_cursors.last_id
                        WHEN sync_cursors.last_id IS NULL THEN excluded.last_id
                        ELSE MAX(sync_cursors.last_id, excluded.last_id)
                    END,
                    last_time_ms = CASE
                        WHEN excluded.last_time_ms IS NULL THEN sync_cursors.last_time_ms
                        WHEN sync_cursors.last_time_ms IS NULL THEN excluded.last_time_ms
                        ELSE MAX(sync_cursors.last_time_ms, excluded.last_time_ms)
                    END,
                    updated_at = CURRENT_TIMESTAMP
                """,
                normalized,
            )
            conn.commit()
            return len(normalized)
        finally:
            conn.close()

    def save_trades(self, df, overwrite: bool = False):
        return self.db.save_trades(df, overwrite=overwrite)

    def log_sync_run(self, **kwargs):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO sync_run_log (
                run_type,
                mode,
                status,
                symbol_count,
                rows_count,
                trades_saved,
                open_saved,
                elapsed_ms,
                error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                kwargs.get("run_type"),
                kwargs.get("mode"),
                kwargs.get("status"),
                int(kwargs.get("symbol_count", 0) or 0),
                int(kwargs.get("rows_count", 0) or 0),
                int(kwargs.get("trades_saved", 0) or 0),
                int(kwargs.get("open_saved", 0) or 0),
                int(kwargs.get("elapsed_ms", 0) or 0),
                (kwargs.get("error_message") or "")[:500],
            ),
        )
        conn.commit()
        conn.close()

    def save_open_positions(self, rows):
        conn = self.db._get_connection()
        cursor = conn.cursor()

        state_map = {}
        try:
            cursor.execute("PRAGMA table_info(open_positions)")
            table_columns = {info[1] for info in cursor.fetchall()}
            if self._open_positions_state_columns is None:
                query_cols = ["symbol", "order_id", "alerted"]
                if "last_alert_time" in table_columns:
                    query_cols.append("last_alert_time")
                if "profit_alerted" in table_columns:
                    query_cols.append("profit_alerted")
                if "profit_alert_time" in table_columns:
                    query_cols.append("profit_alert_time")
                if "reentry_alerted" in table_columns:
                    query_cols.append("reentry_alerted")
                if "reentry_alert_time" in table_columns:
                    query_cols.append("reentry_alert_time")
                if "is_long_term" in table_columns:
                    query_cols.append("is_long_term")
                if "is_incomplete" in table_columns:
                    query_cols.append("is_incomplete")
                self._open_positions_state_columns = tuple(query_cols)

            query_cols = list(self._open_positions_state_columns or ("symbol", "order_id", "alerted"))


            incoming_symbols = sorted({str(pos.get("symbol", "")) for pos in rows if pos.get("symbol")}) if rows else []
            if rows and incoming_symbols:
                placeholders = ",".join("?" for _ in incoming_symbols)
                cursor.execute(
                    f"SELECT {', '.join(query_cols)} FROM open_positions WHERE symbol IN ({placeholders})",
                    tuple(incoming_symbols),
                )
            else:
                cursor.execute("SELECT 1 WHERE 0")

            columns = set(query_cols)
            for row in cursor.fetchall():
                key = f"{row['symbol']}_{row['order_id']}"
                state_data = {"alerted": row["alerted"]}
                if "last_alert_time" in columns:
                    state_data["last_alert_time"] = row["last_alert_time"]
                if "profit_alerted" in columns:
                    state_data["profit_alerted"] = row["profit_alerted"]
                if "profit_alert_time" in columns:
                    state_data["profit_alert_time"] = row["profit_alert_time"]
                if "reentry_alerted" in columns:
                    state_data["reentry_alerted"] = row["reentry_alerted"]
                if "reentry_alert_time" in columns:
                    state_data["reentry_alert_time"] = row["reentry_alert_time"]
                if "is_long_term" in columns:
                    state_data["is_long_term"] = row["is_long_term"]
                state_map[key] = state_data
        except Exception as exc:
            conn.close()
            logger.error(f"加载 open_positions 历史状态失败: {exc}")
            raise RuntimeError("加载 open_positions 历史状态失败") from exc

        if not rows:
            cursor.execute("DELETE FROM open_positions")
            conn.commit()
            conn.close()
            return 0

        # Normalize incoming rows: if order_id is 0 or missing, assign distinct synthetic negative ID
        normalized_rows = []
        zero_order_indices = {}
        for pos in rows:
            p = dict(pos)
            oid = p.get("order_id")
            if oid is None or int(oid) == 0:
                sym = str(p.get("symbol", ""))
                side = str(p.get("side", "")).upper()
                key = (sym, side)
                idx = zero_order_indices.get(key, 0) + 1
                zero_order_indices[key] = idx
                base_code = 100 if side == "LONG" else (200 if side == "SHORT" else 300)
                p["order_id"] = -(base_code + idx)
            else:
                p["order_id"] = int(oid)
            normalized_rows.append(p)

        has_incomplete = "is_incomplete" in table_columns
        insert_rows = []
        for pos in normalized_rows:
            key = f"{pos['symbol']}_{pos['order_id']}"
            saved_state = state_map.get(key, {})
            row_data = [
                pos["date"],
                pos["symbol"],
                pos["side"],
                pos["entry_time"],
                pos["entry_price"],
                pos["qty"],
                pos["entry_amount"],
                pos["order_id"],
                saved_state.get("alerted", 0),
                saved_state.get("last_alert_time"),
                saved_state.get("profit_alerted", 0),
                saved_state.get("profit_alert_time"),
                saved_state.get("reentry_alerted", 0),
                saved_state.get("reentry_alert_time"),
                saved_state.get("is_long_term", 0),
            ]
            if has_incomplete:
                row_data.append(int(pos.get("is_incomplete", 0)))
            insert_rows.append(tuple(row_data))

        if insert_rows:
            col_names = [
                "date", "symbol", "side", "entry_time", "entry_price", "qty", "entry_amount", "order_id",
                "alerted", "last_alert_time", "profit_alerted", "profit_alert_time",
                "reentry_alerted", "reentry_alert_time", "is_long_term",
            ]
            if has_incomplete:
                col_names.append("is_incomplete")
            placeholders = ", ".join(["?"] * len(col_names))
            set_clauses = [f"{col} = excluded.{col}" for col in col_names if col not in ("symbol", "order_id")]
            sql = f"""
            INSERT INTO open_positions ({", ".join(col_names)})
            VALUES ({placeholders})
            ON CONFLICT(symbol, order_id) DO UPDATE SET
                {", ".join(set_clauses)}
            """
            cursor.executemany(sql, insert_rows)

            active_keys = sorted(
                {
                    (str(pos["symbol"]), int(pos["order_id"]))
                    for pos in normalized_rows
                    if pos.get("symbol") is not None and pos.get("order_id") is not None
                }
            )
            if active_keys:
                placeholders = ",".join(["(?, ?)"] * len(active_keys))
                params = []
                for symbol, order_id in active_keys:
                    params.extend([symbol, order_id])
                cursor.execute(
                    f"""
                    DELETE FROM open_positions
                    WHERE (symbol, order_id) NOT IN ({placeholders})
                    """,
                    tuple(params),
                )
            else:
                cursor.execute("DELETE FROM open_positions")

        conn.commit()
        conn.close()
        return len(rows)

    def save_position_snapshots(self, snapshot_time: str, positions: list) -> int:
        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            if not positions:
                cursor.execute(
                    """
                    INSERT INTO position_snapshots (
                        snapshot_time, symbol, position_side, qty, entry_price,
                        mark_price, unrealized_pnl, liquidation_price, leverage,
                        margin_type, is_complete, created_at
                    ) VALUES (?, '__EMPTY__', 'BOTH', 0.0, 0.0, 0.0, 0.0, 0.0, 0, 'cross', 1, CURRENT_TIMESTAMP)
                    ON CONFLICT(snapshot_time, symbol, position_side) DO NOTHING
                    """,
                    (str(snapshot_time),),
                )
                conn.commit()
                return 1

            insert_rows = []
            for p in positions:
                insert_rows.append(
                    (
                        str(snapshot_time),
                        str(p.get("symbol", "")),
                        str(p.get("position_side") or p.get("side") or "BOTH"),
                        float(p.get("qty") or p.get("positionAmt") or 0.0),
                        float(p.get("entry_price") or p.get("entryPrice") or 0.0),
                        float(p.get("mark_price") or p.get("markPrice") or 0.0),
                        float(p.get("unrealized_pnl") or p.get("unRealizedProfit") or 0.0),
                        float(p.get("liquidation_price") or p.get("liquidationPrice") or 0.0),
                        int(p.get("leverage") or 0),
                        str(p.get("margin_type") or p.get("marginType") or "cross"),
                        int(p.get("is_complete", 1)),
                    )
                )
            cursor.executemany(
                """
                INSERT INTO position_snapshots (
                    snapshot_time, symbol, position_side, qty, entry_price,
                    mark_price, unrealized_pnl, liquidation_price, leverage,
                    margin_type, is_complete, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(snapshot_time, symbol, position_side) DO UPDATE SET
                    qty = excluded.qty,
                    entry_price = excluded.entry_price,
                    mark_price = excluded.mark_price,
                    unrealized_pnl = excluded.unrealized_pnl,
                    liquidation_price = excluded.liquidation_price,
                    leverage = excluded.leverage,
                    margin_type = excluded.margin_type,
                    is_complete = excluded.is_complete,
                    created_at = CURRENT_TIMESTAMP
                """,
                insert_rows,
            )
            conn.commit()
            return len(insert_rows)
        finally:
            conn.close()

    def get_latest_position_snapshots(self) -> list:
        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT snapshot_time FROM position_snapshots ORDER BY id DESC LIMIT 1"
            )
            row = cursor.fetchone()
            if not row or not row["snapshot_time"]:
                return []
            latest_time = row["snapshot_time"]
            cursor.execute(
                "SELECT * FROM position_snapshots WHERE snapshot_time = ? AND symbol != '__EMPTY__'",
                (latest_time,),
            )
            return [dict(r) for r in cursor.fetchall()]
        except Exception:
            return []
        finally:
            conn.close()


    def save_transfer_income(self, **kwargs):
        event_time = int(kwargs["event_time"])
        ts_str = datetime.utcfromtimestamp(event_time / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")

        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO transfers (
                timestamp, amount, type, description, event_time, asset, income_type, source_uid
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ts_str,
                float(kwargs["amount"]),
                "binance_income",
                kwargs.get("description"),
                event_time,
                str(kwargs.get("asset") or "USDT"),
                str(kwargs.get("income_type") or "TRANSFER"),
                kwargs.get("source_uid"),
            ),
        )
        inserted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        return inserted

    def save_execution_facts(self, facts: list, venue: str = "binance_futures") -> int:
        if not facts:
            return 0
        import json

        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            insert_rows = []
            for item in facts:
                trade_id = int(item.get("id") or item.get("trade_id") or 0)
                order_id = int(item.get("orderId") or item.get("order_id") or 0)
                symbol = str(item.get("symbol", "")).upper()
                side = str(item.get("side", "")).upper()
                position_side = str(item.get("positionSide") or item.get("position_side") or "BOTH").upper()
                price = float(item.get("price", 0.0))
                qty = float(item.get("qty", 0.0))
                realized_pnl = float(item.get("realizedPnl") or item.get("realized_pnl") or 0.0)
                quote_qty = float(item.get("quoteQty") or item.get("quote_qty") or (price * qty))
                commission = float(item.get("commission", 0.0))
                commission_asset = str(item.get("commissionAsset") or item.get("commission_asset") or "USDT").upper()
                time_ms = int(item.get("time") or item.get("time_ms") or 0)
                is_buyer = 1 if item.get("buyer") or item.get("is_buyer") else 0
                is_maker = 1 if item.get("maker") or item.get("is_maker") else 0
                raw_payload = json.dumps(item, ensure_ascii=False)
                insert_rows.append(
                    (
                        venue, symbol, trade_id, order_id, side, position_side,
                        price, qty, realized_pnl, quote_qty, commission, commission_asset,
                        time_ms, is_buyer, is_maker, raw_payload,
                    )
                )
            before_changes = conn.total_changes
            cursor.executemany(
                """
                INSERT INTO execution_facts (
                    venue, symbol, trade_id, order_id, side, position_side,
                    price, qty, realized_pnl, quote_qty, commission, commission_asset,
                    time_ms, is_buyer, is_maker, raw_payload, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(venue, symbol, trade_id) DO NOTHING
                """,
                insert_rows,
            )
            inserted = conn.total_changes - before_changes
            conn.commit()
            return inserted
        finally:
            conn.close()

    def get_execution_facts(
        self,
        symbol: str = None,
        since_ms: int = None,
        until_ms: int = None,
        venue: str = "binance_futures",
    ) -> list:
        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            conditions = ["venue = ?"]
            params = [venue]
            if symbol:
                conditions.append("symbol = ?")
                params.append(symbol.upper())
            if since_ms is not None:
                conditions.append("time_ms >= ?")
                params.append(int(since_ms))
            if until_ms is not None:
                conditions.append("time_ms <= ?")
                params.append(int(until_ms))
            where_clause = " WHERE " + " AND ".join(conditions)
            sql = f"SELECT * FROM execution_facts{where_clause} ORDER BY time_ms ASC, trade_id ASC"
            cursor.execute(sql, tuple(params))
            return [dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()

    def save_income_facts(self, facts: list, venue: str = "binance_futures") -> int:
        if not facts:
            return 0
        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            insert_rows = []
            for item in facts:
                symbol = str(item.get("symbol") or "").upper()
                income_type = str(item.get("incomeType") or item.get("income_type") or "").upper()
                income = float(item.get("income", 0.0))
                asset = str(item.get("asset") or "USDT").upper()
                time_ms = int(item.get("time") or item.get("time_ms") or 0)
                tran_id = item.get("tranId") or item.get("tran_id")
                tran_id_val = int(tran_id) if tran_id not in (None, "") else None
                trade_id = item.get("tradeId") or item.get("trade_id")
                trade_id_val = int(trade_id) if trade_id not in (None, "") else None
                info = str(item.get("info") or "")
                insert_rows.append(
                    (
                        venue, symbol, income_type, income, asset, time_ms,
                        tran_id_val, trade_id_val, info,
                    )
                )
            before_changes = conn.total_changes
            cursor.executemany(
                """
                INSERT INTO income_facts (
                    venue, symbol, income_type, income, asset, time_ms,
                    tran_id, trade_id, info, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(venue, income_type, tran_id) DO NOTHING
                """,
                insert_rows,
            )
            inserted = conn.total_changes - before_changes
            conn.commit()
            return inserted

        finally:
            conn.close()

    def get_income_facts(
        self,
        symbol: str = None,
        income_type: str = None,
        since_ms: int = None,
        until_ms: int = None,
        venue: str = "binance_futures",
    ) -> list:
        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            conditions = ["venue = ?"]
            params = [venue]
            if symbol:
                conditions.append("symbol = ?")
                params.append(symbol.upper())
            if income_type:
                conditions.append("income_type = ?")
                params.append(income_type.upper())
            if since_ms is not None:
                conditions.append("time_ms >= ?")
                params.append(int(since_ms))
            if until_ms is not None:
                conditions.append("time_ms <= ?")
                params.append(int(until_ms))
            where_clause = " WHERE " + " AND ".join(conditions)
            sql = f"SELECT * FROM income_facts{where_clause} ORDER BY time_ms ASC, id ASC"
            cursor.execute(sql, tuple(params))
            return [dict(r) for r in cursor.fetchall()]
        finally:
            conn.close()

