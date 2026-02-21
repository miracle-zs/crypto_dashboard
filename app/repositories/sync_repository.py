from datetime import datetime

from app.repositories.open_positions_query import fetch_open_positions
from app.repositories.trade_repository import TradeRepository


class SyncRepository:
    def __init__(self, db):
        self.db = db
        self.trade_repo = TradeRepository(db)
        self._open_positions_state_columns = None

    def get_last_entry_time(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(entry_time) FROM trades")
        row = cursor.fetchone()
        conn.close()
        return row[0] if row and row[0] else None

    def update_sync_status(self, **kwargs):
        status = kwargs.get("status", "idle")
        error_message = kwargs.get("error_message")
        last_entry_time = self.get_last_entry_time()

        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM trades")
        total_trades = int(cursor.fetchone()[0] or 0)
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
            (last_entry_time, total_trades, status, error_message),
        )
        conn.commit()
        conn.close()
        return None

    def get_symbol_sync_watermarks(self, symbols):
        if not symbols:
            return {}

        normalized = [str(symbol).upper() for symbol in symbols]
        placeholders = ",".join("?" for _ in normalized)

        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT symbol, last_success_end_ms
            FROM symbol_sync_state
            WHERE symbol IN ({placeholders})
            """,
            tuple(normalized),
        )
        rows = cursor.fetchall()
        conn.close()

        watermarks = {symbol: None for symbol in normalized}
        for row in rows:
            watermarks[row["symbol"]] = row["last_success_end_ms"]
        return watermarks

    def update_symbol_sync_success(self, **kwargs):
        symbol = kwargs.get("symbol")
        end_ms = kwargs.get("end_ms")
        if not symbol or end_ms is None:
            return 0
        return self.update_symbol_sync_success_batch([symbol], end_ms=int(end_ms))

    def update_symbol_sync_failure(self, **kwargs):
        symbol = kwargs.get("symbol")
        end_ms = kwargs.get("end_ms")
        error_message = kwargs.get("error_message")
        if not symbol or end_ms is None:
            return 0
        return self.update_symbol_sync_failure_batch(
            failures={str(symbol).upper(): error_message},
            end_ms=int(end_ms),
        )

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

    def save_trades(self, df, overwrite: bool = False):
        if df.empty:
            return 0

        conn = self.db._get_connection()
        cursor = conn.cursor()

        if overwrite and "Entry_Time" in df.columns:
            min_time = df["Entry_Time"].min()
            max_time = df["Entry_Time"].max()
            cursor.execute(
                "DELETE FROM trades WHERE entry_time >= ? AND entry_time <= ?",
                (min_time, max_time),
            )

        upsert_rows = []
        for row in df.itertuples(index=False):
            upsert_rows.append(
                (
                    int(row.No),
                    row.Date,
                    row.Entry_Time,
                    row.Exit_Time,
                    row.Holding_Time,
                    row.Symbol,
                    row.Side,
                    float(row.Price_Change_Pct),
                    float(row.Entry_Amount),
                    float(row.Entry_Price),
                    float(row.Exit_Price),
                    float(row.Qty),
                    float(row.Fees),
                    float(row.PNL_Net),
                    row.Close_Type,
                    row.Return_Rate,
                    float(row.Open_Price),
                    float(row.PNL_Before_Fees),
                    int(row.Entry_Order_ID),
                    str(row.Exit_Order_ID),
                )
            )

        cursor.executemany(
            """
            INSERT INTO trades (
                no, date, entry_time, exit_time, holding_time, symbol, side,
                price_change_pct, entry_amount, entry_price, exit_price, qty,
                fees, pnl_net, close_type, return_rate, open_price,
                pnl_before_fees, entry_order_id, exit_order_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(symbol, entry_order_id, exit_order_id) DO UPDATE SET
                no = excluded.no,
                date = excluded.date,
                entry_time = excluded.entry_time,
                exit_time = excluded.exit_time,
                holding_time = excluded.holding_time,
                side = excluded.side,
                price_change_pct = excluded.price_change_pct,
                entry_amount = excluded.entry_amount,
                entry_price = excluded.entry_price,
                exit_price = excluded.exit_price,
                qty = excluded.qty,
                fees = excluded.fees,
                pnl_net = excluded.pnl_net,
                close_type = excluded.close_type,
                return_rate = excluded.return_rate,
                open_price = excluded.open_price,
                pnl_before_fees = excluded.pnl_before_fees,
                updated_at = CURRENT_TIMESTAMP
            """,
            upsert_rows,
        )

        conn.commit()
        conn.close()
        return len(upsert_rows)

    def recompute_trade_summary(self):
        return self.trade_repo.recompute_trade_summary()

    def get_statistics(self):
        return self.trade_repo.get_statistics()

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

    def get_sync_status(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM sync_status WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        return dict(row) if row else {}

    def list_sync_run_logs(self, limit: int = 100):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                id,
                run_type,
                mode,
                status,
                symbol_count,
                rows_count,
                trades_saved,
                open_saved,
                elapsed_ms,
                error_message,
                created_at
            FROM sync_run_log
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_open_positions(self):
        return fetch_open_positions(self.db)

    def save_open_positions(self, rows):
        conn = self.db._get_connection()
        cursor = conn.cursor()

        state_map = {}
        try:
            if self._open_positions_state_columns is None:
                cursor.execute("PRAGMA table_info(open_positions)")
                columns = [info[1] for info in cursor.fetchall()]
                query_cols = ["symbol", "order_id", "alerted"]
                if "last_alert_time" in columns:
                    query_cols.append("last_alert_time")
                if "profit_alerted" in columns:
                    query_cols.append("profit_alerted")
                if "profit_alert_time" in columns:
                    query_cols.append("profit_alert_time")
                if "reentry_alerted" in columns:
                    query_cols.append("reentry_alerted")
                if "reentry_alert_time" in columns:
                    query_cols.append("reentry_alert_time")
                if "is_long_term" in columns:
                    query_cols.append("is_long_term")
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
        except Exception:
            pass

        cursor.execute("DELETE FROM open_positions")

        if not rows:
            conn.commit()
            conn.close()
            return 0

        insert_rows = []
        for pos in rows:
            key = f"{pos['symbol']}_{pos['order_id']}"
            saved_state = state_map.get(key, {})
            insert_rows.append(
                (
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
                )
            )

        if insert_rows:
            cursor.executemany(
                """
                INSERT INTO open_positions (
                    date, symbol, side, entry_time, entry_price, qty, entry_amount, order_id,
                    alerted, last_alert_time, profit_alerted, profit_alert_time,
                    reentry_alerted, reentry_alert_time, is_long_term
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                insert_rows,
            )

        conn.commit()
        conn.close()
        return len(rows)

    def get_latest_transfer_event_time(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT MAX(event_time) AS latest_event_time
            FROM transfers
            WHERE event_time IS NOT NULL
            """
        )
        row = cursor.fetchone()
        conn.close()
        if not row or row["latest_event_time"] is None:
            return None
        return int(row["latest_event_time"])

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
