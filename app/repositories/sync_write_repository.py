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
        except Exception as exc:
            conn.close()
            logger.error(f"加载 open_positions 历史状态失败: {exc}")
            raise RuntimeError("加载 open_positions 历史状态失败") from exc

        if not rows:
            cursor.execute("DELETE FROM open_positions")
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
                ON CONFLICT(symbol, order_id) DO UPDATE SET
                    date = excluded.date,
                    side = excluded.side,
                    entry_time = excluded.entry_time,
                    entry_price = excluded.entry_price,
                    qty = excluded.qty,
                    entry_amount = excluded.entry_amount,
                    alerted = excluded.alerted,
                    last_alert_time = excluded.last_alert_time,
                    profit_alerted = excluded.profit_alerted,
                    profit_alert_time = excluded.profit_alert_time,
                    reentry_alerted = excluded.reentry_alerted,
                    reentry_alert_time = excluded.reentry_alert_time,
                    is_long_term = excluded.is_long_term
                """,
                insert_rows,
            )

            active_keys = sorted(
                {
                    (str(pos["symbol"]), int(pos["order_id"]))
                    for pos in rows
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
