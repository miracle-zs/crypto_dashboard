import json
from datetime import datetime, timedelta, timezone

import pandas as pd

from app.repositories.open_positions_query import fetch_open_position_symbols, fetch_open_positions


class TradeReadRepository:
    def __init__(self, db):
        self.db = db

    def get_trade_summary(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                id,
                total_pnl,
                total_fees,
                win_rate,
                win_count,
                loss_count,
                total_trades,
                equity_curve,
                current_streak,
                best_win_streak,
                worst_loss_streak,
                max_single_loss,
                max_drawdown,
                profit_factor,
                kelly_criterion,
                sqn,
                expected_value,
                risk_reward_ratio,
                updated_at
            FROM trade_summary
            WHERE id = 1
            """
        )
        row = cursor.fetchone()
        conn.close()

        if not row:
            return None
        data = dict(row)
        data.pop("id", None)
        data.pop("updated_at", None)
        if data.get("equity_curve"):
            try:
                data["equity_curve"] = json.loads(data["equity_curve"])
            except Exception:
                data["equity_curve"] = []
        else:
            data["equity_curve"] = []
        max_single_loss = data.get("max_single_loss")
        max_drawdown = data.get("max_drawdown")
        if max_single_loss is None:
            max_single_loss = max_drawdown
        if max_drawdown is None:
            max_drawdown = max_single_loss
        data["max_single_loss"] = float(max_single_loss or 0.0)
        data["max_drawdown"] = float(max_drawdown or 0.0)
        return data

    def get_statistics(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_trades,
                MIN(entry_time) AS earliest_trade,
                MAX(entry_time) AS latest_trade,
                COUNT(DISTINCT symbol) AS unique_symbols
            FROM trades
            """
        )
        row = cursor.fetchone()
        conn.close()

        return {
            "total_trades": int(row["total_trades"] or 0) if row else 0,
            "earliest_trade": row["earliest_trade"] if row else None,
            "latest_trade": row["latest_trade"] if row else None,
            "unique_symbols": int(row["unique_symbols"] or 0) if row else 0,
        }

    def get_cached_total_trades(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT total_trades FROM sync_status WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        if not row or row["total_trades"] is None:
            return None
        return int(row["total_trades"])

    def get_all_trades(self, limit: int = None, offset: int = 0):
        conn = self.db._get_connection()
        base_select = """
            SELECT no, date, entry_time, exit_time, holding_time, symbol, side,
                   price_change_pct, entry_amount, entry_price, exit_price, qty,
                   fees, pnl_net, close_type, return_rate, open_price,
                   pnl_before_fees, entry_order_id, exit_order_id
        """
        params = []
        if limit is not None:
            query = f"""
                SELECT *
                FROM (
                    {base_select}
                    FROM trades
                    ORDER BY entry_time DESC
                    LIMIT ?
                """
            params.append(int(limit))
            if offset > 0:
                query += " OFFSET ?"
                params.append(int(offset))
            query += """
                ) recent
                ORDER BY entry_time ASC
            """
        elif offset > 0:
            query = f"{base_select} FROM trades ORDER BY entry_time ASC LIMIT -1 OFFSET ?"
            params.append(int(offset))
        else:
            query = f"{base_select} FROM trades ORDER BY entry_time ASC"

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        if not df.empty:
            df.columns = [
                "No",
                "Date",
                "Entry_Time",
                "Exit_Time",
                "Holding_Time",
                "Symbol",
                "Side",
                "Price_Change_Pct",
                "Entry_Amount",
                "Entry_Price",
                "Exit_Price",
                "Qty",
                "Fees",
                "PNL_Net",
                "Close_Type",
                "Return_Rate",
                "Open_Price",
                "PNL_Before_Fees",
                "Entry_Order_ID",
                "Exit_Order_ID",
            ]
        return df

    def get_open_positions(self):
        return fetch_open_positions(self.db)

    def get_open_position_symbols(self):
        return fetch_open_position_symbols(self.db)

    def get_balance_history(self, **kwargs):
        start_time = kwargs.get("start_time")
        end_time = kwargs.get("end_time")
        limit = kwargs.get("limit")

        conn = self.db._get_connection()
        cursor = conn.cursor()
        query = "SELECT timestamp, balance, wallet_balance FROM balance_history WHERE 1=1"
        params = []

        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time.isoformat().replace("T", " "))
        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time.isoformat().replace("T", " "))

        query += " ORDER BY timestamp DESC"
        if limit:
            query += " LIMIT ?"
            params.append(limit)

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in reversed(rows)]

    def get_transfers(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT *
            FROM transfers
            WHERE (type != 'auto') OR (source_uid IS NOT NULL)
            ORDER BY timestamp ASC
            """
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_transfer_timeline(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT timestamp, amount
            FROM transfers
            WHERE (type != 'auto') OR (source_uid IS NOT NULL)
            ORDER BY timestamp ASC
            """
        )
        rows = cursor.fetchall()
        conn.close()
        return [dict(row) for row in rows]

    def get_daily_stats(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                date,
                SUM(trade_count) as trade_count,
                SUM(total_amount) as total_amount,
                SUM(total_pnl) as total_pnl,
                SUM(win_count) as win_count,
                SUM(loss_count) as loss_count
            FROM (
                SELECT
                    date,
                    COUNT(*) as trade_count,
                    SUM(entry_amount) as total_amount,
                    SUM(pnl_net) as total_pnl,
                    SUM(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END) as win_count,
                    SUM(CASE WHEN pnl_net < 0 THEN 1 ELSE 0 END) as loss_count
                FROM trades
                GROUP BY date
                UNION ALL
                SELECT
                    date,
                    COUNT(*) as trade_count,
                    SUM(entry_amount) as total_amount,
                    0 as total_pnl,
                    0 as win_count,
                    0 as loss_count
                FROM open_positions
                GROUP BY date
            )
            GROUP BY date
            ORDER BY date DESC
            """
        )
        rows = cursor.fetchall()
        conn.close()

        results = []
        for row in rows:
            trade_count = row["trade_count"]
            win_count = row["win_count"]
            win_rate = (win_count / trade_count * 100) if trade_count > 0 else 0.0
            results.append(
                {
                    "date": row["date"],
                    "trade_count": trade_count,
                    "total_amount": float(row["total_amount"] or 0),
                    "total_pnl": float(row["total_pnl"] or 0),
                    "win_count": win_count,
                    "loss_count": row["loss_count"],
                    "win_rate": round(win_rate, 2),
                }
            )
        return results

    def get_monthly_target(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT monthly_target FROM user_settings WHERE id = 1")
        row = cursor.fetchone()
        conn.close()
        return row["monthly_target"] if row else 30000

    def get_monthly_pnl(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        utc8 = timezone(timedelta(hours=8))
        now = datetime.now(utc8)
        month_start = now.strftime("%Y%m01")
        cursor.execute(
            """
            SELECT COALESCE(SUM(pnl_net), 0) as monthly_pnl
            FROM trades
            WHERE date >= ?
            """,
            (month_start,),
        )
        row = cursor.fetchone()
        conn.close()
        return float(row["monthly_pnl"]) if row else 0.0

    def get_trade_aggregates(self, window: str = "all"):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        utc8 = timezone(timedelta(hours=8))
        now = datetime.now(utc8)
        window = str(window or "all").lower()
        if window not in {"all", "7d", "30d"}:
            window = "all"
        window_since = None
        if window == "7d":
            window_since = (now - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        elif window == "30d":
            window_since = (now - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

        if window_since is not None:
            source_where = " WHERE entry_time IS NOT NULL AND entry_time >= ? "
            source_params = (window_since,)
        else:
            source_where = ""
            source_params = ()

        cursor.execute(
            """
            SELECT
                COUNT(*) AS trades_count,
                COALESCE(MAX(updated_at), '') AS latest_trade_updated_at
            FROM trades
            """
            + source_where,
            source_params,
        )
        source_row = cursor.fetchone()
        source_trades_count = int(source_row["trades_count"] or 0) if source_row else 0
        source_latest_updated_at = str(source_row["latest_trade_updated_at"] or "")

        if window == "all":
            cursor.execute(
                """
                SELECT trades_count, latest_trade_updated_at, payload_json
                FROM trade_aggregates_cache
                WHERE id = 1
                """
            )
            cache_row = cursor.fetchone()
            if cache_row:
                cached_payload = cache_row["payload_json"]
                cache_trades_count = int(cache_row["trades_count"] or 0)
                cache_latest_updated_at = str(cache_row["latest_trade_updated_at"] or "")
                if (
                    cached_payload
                    and cache_trades_count == source_trades_count
                    and cache_latest_updated_at == source_latest_updated_at
                ):
                    try:
                        payload = json.loads(cached_payload)
                        conn.close()
                        return payload
                    except Exception:
                        pass

        # Hourly net pnl (0-23)
        hourly_pnl = [0.0] * 24
        hourly_sql = """
            SELECT CAST(strftime('%H', entry_time) AS INTEGER) AS hour, COALESCE(SUM(pnl_net), 0) AS total_pnl
            FROM trades
            WHERE entry_time IS NOT NULL
            """
        hourly_params = []
        if window_since is not None:
            hourly_sql += " AND entry_time >= ? "
            hourly_params.append(window_since)
        hourly_sql += " GROUP BY hour "
        cursor.execute(hourly_sql, tuple(hourly_params))

        for row in cursor.fetchall():
            hour = row["hour"]
            if hour is None:
                continue
            hour = int(hour)
            if 0 <= hour <= 23:
                hourly_pnl[hour] = float(row["total_pnl"] or 0.0)

        # Duration buckets via SQL-calculated minutes.
        duration_labels = ["0-5m", "5-15m", "15-30m", "30-60m", "1-2h", "2h+"]
        bucket_map = {
            label: {"label": label, "trade_count": 0, "win_pnl": 0.0, "loss_pnl": 0.0}
            for label in duration_labels
        }
        duration_bucket_sql = """
            SELECT
                CASE
                    WHEN duration_minutes < 5 THEN '0-5m'
                    WHEN duration_minutes < 15 THEN '5-15m'
                    WHEN duration_minutes < 30 THEN '15-30m'
                    WHEN duration_minutes < 60 THEN '30-60m'
                    WHEN duration_minutes < 120 THEN '1-2h'
                    ELSE '2h+'
                END AS bucket,
                COUNT(*) AS trade_count,
                COALESCE(SUM(CASE WHEN pnl_net >= 0 THEN pnl_net ELSE 0 END), 0) AS win_pnl,
                COALESCE(SUM(CASE WHEN pnl_net < 0 THEN pnl_net ELSE 0 END), 0) AS loss_pnl
            FROM (
                SELECT
                    pnl_net,
                    MAX(
                        0.0,
                        (julianday(exit_time) - julianday(entry_time)) * 24.0 * 60.0
                    ) AS duration_minutes
                FROM trades
                WHERE entry_time IS NOT NULL AND exit_time IS NOT NULL
        """
        duration_bucket_params = []
        if window_since is not None:
            duration_bucket_sql += " AND entry_time >= ? "
            duration_bucket_params.append(window_since)
        duration_bucket_sql += """
            ) t
            GROUP BY bucket
        """
        cursor.execute(duration_bucket_sql, tuple(duration_bucket_params))
        for row in cursor.fetchall():
            bucket = str(row["bucket"] or "")
            if bucket not in bucket_map:
                continue
            bucket_map[bucket] = {
                "label": bucket,
                "trade_count": int(row["trade_count"] or 0),
                "win_pnl": float(row["win_pnl"] or 0.0),
                "loss_pnl": float(row["loss_pnl"] or 0.0),
            }

        # Duration scatter points (sample recent records for rendering performance).
        duration_scatter_sql = """
            SELECT
                symbol,
                holding_time,
                pnl_net,
                MAX(
                    0.0,
                    (julianday(exit_time) - julianday(entry_time)) * 24.0 * 60.0
                ) AS duration_minutes
            FROM trades
            WHERE entry_time IS NOT NULL
              AND exit_time IS NOT NULL
        """
        duration_scatter_params = []
        if window_since is not None:
            duration_scatter_sql += " AND entry_time >= ? "
            duration_scatter_params.append(window_since)
        duration_scatter_sql += """
            ORDER BY entry_time DESC
            LIMIT 1200
        """
        cursor.execute(duration_scatter_sql, tuple(duration_scatter_params))
        duration_points = [
            {
                "x": round(float(row["duration_minutes"] or 0.0), 1),
                "y": float(row["pnl_net"] or 0.0),
                "symbol": str(row["symbol"] or "--"),
                "time": str(row["holding_time"] or "--"),
            }
            for row in cursor.fetchall()
        ]

        symbol_rank_sql = """
            SELECT
                symbol,
                COALESCE(SUM(pnl_net), 0) AS pnl,
                COUNT(*) AS trade_count,
                SUM(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END) AS win_count
            FROM trades
            WHERE entry_time IS NOT NULL
        """
        symbol_rank_params = []
        if window_since is not None:
            symbol_rank_sql += " AND entry_time >= ? "
            symbol_rank_params.append(window_since)
        symbol_rank_sql += """
            GROUP BY symbol
        """
        cursor.execute(symbol_rank_sql, tuple(symbol_rank_params))
        rows = cursor.fetchall()

        symbol_rows = []
        total_abs_pnl = 0.0
        for row in rows:
            symbol = str(row["symbol"] or "--")
            pnl = float(row["pnl"] or 0.0)
            trade_count = int(row["trade_count"] or 0)
            win_count = int(row["win_count"] or 0)
            win_rate = (win_count / trade_count * 100.0) if trade_count > 0 else 0.0
            total_abs_pnl += abs(pnl)
            symbol_rows.append(
                {
                    "symbol": symbol,
                    "pnl": pnl,
                    "trade_count": trade_count,
                    "win_rate": round(win_rate, 1),
                }
            )

        total_abs_pnl = total_abs_pnl or 1.0
        for item in symbol_rows:
            item["share"] = round(abs(item["pnl"]) / total_abs_pnl * 100.0, 1)

        winners = sorted(
            [row for row in symbol_rows if row["pnl"] > 0],
            key=lambda x: x["pnl"],
            reverse=True,
        )[:5]
        losers = sorted(
            [row for row in symbol_rows if row["pnl"] < 0],
            key=lambda x: x["pnl"],
        )[:5]

        payload = {
            "duration_buckets": [bucket_map[label] for label in duration_labels],
            "duration_points": duration_points,
            "hourly_pnl": hourly_pnl,
            "symbol_rank": {
                "winners": winners,
                "losers": losers,
            },
        }
        if window == "all":
            cursor.execute(
                """
                INSERT INTO trade_aggregates_cache (
                    id, trades_count, latest_trade_updated_at, payload_json, updated_at
                ) VALUES (1, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(id) DO UPDATE SET
                    trades_count = excluded.trades_count,
                    latest_trade_updated_at = excluded.latest_trade_updated_at,
                    payload_json = excluded.payload_json,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    source_trades_count,
                    source_latest_updated_at,
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
        conn.commit()
        conn.close()
        return payload
