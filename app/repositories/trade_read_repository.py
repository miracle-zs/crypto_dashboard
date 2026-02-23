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
        data["max_single_loss"] = float(data.get("max_drawdown", 0.0) or 0.0)
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

    def get_all_trades(self, limit: int = None):
        conn = self.db._get_connection()
        query = """
            SELECT no, date, entry_time, exit_time, holding_time, symbol, side,
                   price_change_pct, entry_amount, entry_price, exit_price, qty,
                   fees, pnl_net, close_type, return_rate, open_price,
                   pnl_before_fees, entry_order_id, exit_order_id
            FROM trades
            ORDER BY entry_time ASC
        """
        params = []
        if limit:
            query += " LIMIT ?"
            params.append(int(limit))

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
