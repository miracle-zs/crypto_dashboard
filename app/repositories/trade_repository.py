import json
from datetime import datetime
from datetime import timedelta, timezone

import numpy as np
import pandas as pd

from app.repositories.open_positions_query import fetch_open_positions


class TradeRepository:
    def __init__(self, db):
        self.db = db

    def get_trade_summary(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM trade_summary WHERE id = 1")
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

    def recompute_trade_summary(self):
        df = self.get_all_trades()

        if df.empty:
            summary = {
                "total_pnl": 0.0,
                "total_fees": 0.0,
                "win_rate": 0.0,
                "win_count": 0,
                "loss_count": 0,
                "total_trades": 0,
                "equity_curve": [],
                "current_streak": 0,
                "best_win_streak": 0,
                "worst_loss_streak": 0,
                "max_single_loss": 0.0,
                "max_drawdown": 0.0,
                "profit_factor": 0.0,
                "kelly_criterion": 0.0,
                "sqn": 0.0,
                "expected_value": 0.0,
                "risk_reward_ratio": 0.0,
            }
            self._save_trade_summary(summary)
            return summary

        total_pnl = float(df["PNL_Net"].sum())
        total_fees = float(df["Fees"].sum())
        win_count = len(df[df["PNL_Net"] > 0])
        loss_count = len(df[df["PNL_Net"] < 0])
        total_trades = len(df)
        win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0.0
        equity_curve = df["PNL_Net"].cumsum().tolist()

        current_streak = 0
        if not df.empty:
            pnl_values = df["PNL_Net"].values
            last_pnl = pnl_values[-1]
            for pnl in reversed(pnl_values):
                if pnl == 0:
                    break
                if last_pnl > 0:
                    if pnl > 0:
                        current_streak += 1
                    else:
                        break
                elif last_pnl < 0:
                    if pnl < 0:
                        current_streak -= 1
                    else:
                        break

        best_win_streak = 0
        worst_loss_streak = 0
        streak = 0
        for pnl in df["PNL_Net"].values:
            if pnl > 0:
                streak = streak + 1 if streak >= 0 else 1
            elif pnl < 0:
                streak = streak - 1 if streak <= 0 else -1
            else:
                streak = 0
            if streak > best_win_streak:
                best_win_streak = streak
            if streak < worst_loss_streak:
                worst_loss_streak = streak

        max_single_loss = float(df["PNL_Net"].min()) if not df.empty else 0.0
        total_wins = float(df[df["PNL_Net"] > 0]["PNL_Net"].sum())
        total_losses = abs(float(df[df["PNL_Net"] < 0]["PNL_Net"].sum()))
        profit_factor = (total_wins / total_losses) if total_losses > 0 else 0.0

        avg_win = total_wins / win_count if win_count > 0 else 0
        avg_loss = total_losses / loss_count if loss_count > 0 else 0
        win_prob = win_count / total_trades if total_trades > 0 else 0
        loss_prob = loss_count / total_trades if total_trades > 0 else 0

        if avg_loss > 0 and avg_win > 0:
            kelly_criterion = (win_prob * avg_win - loss_prob * avg_loss) / avg_win
        else:
            kelly_criterion = 0.0

        if total_trades > 0:
            pnl_mean = df["PNL_Net"].mean()
            pnl_std = df["PNL_Net"].std()
            sqn = (pnl_mean / pnl_std) * np.sqrt(total_trades) if pnl_std > 0 else 0.0
        else:
            sqn = 0.0

        expected_value = (win_prob * avg_win) - (loss_prob * avg_loss)
        risk_reward_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0

        summary = {
            "total_pnl": total_pnl,
            "total_fees": total_fees,
            "win_rate": win_rate,
            "win_count": win_count,
            "loss_count": loss_count,
            "total_trades": total_trades,
            "equity_curve": equity_curve,
            "current_streak": current_streak,
            "best_win_streak": best_win_streak,
            "worst_loss_streak": worst_loss_streak,
            "max_single_loss": max_single_loss,
            "max_drawdown": max_single_loss,
            "profit_factor": profit_factor,
            "kelly_criterion": kelly_criterion,
            "sqn": float(sqn),
            "expected_value": expected_value,
            "risk_reward_ratio": risk_reward_ratio,
        }
        self._save_trade_summary(summary)
        return summary

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

    def save_balance_history(self, balance: float, wallet_balance: float = 0.0):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO balance_history (timestamp, balance, wallet_balance) VALUES (?, ?, ?)",
            (datetime.utcnow(), balance, wallet_balance),
        )
        conn.commit()
        conn.close()

    def save_ws_event(self, event_type: str, event_time: int, payload):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO ws_events (event_type, event_time, payload) VALUES (?, ?, ?)",
            (str(event_type), int(event_time), json.dumps(payload, ensure_ascii=False)),
        )
        conn.commit()
        conn.close()

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

    def set_monthly_target(self, target: float):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE user_settings
            SET monthly_target = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = 1
            """,
            (float(target),),
        )
        conn.commit()
        conn.close()

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

    def _save_trade_summary(self, summary):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        equity_curve = summary.get("equity_curve", [])
        equity_curve_json = json.dumps(equity_curve, ensure_ascii=False)
        cursor.execute(
            """
            INSERT INTO trade_summary (
                id, total_pnl, total_fees, win_rate, win_count, loss_count,
                total_trades, equity_curve, current_streak, best_win_streak,
                worst_loss_streak, max_drawdown, profit_factor, kelly_criterion,
                sqn, expected_value, risk_reward_ratio, updated_at
            ) VALUES (
                1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
            )
            ON CONFLICT(id) DO UPDATE SET
                total_pnl = excluded.total_pnl,
                total_fees = excluded.total_fees,
                win_rate = excluded.win_rate,
                win_count = excluded.win_count,
                loss_count = excluded.loss_count,
                total_trades = excluded.total_trades,
                equity_curve = excluded.equity_curve,
                current_streak = excluded.current_streak,
                best_win_streak = excluded.best_win_streak,
                worst_loss_streak = excluded.worst_loss_streak,
                max_drawdown = excluded.max_drawdown,
                profit_factor = excluded.profit_factor,
                kelly_criterion = excluded.kelly_criterion,
                sqn = excluded.sqn,
                expected_value = excluded.expected_value,
                risk_reward_ratio = excluded.risk_reward_ratio,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                float(summary.get("total_pnl", 0.0)),
                float(summary.get("total_fees", 0.0)),
                float(summary.get("win_rate", 0.0)),
                int(summary.get("win_count", 0)),
                int(summary.get("loss_count", 0)),
                int(summary.get("total_trades", 0)),
                equity_curve_json,
                int(summary.get("current_streak", 0)),
                int(summary.get("best_win_streak", 0)),
                int(summary.get("worst_loss_streak", 0)),
                float(summary.get("max_single_loss", summary.get("max_drawdown", 0.0))),
                float(summary.get("profit_factor", 0.0)),
                float(summary.get("kelly_criterion", 0.0)),
                float(summary.get("sqn", 0.0)),
                float(summary.get("expected_value", 0.0)),
                float(summary.get("risk_reward_ratio", 0.0)),
            ),
        )
        conn.commit()
        conn.close()
