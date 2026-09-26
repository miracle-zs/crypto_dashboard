import json
from datetime import datetime


class TradeWriteRepository:
    def __init__(self, db):
        self.db = db

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

    def save_trade_summary(self, summary):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        equity_curve = summary.get("equity_curve", [])
        equity_curve_json = json.dumps(equity_curve, ensure_ascii=False)
        cursor.execute(
            """
            INSERT INTO trade_summary (
                id, total_pnl, total_fees, win_rate, win_count, loss_count,
                total_trades, equity_curve, current_streak, best_win_streak,
                worst_loss_streak, max_single_loss, max_drawdown, profit_factor, kelly_criterion,
                sqn, expected_value, risk_reward_ratio, updated_at
            ) VALUES (
                1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP
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
                max_single_loss = excluded.max_single_loss,
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
                float(summary.get("max_single_loss", 0.0)),
                float(summary.get("max_drawdown", summary.get("max_single_loss", 0.0))),
                float(summary.get("profit_factor", 0.0)),
                float(summary.get("kelly_criterion", 0.0)),
                float(summary.get("sqn", 0.0)),
                float(summary.get("expected_value", 0.0)),
                float(summary.get("risk_reward_ratio", 0.0)),
            ),
        )
        conn.commit()
        conn.close()

    def prune_balance_history(
        self,
        days_to_keep_raw: int = 7,
        days_to_keep_hourly: int = 90,
        days_to_keep_daily: int = 365,
    ) -> int:
        """
        Prune balance_history downsampling old points to prevent unlimited DB bloat:
        - Points older than days_to_keep_daily: completely deleted.
        - Points between days_to_keep_hourly and days_to_keep_daily: kept at 1 point per day.
        - Points between days_to_keep_raw and days_to_keep_hourly: kept at 1 point per hour.
        - Points within days_to_keep_raw: preserved completely at raw 30s cadence.
        """
        conn = self.db._get_connection()
        try:
            cursor = conn.cursor()
            before_changes = conn.total_changes

            # 1. Beyond daily retention limit (e.g. 365 days): delete
            cursor.execute(
                "DELETE FROM balance_history WHERE timestamp < datetime('now', ?)",
                (f"-{int(days_to_keep_daily)} days",),
            )

            # 2. Between hourly and daily (e.g. 90d to 365d): keep 1 per day
            cursor.execute(
                f"""
                DELETE FROM balance_history
                WHERE timestamp < datetime('now', ?)
                  AND timestamp >= datetime('now', ?)
                  AND id NOT IN (
                      SELECT min(id)
                      FROM balance_history
                      WHERE timestamp < datetime('now', ?)
                        AND timestamp >= datetime('now', ?)
                      GROUP BY strftime('%Y-%m-%d', timestamp)
                  )
                """,
                (
                    f"-{int(days_to_keep_hourly)} days",
                    f"-{int(days_to_keep_daily)} days",
                    f"-{int(days_to_keep_hourly)} days",
                    f"-{int(days_to_keep_daily)} days",
                ),
            )

            # 3. Between raw and hourly (e.g. 7d to 90d): keep 1 per hour
            cursor.execute(
                f"""
                DELETE FROM balance_history
                WHERE timestamp < datetime('now', ?)
                  AND timestamp >= datetime('now', ?)
                  AND id NOT IN (
                      SELECT min(id)
                      FROM balance_history
                      WHERE timestamp < datetime('now', ?)
                        AND timestamp >= datetime('now', ?)
                      GROUP BY strftime('%Y-%m-%d %H', timestamp)
                  )
                """,
                (
                    f"-{int(days_to_keep_raw)} days",
                    f"-{int(days_to_keep_hourly)} days",
                    f"-{int(days_to_keep_raw)} days",
                    f"-{int(days_to_keep_hourly)} days",
                ),
            )

            total_deleted = conn.total_changes - before_changes
            conn.commit()
            return total_deleted
        finally:
            conn.close()
