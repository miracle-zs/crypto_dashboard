import numpy as np

from app.repositories.trade_read_repository import TradeReadRepository
from app.repositories.trade_write_repository import TradeWriteRepository


class TradeRepository:
    """Backward-compatible facade for trade read/write repositories."""

    def __init__(self, db):
        self.db = db
        self._read = TradeReadRepository(db)
        self._write = TradeWriteRepository(db)

    def get_trade_summary(self):
        return self._read.get_trade_summary()

    def get_statistics(self):
        return self._read.get_statistics()

    def get_cached_total_trades(self):
        return self._read.get_cached_total_trades()

    def recompute_trade_summary(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT pnl_net, fees
            FROM trades
            ORDER BY entry_time ASC
            """
        )
        rows = cursor.fetchall()
        conn.close()

        if not rows:
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
            self._write.save_trade_summary(summary)
            return summary

        pnl_values = np.array([float(row["pnl_net"] or 0.0) for row in rows], dtype=float)
        fees_values = np.array([float(row["fees"] or 0.0) for row in rows], dtype=float)

        total_pnl = float(pnl_values.sum())
        total_fees = float(fees_values.sum())
        win_count = int(np.sum(pnl_values > 0))
        loss_count = int(np.sum(pnl_values < 0))
        total_trades = int(len(pnl_values))
        win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0.0
        equity_curve = np.cumsum(pnl_values).tolist()

        current_streak = 0
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
        for pnl in pnl_values:
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

        max_single_loss = float(np.min(pnl_values))
        total_wins = float(np.sum(pnl_values[pnl_values > 0]))
        total_losses = abs(float(np.sum(pnl_values[pnl_values < 0])))
        profit_factor = (total_wins / total_losses) if total_losses > 0 else 0.0

        avg_win = total_wins / win_count if win_count > 0 else 0
        avg_loss = total_losses / loss_count if loss_count > 0 else 0
        win_prob = win_count / total_trades if total_trades > 0 else 0
        loss_prob = loss_count / total_trades if total_trades > 0 else 0

        if avg_loss > 0 and avg_win > 0:
            kelly_criterion = (win_prob * avg_win - loss_prob * avg_loss) / avg_win
        else:
            kelly_criterion = 0.0

        pnl_mean = float(np.mean(pnl_values)) if total_trades > 0 else 0.0
        pnl_std = float(np.std(pnl_values, ddof=1)) if total_trades > 1 else 0.0
        sqn = (pnl_mean / pnl_std) * np.sqrt(total_trades) if pnl_std > 0 else 0.0

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
        self._write.save_trade_summary(summary)
        return summary

    def get_all_trades(self, limit: int = None, offset: int = 0):
        return self._read.get_all_trades(limit=limit, offset=offset)

    def get_open_positions(self):
        return self._read.get_open_positions()

    def get_open_position_symbols(self):
        return self._read.get_open_position_symbols()

    def get_balance_history(self, **kwargs):
        return self._read.get_balance_history(**kwargs)

    def get_transfers(self):
        return self._read.get_transfers()

    def get_transfer_timeline(self):
        return self._read.get_transfer_timeline()

    def save_balance_history(self, balance: float, wallet_balance: float = 0.0):
        return self._write.save_balance_history(balance=balance, wallet_balance=wallet_balance)

    def save_ws_event(self, event_type: str, event_time: int, payload):
        return self._write.save_ws_event(event_type=event_type, event_time=event_time, payload=payload)

    def get_daily_stats(self):
        return self._read.get_daily_stats()

    def get_monthly_target(self):
        return self._read.get_monthly_target()

    def set_monthly_target(self, target: float):
        return self._write.set_monthly_target(target=target)

    def get_monthly_pnl(self):
        return self._read.get_monthly_pnl()

    def get_trade_aggregates(self):
        return self._read.get_trade_aggregates()
