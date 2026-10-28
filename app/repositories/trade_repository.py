import numpy as np

from app.repositories.trade_read_repository import TradeReadRepository
from app.repositories.trade_write_repository import TradeWriteRepository


class TradeRepository(TradeReadRepository, TradeWriteRepository):
    """The public persistence interface for the trade aggregate."""

    def __init__(self, db):
        self.db = db

    def recompute_trade_summary(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT pnl_net, fees
            FROM trades
            ORDER BY exit_time ASC, entry_time ASC
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
            self.save_trade_summary(summary)
            return summary

        pnl_values = np.array([float(row["pnl_net"] or 0.0) for row in rows], dtype=float)
        fees_values = np.array([float(row["fees"] or 0.0) for row in rows], dtype=float)
        total_pnl = float(pnl_values.sum())
        total_fees = float(fees_values.sum())
        win_count = int(np.sum(pnl_values > 0))
        loss_count = int(np.sum(pnl_values < 0))
        total_trades = int(len(pnl_values))
        win_rate = (win_count / total_trades * 100) if total_trades else 0.0
        equity_curve = np.cumsum(pnl_values).tolist()

        current_streak = 0
        last_pnl = pnl_values[-1]
        for pnl in reversed(pnl_values):
            if pnl == 0:
                break
            if (last_pnl > 0 and pnl > 0) or (last_pnl < 0 and pnl < 0):
                current_streak += 1 if last_pnl > 0 else -1
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
            best_win_streak = max(best_win_streak, streak)
            worst_loss_streak = min(worst_loss_streak, streak)

        max_single_loss = float(np.min(pnl_values))
        equity_values = np.asarray(equity_curve, dtype=float)
        running_peak = np.maximum.accumulate(np.concatenate(([0.0], equity_values)))[1:]
        max_drawdown = float(np.min(equity_values - running_peak))
        total_wins = float(np.sum(pnl_values[pnl_values > 0]))
        total_losses = abs(float(np.sum(pnl_values[pnl_values < 0])))
        profit_factor = total_wins / total_losses if total_losses else 0.0
        avg_win = total_wins / win_count if win_count else 0.0
        avg_loss = total_losses / loss_count if loss_count else 0.0
        win_prob = win_count / total_trades if total_trades else 0.0
        loss_prob = loss_count / total_trades if total_trades else 0.0
        kelly_criterion = (
            (win_prob * avg_win - loss_prob * avg_loss) / avg_win
            if avg_loss > 0 and avg_win > 0
            else 0.0
        )
        pnl_mean = float(np.mean(pnl_values)) if total_trades else 0.0
        pnl_std = float(np.std(pnl_values, ddof=1)) if total_trades > 1 else 0.0
        sqn = (pnl_mean / pnl_std) * np.sqrt(total_trades) if pnl_std > 0 else 0.0

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
            "max_drawdown": max_drawdown,
            "profit_factor": profit_factor,
            "kelly_criterion": kelly_criterion,
            "sqn": float(sqn),
            "expected_value": (win_prob * avg_win) - (loss_prob * avg_loss),
            "risk_reward_ratio": avg_win / avg_loss if avg_loss > 0 else 0.0,
        }
        self.save_trade_summary(summary)
        return summary
