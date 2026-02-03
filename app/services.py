"""
Services module - 从数据库读取交易数据
"""
from app.database import Database
from app.models import Trade, TradeSummary
from typing import List
from datetime import datetime
import numpy as np


class BinanceOrderAnalyzer:
    """交易数据服务层 - 从数据库读取"""

    def __init__(self, api_key: str = None, api_secret: str = None):
        # API密钥用于定时任务，这里只负责读取数据库
        self.db = Database()

    def get_summary(self) -> TradeSummary:
        """获取交易汇总数据和统计指标"""
        df = self.db.get_all_trades()

        if df.empty:
            return TradeSummary(
                total_pnl=0.0,
                total_fees=0.0,
                win_rate=0.0,
                win_count=0,
                loss_count=0,
                total_trades=0,
                equity_curve=[],
                current_streak=0,
                max_drawdown=0.0,
                profit_factor=0.0,
                kelly_criterion=0.0,
                sqn=0.0,
                expected_value=0.0,
                risk_reward_ratio=0.0
            )

        # 基础指标
        total_pnl = float(df['PNL_Net'].sum())
        total_fees = float(df['Fees'].sum())
        win_count = len(df[df['PNL_Net'] > 0])
        loss_count = len(df[df['PNL_Net'] < 0])
        total_trades = len(df)
        win_rate = (win_count / total_trades * 100) if total_trades > 0 else 0.0

        # 权益曲线
        equity_curve = df['PNL_Net'].cumsum().tolist()

        # 当前连胜/连败
        current_streak = 0
        for pnl in reversed(df['PNL_Net'].values):
            if (current_streak >= 0 and pnl > 0) or (current_streak < 0 and pnl < 0):
                current_streak += 1 if pnl > 0 else -1
            else:
                break

        # 最大回撤
        cumulative = df['PNL_Net'].cumsum()
        running_max = cumulative.expanding().max()
        drawdown = cumulative - running_max
        max_drawdown = float(drawdown.min()) if len(drawdown) > 0 else 0.0

        # 盈亏比
        total_wins = float(df[df['PNL_Net'] > 0]['PNL_Net'].sum())
        total_losses = abs(float(df[df['PNL_Net'] < 0]['PNL_Net'].sum()))
        profit_factor = (total_wins / total_losses) if total_losses > 0 else 0.0

        # 凯利公式
        avg_win = total_wins / win_count if win_count > 0 else 0
        avg_loss = total_losses / loss_count if loss_count > 0 else 0
        win_prob = win_count / total_trades if total_trades > 0 else 0
        loss_prob = loss_count / total_trades if total_trades > 0 else 0

        if avg_loss > 0:
            kelly_criterion = (win_prob * avg_win - loss_prob * avg_loss) / avg_win
        else:
            kelly_criterion = 0.0

        # 系统质量数 (SQN)
        if total_trades > 0:
            pnl_mean = df['PNL_Net'].mean()
            pnl_std = df['PNL_Net'].std()
            sqn = (pnl_mean / pnl_std) * np.sqrt(total_trades) if pnl_std > 0 else 0.0
        else:
            sqn = 0.0

        # 期望值
        expected_value = (win_prob * avg_win) - (loss_prob * avg_loss)

        # 风险回报比
        risk_reward_ratio = (avg_win / avg_loss) if avg_loss > 0 else 0.0

        return TradeSummary(
            total_pnl=total_pnl,
            total_fees=total_fees,
            win_rate=win_rate,
            win_count=win_count,
            loss_count=loss_count,
            total_trades=total_trades,
            equity_curve=equity_curve,
            current_streak=current_streak,
            max_drawdown=max_drawdown,
            profit_factor=profit_factor,
            kelly_criterion=kelly_criterion,
            sqn=float(sqn),
            expected_value=expected_value,
            risk_reward_ratio=risk_reward_ratio
        )

    def get_trades_list(self) -> List[Trade]:
        """获取交易记录列表"""
        df = self.db.get_all_trades()

        if df.empty:
            return []

        trades = []
        for _, row in df.iterrows():
            # Calculate duration in minutes
            try:
                entry_dt = datetime.strptime(str(row['Entry_Time']), "%Y-%m-%d %H:%M:%S")
                exit_dt = datetime.strptime(str(row['Exit_Time']), "%Y-%m-%d %H:%M:%S")
                duration_min = (exit_dt - entry_dt).total_seconds() / 60.0
            except Exception:
                duration_min = 0.0

            trade = Trade(
                no=int(row['No']),
                date=str(row['Date']),
                entry_time=str(row['Entry_Time']),
                exit_time=str(row['Exit_Time']),
                holding_time=str(row['Holding_Time']),
                duration_minutes=float(duration_min),
                symbol=str(row['Symbol']),
                side=str(row['Side']),
                price_change_pct=float(row['Price_Change_Pct']),
                entry_amount=float(row['Entry_Amount']),
                entry_price=float(row['Entry_Price']),
                exit_price=float(row['Exit_Price']),
                qty=float(row['Qty']),
                fees=float(row['Fees']),
                pnl_net=float(row['PNL_Net']),
                close_type=str(row['Close_Type']),
                return_rate=str(row['Return_Rate']),
                open_price=float(row['Open_Price']),
                pnl_before_fees=float(row['PNL_Before_Fees']),
                entry_order_id=int(row['Entry_Order_ID']),
                exit_order_id=str(row['Exit_Order_ID'])
            )
            trades.append(trade)

        return trades
