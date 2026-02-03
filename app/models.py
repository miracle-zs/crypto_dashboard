from pydantic import BaseModel
from typing import List, Optional, Any

class Trade(BaseModel):
    no: int
    date: str
    entry_time: str
    exit_time: str
    holding_time: str
    symbol: str
    side: str
    price_change_pct: float
    entry_amount: float
    entry_price: float
    exit_price: float
    qty: float
    fees: float
    pnl_net: float
    close_type: str
    return_rate: str
    open_price: float
    pnl_before_fees: float
    entry_order_id: int
    exit_order_id: str

class TradeSummary(BaseModel):
    total_pnl: float
    total_fees: float
    win_rate: float
    win_count: int
    loss_count: int
    total_trades: int
    equity_curve: List[float]
    current_streak: int
    max_drawdown: float
    profit_factor: float
    kelly_criterion: float
    sqn: float
    expected_value: float
    risk_reward_ratio: float

class BalanceHistoryItem(BaseModel):
    time: int
    value: float
