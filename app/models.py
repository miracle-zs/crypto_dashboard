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
    duration_minutes: float

class TradeSummary(BaseModel):
    total_pnl: float
    total_fees: float
    win_rate: float
    win_count: int
    loss_count: int
    total_trades: int
    equity_curve: List[float]
    current_streak: int
    best_win_streak: int
    worst_loss_streak: int
    max_drawdown: float
    profit_factor: float
    kelly_criterion: float
    sqn: float
    expected_value: float
    risk_reward_ratio: float

class BalanceHistoryItem(BaseModel):
    time: int
    value: float
    cumulative_equity: Optional[float] = None


class DailyStats(BaseModel):
    date: str
    trade_count: int
    total_amount: float
    total_pnl: float
    win_count: int
    loss_count: int
    win_rate: float


class OpenPositionItem(BaseModel):
    symbol: str
    order_id: int
    side: str
    qty: float
    entry_price: float
    mark_price: Optional[float] = None
    entry_time: str
    holding_minutes: int
    holding_time: str
    entry_amount: float
    notional: float
    unrealized_pnl: Optional[float] = None
    unrealized_pnl_pct: Optional[float] = None
    weight: float
    is_long_term: bool = False
    profit_alerted: bool = False


class OpenPositionsSummary(BaseModel):
    total_positions: int
    long_count: int
    short_count: int
    total_notional: float
    long_notional: float
    short_notional: float
    net_exposure: float
    total_unrealized_pnl: float
    avg_holding_minutes: float
    avg_holding_time: str
    concentration_top1: float
    concentration_top3: float
    concentration_hhi: float
    recent_loss_count: int = 0  # New field for risk protocol
    profit_alert_threshold_pct: float = 20.0


class OpenPositionsResponse(BaseModel):
    as_of: str
    positions: List[OpenPositionItem]
    summary: OpenPositionsSummary
