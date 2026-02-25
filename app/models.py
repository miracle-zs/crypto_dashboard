from pydantic import BaseModel, Field
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
    max_single_loss: Optional[float] = None
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
    transfer_amount: Optional[float] = None
    transfer_count: Optional[int] = None


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
    recent_loss_total_if_stop_now: float = 0.0
    recent_loss_pct_of_balance: float = 0.0
    recent_loss_snapshot_date: Optional[str] = None
    recent_loss_snapshot_time: Optional[str] = None
    recent_loss_snapshot_ready: bool = False
    noon_review_snapshot_date: Optional[str] = None
    noon_review_snapshot_time: Optional[str] = None
    noon_review_snapshot_ready: bool = False
    noon_review_not_cut_count: int = 0
    noon_review_noon_cut_loss_total: float = 0.0
    noon_review_hold_loss_total: float = 0.0
    noon_review_delta_loss_total: float = 0.0
    noon_review_pct_of_balance: float = 0.0
    profit_alert_threshold_pct: float = 20.0


class OpenPositionsResponse(BaseModel):
    as_of: Optional[str] = None
    positions: List[OpenPositionItem] = Field(default_factory=list)
    summary: Optional[OpenPositionsSummary] = None
    version: Optional[int] = None
    incremental: Optional[bool] = None
    changed: Optional[bool] = None


class TradeDurationBucket(BaseModel):
    label: str
    trade_count: int
    win_pnl: float
    loss_pnl: float


class TradeDurationPoint(BaseModel):
    x: float
    y: float
    symbol: str
    time: str


class SymbolRankItem(BaseModel):
    symbol: str
    pnl: float
    trade_count: int
    win_rate: float
    share: float


class SymbolRankData(BaseModel):
    winners: List[SymbolRankItem] = Field(default_factory=list)
    losers: List[SymbolRankItem] = Field(default_factory=list)


class TradeAggregatesResponse(BaseModel):
    duration_buckets: List[TradeDurationBucket] = Field(default_factory=list)
    duration_points: List[TradeDurationPoint] = Field(default_factory=list)
    hourly_pnl: List[float] = Field(default_factory=list)
    symbol_rank: SymbolRankData


class LogsResponse(BaseModel):
    logs: List[str] = Field(default_factory=list)


class NoonLossReviewSummary(BaseModel):
    reviewed_count_all: int = 0
    delta_sum_all: float = 0.0


class NoonLossReviewRow(BaseModel):
    snapshot_date: str
    noon_snapshot_time: Optional[str] = None
    noon_loss_count: int = 0
    noon_cut_loss_total: float = 0.0
    noon_pct_of_balance: float = 0.0
    review_time: Optional[str] = None
    not_cut_count: int = 0
    hold_loss_total: float = 0.0
    delta_loss_total: float = 0.0
    review_pct_of_balance: float = 0.0
    rows: List[Any] = Field(default_factory=list)


class NoonLossReviewHistoryResponse(BaseModel):
    rows: List[NoonLossReviewRow] = Field(default_factory=list)
    summary: NoonLossReviewSummary


class SyncRunItem(BaseModel):
    id: int
    run_type: str
    mode: Optional[str] = None
    status: str
    symbol_count: int = 0
    rows_count: int = 0
    trades_saved: int = 0
    open_saved: int = 0
    elapsed_ms: int = 0
    error_message: Optional[str] = None
    created_at: str


class SyncRunsResponse(BaseModel):
    rows: List[SyncRunItem] = Field(default_factory=list)


class SnapshotDatesResponse(BaseModel):
    dates: List[str] = Field(default_factory=list)


class MetricsHistoryResponse(BaseModel):
    rows: List[dict[str, Any]] = Field(default_factory=list)


class LeaderboardSnapshotResponse(BaseModel):
    ok: bool
    reason: Optional[str] = None
    message: Optional[str] = None
    snapshot_date: Optional[str] = None
    snapshot_time: Optional[str] = None
    rows: List[dict[str, Any]] = Field(default_factory=list)
    losers_rows: List[dict[str, Any]] = Field(default_factory=list)
    gainers_top_count: Optional[int] = None
    losers_top_count: Optional[int] = None


class ReboundSnapshotResponse(BaseModel):
    ok: bool
    reason: Optional[str] = None
    message: Optional[str] = None
    snapshot_date: Optional[str] = None
    snapshot_time: Optional[str] = None
    rows: List[dict[str, Any]] = Field(default_factory=list)
    top_count: Optional[int] = None
