"""
定时任务调度器 - 自动更新交易数据
"""
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
from dotenv import load_dotenv
import pandas as pd

from app.trade_processor import TradeDataProcessor
from app.database import Database
from app.jobs.alert_jobs import run_profit_alert_check, run_reentry_alert_check
from app.jobs.noon_loss_job import run_noon_loss_check, run_noon_loss_review
from app.jobs.risk_jobs import run_long_held_positions_check, run_sleep_risk_check
from app.jobs.sync_jobs import run_sync_open_positions, run_sync_trades_incremental
from app.jobs.sync_trades_job import run_sync_trades_data_impl
from app.jobs.trades_compensation_jobs import (
    request_trades_compensation_job,
    run_pending_trades_compensation_job,
    sync_trades_compensation_job,
)
from app.jobs.market_snapshot_jobs import (
    build_rebound_snapshot_job,
    build_top_gainers_snapshot_job,
    get_rebound_snapshot_job,
    get_top_gainers_snapshot_job,
    send_morning_top_gainers_job,
    snapshot_morning_rebound_job,
)
from app.jobs.sync_pipeline_jobs import (
    fetch_and_analyze_closed_trades,
    persist_closed_trades_and_watermarks,
    resolve_sync_window,
)
from app.jobs.balance_sync_job import run_balance_sync_job
from app.jobs.scheduler_startup_jobs import register_scheduler_jobs
from app.core.job_runtime import JobRuntimeController
from app.core.metrics import log_job_metric, measure_ms
from app.core.scheduler_config import load_scheduler_config
from app.core.scheduler_runtime import get_scheduler_singleton, should_start_scheduler_runtime
from app.core.symbols import normalize_futures_symbol
from app.logger import logger
from app.repositories import RiskRepository, SnapshotRepository, SyncRepository, TradeRepository
from app.services.market_price_service import MarketPriceService

load_dotenv()

# 定义UTC+8时区
UTC8 = ZoneInfo("Asia/Shanghai")


class TradeDataScheduler:
    """交易数据定时更新调度器"""

    _CONFIG_FIELDS = (
        "days_to_fetch",
        "update_interval_minutes",
        "trades_incremental_fallback_interval_minutes",
        "open_positions_update_interval_minutes",
        "start_date",
        "end_date",
        "sync_lookback_minutes",
        "symbol_sync_overlap_minutes",
        "open_positions_lookback_days",
        "enable_daily_open_positions_full_sync",
        "open_positions_full_lookback_days",
        "open_positions_full_sync_hour",
        "open_positions_full_sync_minute",
        "enable_daily_full_sync",
        "daily_full_sync_hour",
        "daily_full_sync_minute",
        "use_time_filter",
        "enable_user_stream",
        "force_full_sync",
        "enable_leaderboard_alert",
        "leaderboard_top_n",
        "leaderboard_min_quote_volume",
        "leaderboard_max_symbols",
        "leaderboard_kline_workers",
        "leaderboard_weight_budget_per_minute",
        "leaderboard_alert_hour",
        "leaderboard_alert_minute",
        "leaderboard_guard_before_minutes",
        "leaderboard_guard_after_minutes",
        "enable_rebound_7d_snapshot",
        "rebound_7d_top_n",
        "rebound_7d_kline_workers",
        "rebound_7d_weight_budget_per_minute",
        "rebound_7d_hour",
        "rebound_7d_minute",
        "enable_rebound_30d_snapshot",
        "rebound_30d_top_n",
        "rebound_30d_kline_workers",
        "rebound_30d_weight_budget_per_minute",
        "rebound_30d_hour",
        "rebound_30d_minute",
        "enable_rebound_60d_snapshot",
        "rebound_60d_top_n",
        "rebound_60d_kline_workers",
        "rebound_60d_weight_budget_per_minute",
        "rebound_60d_hour",
        "rebound_60d_minute",
        "noon_loss_check_hour",
        "noon_loss_check_minute",
        "noon_review_hour",
        "noon_review_minute",
        "noon_review_target_day_offset",
        "enable_profit_alert",
        "enable_reentry_alert",
        "profit_alert_threshold_pct",
        "api_job_lock_wait_seconds",
        "enable_triggered_trades_compensation",
        "trades_compensation_lookback_minutes",
    )

    def __init__(self):
        config = load_scheduler_config()
        self.config = config
        scheduler_tz = config.scheduler_timezone
        try:
            self.scheduler = BackgroundScheduler(timezone=ZoneInfo(scheduler_tz))
        except Exception as exc:
            logger.warning(f"无效的调度器时区 {scheduler_tz}: {exc}，使用默认时区")
            self.scheduler = BackgroundScheduler()
        self.db = Database()
        self.sync_repo = SyncRepository(self.db)
        self.risk_repo = RiskRepository(self.db)
        self.snapshot_repo = SnapshotRepository(self.db)
        self.trade_repo = TradeRepository(self.db)

        # 从环境变量获取配置
        api_key = os.getenv('BINANCE_API_KEY')
        api_secret = os.getenv('BINANCE_API_SECRET')

        if not api_key or not api_secret:
            logger.warning("未配置Binance API密钥，定时任务将无法运行")
            self.processor = None
        else:
            self.processor = TradeDataProcessor(api_key, api_secret)

        self._apply_scheduler_config(config)
        self.runtime_controller = JobRuntimeController(lock_wait_seconds=self.api_job_lock_wait_seconds)
        self._pending_compensation_since_ms: dict[str, int] = {}

    def _apply_scheduler_config(self, config):
        for field in self._CONFIG_FIELDS:
            setattr(self, field, getattr(config, field))

    def _is_api_cooldown_active(self, source: str) -> bool:
        return self.runtime_controller.is_cooldown_active(source=source)

    def _try_enter_api_job_slot(self, source: str) -> bool:
        return self.runtime_controller.try_acquire(source=source)

    def _release_api_job_slot(self):
        self.runtime_controller.release()

    def _format_ms_to_utc8(self, ts_ms: int) -> str:
        """将毫秒时间戳格式化为 UTC+8 可读时间。"""
        try:
            dt = datetime.fromtimestamp(int(ts_ms) / 1000, tz=UTC8)
            return dt.strftime("%Y-%m-%d %H:%M:%S%z")
        except Exception:
            return str(ts_ms)

    def _format_window_with_ms(self, start_ms: int, end_ms: int) -> str:
        """输出窗口的可读时间和原始毫秒，便于日志排查。"""
        start_text = self._format_ms_to_utc8(start_ms)
        end_text = self._format_ms_to_utc8(end_ms)
        return f"[{start_text} ~ {end_text}] ({start_ms} ~ {end_ms})"

    def _is_leaderboard_guard_window(self) -> bool:
        """
        在晨间涨幅榜前后短时间窗口内跳过交易同步，避免 API 权重叠加。
        默认窗口: 榜单前2分钟到后5分钟。
        """
        if not self.enable_leaderboard_alert:
            return False

        now = datetime.now(UTC8)
        leaderboard_dt = now.replace(
            hour=self.leaderboard_alert_hour,
            minute=self.leaderboard_alert_minute,
            second=0,
            microsecond=0
        )
        window_start = leaderboard_dt - timedelta(minutes=self.leaderboard_guard_before_minutes)
        window_end = leaderboard_dt + timedelta(minutes=self.leaderboard_guard_after_minutes)
        return window_start <= now <= window_end

    def sync_trades_data(self, force_full: bool = False, emit_metric: bool = True):
        """同步交易数据到数据库"""
        if not emit_metric:
            return self._sync_trades_data_impl(force_full=force_full)

        job_status = "success"
        with measure_ms("scheduler.sync_trades_data", mode="full" if force_full else "incremental") as metric:
            try:
                ok = self._sync_trades_data_impl(force_full=force_full)
                if ok is False:
                    job_status = "error"
            except Exception:
                job_status = "error"
                raise
            finally:
                log_job_metric(job_name="sync_trades_data", status=job_status, snapshot=metric)
        return job_status == "success"

    def _sync_trades_data_impl(self, force_full: bool = False):
        return run_sync_trades_data_impl(self, force_full=force_full)

    def _resolve_sync_window(self, *, force_full: bool, last_entry_time: str | None) -> tuple[int, int, bool]:
        return resolve_sync_window(self, force_full=force_full, last_entry_time=last_entry_time, utc8=UTC8)

    def _fetch_and_analyze_closed_trades(
        self,
        *,
        since: int,
        until: int,
        is_full_sync_run: bool,
    ):
        return fetch_and_analyze_closed_trades(
            self,
            since=since,
            until=until,
            is_full_sync_run=is_full_sync_run,
        )

    def _persist_closed_trades_and_watermarks(
        self,
        *,
        df: pd.DataFrame,
        force_full: bool,
        success_symbols: list[str],
        failure_symbols: dict[str, str],
        until: int,
    ) -> tuple[float, int]:
        return persist_closed_trades_and_watermarks(
            self,
            df=df,
            force_full=force_full,
            success_symbols=success_symbols,
            failure_symbols=failure_symbols,
            until=until,
        )

    def sync_open_positions_data(self):
        status = "success"
        with measure_ms("scheduler.sync_open_positions_data") as metric:
            try:
                return run_sync_open_positions(self)
            except Exception:
                status = "error"
                raise
            finally:
                log_job_metric(job_name="sync_open_positions_data", status=status, snapshot=metric)

    def sync_open_positions_full_window(self):
        status = "success"
        with measure_ms("scheduler.sync_open_positions_full_window") as metric:
            try:
                return run_sync_open_positions(
                    self,
                    lookback_days=self.open_positions_full_lookback_days,
                    mode="full",
                )
            except Exception:
                status = "error"
                raise
            finally:
                log_job_metric(job_name="sync_open_positions_full_window", status=status, snapshot=metric)

    def sync_trades_incremental(self):
        """增量同步交易数据"""
        status = "success"
        with measure_ms("scheduler.sync_trades_incremental") as metric:
            try:
                ok = self.sync_trades_data(force_full=False, emit_metric=False)
                if ok is False:
                    status = "error"
            except Exception:
                status = "error"
                raise
            finally:
                log_job_metric(job_name="sync_trades_incremental", status=status, snapshot=metric)
        return status == "success"

    def request_trades_compensation(
        self,
        symbols: list[str],
        *,
        reason: str = "open_positions_change",
        symbol_since_ms: dict[str, int] | None = None,
    ):
        return request_trades_compensation_job(
            self,
            symbols,
            reason=reason,
            symbol_since_ms=symbol_since_ms,
        )

    def _run_pending_trades_compensation(self):
        return run_pending_trades_compensation_job(self)

    def sync_trades_compensation(
        self,
        *,
        symbols: list[str],
        reason: str = "triggered",
        symbol_since_ms: dict[str, int] | None = None,
    ):
        return sync_trades_compensation_job(
            self,
            symbols=symbols,
            reason=reason,
            symbol_since_ms=symbol_since_ms,
        )

    def sync_trades_full(self):
        """全量同步交易数据"""
        status = "success"
        with measure_ms("scheduler.sync_trades_full") as metric:
            try:
                ok = self.sync_trades_data(force_full=True, emit_metric=False)
                if ok is False:
                    status = "error"
            except Exception:
                status = "error"
                raise
            finally:
                log_job_metric(job_name="sync_trades_full", status=status, snapshot=metric)
        return status == "success"

    def _get_mark_price_map(self, symbols: list[str]) -> dict[str, float]:
        """批量获取标记价格（优先 premiumIndex，其次 ticker/price）。"""
        if not symbols or not self.processor:
            return {}
        return MarketPriceService.get_mark_price_map(symbols, self.processor.client)

    @staticmethod
    def _parse_entry_time_utc8(entry_time_value) -> datetime | None:
        """解析数据库中的 entry_time（当前按 UTC+8 存储）为 timezone-aware datetime。"""
        if not entry_time_value:
            return None
        try:
            return datetime.strptime(str(entry_time_value), '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC8)
        except ValueError:
            return None

    def check_same_symbol_reentry_alert(self):
        return run_reentry_alert_check(self)

    def check_open_positions_profit_alert(self, threshold_pct: float):
        return run_profit_alert_check(self, threshold_pct)

    def check_long_held_positions(self):
        return run_long_held_positions_check(self)

    def check_risk_before_sleep(self):
        return run_sleep_risk_check(self)

    def check_recent_losses_at_noon(self):
        return run_noon_loss_check(self)

    def review_noon_loss_at_night(
        self,
        snapshot_date: str | None = None,
        send_notification: bool = True
    ):
        return run_noon_loss_review(
            self,
            snapshot_date=snapshot_date,
            send_notification=send_notification,
        )

    def backfill_noon_loss_review(self, snapshot_date: str, send_notification: bool = False):
        """手动回填指定日期的午间止损复盘结果。"""
        logger.info(
            f"开始手动回填午间止损复盘: snapshot_date={snapshot_date}, "
            f"send_notification={send_notification}"
        )
        self.review_noon_loss_at_night(snapshot_date=snapshot_date, send_notification=send_notification)

    def _build_top_gainers_snapshot(self):
        return build_top_gainers_snapshot_job(self, UTC8)

    def get_top_gainers_snapshot(self, source: str = "涨幅榜接口"):
        """获取涨幅榜快照（带冷却与互斥保护），供API或任务复用。"""
        return get_top_gainers_snapshot_job(self, source=source, utc8=UTC8)

    def send_morning_top_gainers(self):
        """每天早上发送币安合约涨跌幅榜（按UTC当日开盘到当前涨跌幅）"""
        return send_morning_top_gainers_job(
            self,
            source="晨间涨幅榜",
            schedule_hour=self.leaderboard_alert_hour,
            schedule_minute=self.leaderboard_alert_minute,
            utc8=UTC8,
        )

    def _build_rebound_snapshot(
        self,
        *,
        window_days: int,
        top_n: int,
        kline_workers: int,
        weight_budget_per_minute: int,
        label: str
    ):
        return build_rebound_snapshot_job(
            self,
            utc8=UTC8,
            window_days=window_days,
            top_n=top_n,
            kline_workers=kline_workers,
            weight_budget_per_minute=weight_budget_per_minute,
            label=label,
        )

    def _build_rebound_7d_snapshot(self):
        """构建14D反弹幅度榜快照（兼容历史函数名）。"""
        return self._build_rebound_snapshot(
            window_days=14,
            top_n=self.rebound_7d_top_n,
            kline_workers=self.rebound_7d_kline_workers,
            weight_budget_per_minute=self.rebound_7d_weight_budget_per_minute,
            label="14D反弹榜"
        )

    def _build_rebound_30d_snapshot(self):
        """构建30D反弹幅度榜快照。"""
        return self._build_rebound_snapshot(
            window_days=30,
            top_n=self.rebound_30d_top_n,
            kline_workers=self.rebound_30d_kline_workers,
            weight_budget_per_minute=self.rebound_30d_weight_budget_per_minute,
            label="30D反弹榜"
        )

    def _build_rebound_60d_snapshot(self):
        """构建60D反弹幅度榜快照。"""
        return self._build_rebound_snapshot(
            window_days=60,
            top_n=self.rebound_60d_top_n,
            kline_workers=self.rebound_60d_kline_workers,
            weight_budget_per_minute=self.rebound_60d_weight_budget_per_minute,
            label="60D反弹榜"
        )

    def get_rebound_7d_snapshot(self, source: str = "14D反弹榜接口"):
        """获取14D反弹榜快照（带冷却与互斥保护），供API或任务复用。"""
        return get_rebound_snapshot_job(self, source=source, build_snapshot=self._build_rebound_7d_snapshot)

    def snapshot_morning_rebound_7d(self):
        """每天早上07:30生成14D反弹幅度Top榜快照并入库。"""
        return snapshot_morning_rebound_job(
            self,
            source="晨间14D反弹榜",
            label="14D反弹榜",
            schedule_hour=self.rebound_7d_hour,
            schedule_minute=self.rebound_7d_minute,
            get_snapshot=self.get_rebound_7d_snapshot,
            save_snapshot=self.snapshot_repo.save_rebound_7d_snapshot,
        )

    def get_rebound_30d_snapshot(self, source: str = "30D反弹榜接口"):
        """获取30D反弹榜快照（带冷却与互斥保护），供API或任务复用。"""
        return get_rebound_snapshot_job(self, source=source, build_snapshot=self._build_rebound_30d_snapshot)

    def snapshot_morning_rebound_30d(self):
        """每天早上生成30D反弹幅度Top榜快照并入库。"""
        return snapshot_morning_rebound_job(
            self,
            source="晨间30D反弹榜",
            label="30D反弹榜",
            schedule_hour=self.rebound_30d_hour,
            schedule_minute=self.rebound_30d_minute,
            get_snapshot=self.get_rebound_30d_snapshot,
            save_snapshot=self.snapshot_repo.save_rebound_30d_snapshot,
        )

    def get_rebound_60d_snapshot(self, source: str = "60D反弹榜接口"):
        """获取60D反弹榜快照（带冷却与互斥保护），供API或任务复用。"""
        return get_rebound_snapshot_job(self, source=source, build_snapshot=self._build_rebound_60d_snapshot)

    def snapshot_morning_rebound_60d(self):
        """每天早上生成60D反弹幅度Top榜快照并入库。"""
        return snapshot_morning_rebound_job(
            self,
            source="晨间60D反弹榜",
            label="60D反弹榜",
            schedule_hour=self.rebound_60d_hour,
            schedule_minute=self.rebound_60d_minute,
            get_snapshot=self.get_rebound_60d_snapshot,
            save_snapshot=self.snapshot_repo.save_rebound_60d_snapshot,
        )

    @staticmethod
    def _normalize_futures_symbol(symbol: str) -> str:
        """将库内symbol规范化为Binance USDT交易对symbol"""
        return normalize_futures_symbol(symbol, preserve_busd=True)

    def sync_balance_data(self):
        """同步账户余额数据到数据库"""
        with measure_ms("scheduler.sync_balance_data") as metric:
            status = "success"
            try:
                status = run_balance_sync_job(self)
            finally:
                log_job_metric(job_name="sync_balance_data", status=status, snapshot=metric)

    def start(self):
        """启动定时任务"""
        if not self.processor:
            logger.warning("定时任务未启动: API密钥未配置")
            return
        register_scheduler_jobs(self, utc8=UTC8)

    def stop(self):
        """停止定时任务"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("定时任务已停止")

    def get_next_run_time(self):
        """获取下次运行时间"""
        job = self.scheduler.get_job('sync_trades_incremental')
        if not job:
            job = self.scheduler.get_job('sync_trades_full_daily')
        if job:
            return job.next_run_time
        return None


def should_start_scheduler() -> tuple[bool, str]:
    return should_start_scheduler_runtime()


def get_scheduler() -> TradeDataScheduler:
    """获取调度器单例"""
    return get_scheduler_singleton(TradeDataScheduler)
