"""
定时任务调度器 - 自动更新交易数据
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
import time
from functools import partial
from dotenv import load_dotenv
import pandas as pd

from app.trade_processor import TradeDataProcessor
from app.database import Database
from app.jobs.alert_jobs import run_profit_alert_check, run_reentry_alert_check
from app.jobs.noon_loss_job import run_noon_loss_check, run_noon_loss_review
from app.jobs.risk_jobs import run_long_held_positions_check, run_sleep_risk_check
from app.jobs.sync_jobs import run_sync_open_positions, run_sync_trades_incremental
from app.jobs.market_snapshot_jobs import (
    build_rebound_snapshot_job,
    build_top_gainers_snapshot_job,
    get_rebound_snapshot_job,
    get_top_gainers_snapshot_job,
    send_morning_top_gainers_job,
    snapshot_morning_rebound_job,
)
from app.core.job_runtime import JobRuntimeController
from app.core.metrics import log_job_metric, measure_ms
from app.core.scheduler_config import load_scheduler_config
from app.core.symbols import normalize_futures_symbol
from app.logger import logger
from app.repositories import RiskRepository, SnapshotRepository, SyncRepository, TradeRepository
from app.services.market_price_service import MarketPriceService
from app.services.sync_planning_service import build_symbol_since_map

load_dotenv()

# 定义UTC+8时区
UTC8 = ZoneInfo("Asia/Shanghai")


class TradeDataScheduler:
    """交易数据定时更新调度器"""

    def __init__(self):
        config = load_scheduler_config()
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

        self.days_to_fetch = config.days_to_fetch
        self.update_interval_minutes = config.update_interval_minutes
        self.open_positions_update_interval_minutes = config.open_positions_update_interval_minutes
        self.start_date = config.start_date
        self.end_date = config.end_date
        self.sync_lookback_minutes = config.sync_lookback_minutes
        self.symbol_sync_overlap_minutes = config.symbol_sync_overlap_minutes
        self.open_positions_lookback_days = config.open_positions_lookback_days
        self.enable_daily_full_sync = config.enable_daily_full_sync
        self.daily_full_sync_hour = config.daily_full_sync_hour
        self.daily_full_sync_minute = config.daily_full_sync_minute
        self.use_time_filter = config.use_time_filter
        self.enable_user_stream = config.enable_user_stream
        self.force_full_sync = config.force_full_sync
        self.enable_leaderboard_alert = config.enable_leaderboard_alert
        self.leaderboard_top_n = config.leaderboard_top_n
        self.leaderboard_min_quote_volume = config.leaderboard_min_quote_volume
        self.leaderboard_max_symbols = config.leaderboard_max_symbols
        self.leaderboard_kline_workers = config.leaderboard_kline_workers
        self.leaderboard_weight_budget_per_minute = config.leaderboard_weight_budget_per_minute
        self.leaderboard_alert_hour = config.leaderboard_alert_hour
        self.leaderboard_alert_minute = config.leaderboard_alert_minute
        self.leaderboard_guard_before_minutes = config.leaderboard_guard_before_minutes
        self.leaderboard_guard_after_minutes = config.leaderboard_guard_after_minutes
        self.enable_rebound_7d_snapshot = config.enable_rebound_7d_snapshot
        self.rebound_7d_top_n = config.rebound_7d_top_n
        self.rebound_7d_kline_workers = config.rebound_7d_kline_workers
        self.rebound_7d_weight_budget_per_minute = config.rebound_7d_weight_budget_per_minute
        self.rebound_7d_hour = config.rebound_7d_hour
        self.rebound_7d_minute = config.rebound_7d_minute
        self.enable_rebound_30d_snapshot = config.enable_rebound_30d_snapshot
        self.rebound_30d_top_n = config.rebound_30d_top_n
        self.rebound_30d_kline_workers = config.rebound_30d_kline_workers
        self.rebound_30d_weight_budget_per_minute = config.rebound_30d_weight_budget_per_minute
        self.rebound_30d_hour = config.rebound_30d_hour
        self.rebound_30d_minute = config.rebound_30d_minute
        self.enable_rebound_60d_snapshot = config.enable_rebound_60d_snapshot
        self.rebound_60d_top_n = config.rebound_60d_top_n
        self.rebound_60d_kline_workers = config.rebound_60d_kline_workers
        self.rebound_60d_weight_budget_per_minute = config.rebound_60d_weight_budget_per_minute
        self.rebound_60d_hour = config.rebound_60d_hour
        self.rebound_60d_minute = config.rebound_60d_minute
        self.noon_loss_check_hour = config.noon_loss_check_hour
        self.noon_loss_check_minute = config.noon_loss_check_minute
        self.noon_review_hour = config.noon_review_hour
        self.noon_review_minute = config.noon_review_minute
        self.noon_review_target_day_offset = config.noon_review_target_day_offset
        self.enable_profit_alert = config.enable_profit_alert
        self.enable_reentry_alert = config.enable_reentry_alert
        self.profit_alert_threshold_pct = config.profit_alert_threshold_pct
        self.api_job_lock_wait_seconds = config.api_job_lock_wait_seconds
        self.runtime_controller = JobRuntimeController(lock_wait_seconds=self.api_job_lock_wait_seconds)

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
        """同步交易数据到数据库（实际执行逻辑）"""
        if not self.processor:
            logger.warning("无法同步: API密钥未配置")
            return True
        if self._is_leaderboard_guard_window():
            logger.info(
                "跳过交易同步: 位于晨间涨幅榜保护窗口内 "
                f"({self.leaderboard_alert_hour:02d}:{self.leaderboard_alert_minute:02d} "
                f"前{self.leaderboard_guard_before_minutes}分钟至后{self.leaderboard_guard_after_minutes}分钟)"
            )
            return True
        if self._is_api_cooldown_active(source='交易同步'):
            return True
        if not self._try_enter_api_job_slot(source='交易同步'):
            return True

        sync_started_at = time.perf_counter()
        symbols_elapsed = 0.0
        analyze_elapsed = 0.0
        save_trades_elapsed = 0.0
        open_positions_elapsed = 0.0
        risk_check_elapsed = 0.0
        trades_saved = 0
        open_saved = 0
        symbol_count = 0

        try:
            logger.info("=" * 50)
            run_mode = "全量" if force_full else "增量"
            logger.info(f"开始同步交易数据... mode={run_mode}")

            # 更新同步状态为进行中
            self.sync_repo.update_sync_status(status='syncing')

            last_entry_time = self.sync_repo.get_last_entry_time()
            since, until, is_full_sync_run = self._resolve_sync_window(
                force_full=force_full,
                last_entry_time=last_entry_time,
            )
            (
                df,
                success_symbols,
                failure_symbols,
                symbol_count,
                symbols_elapsed,
                analyze_elapsed,
            ) = self._fetch_and_analyze_closed_trades(
                since=since,
                until=until,
                is_full_sync_run=is_full_sync_run,
            )

            save_trades_elapsed, trades_saved = self._persist_closed_trades_and_watermarks(
                df=df,
                force_full=force_full,
                success_symbols=success_symbols,
                failure_symbols=failure_symbols,
                until=until,
            )

            # 检查持仓超时告警
            stage_started = time.perf_counter()
            self.check_long_held_positions()
            risk_check_elapsed = time.perf_counter() - stage_started

            # 更新同步状态
            self.sync_repo.update_sync_status(status='idle')

            # 显示统计信息
            stats = self.sync_repo.get_statistics()
            logger.info("同步完成!")
            logger.info(f"数据库统计: 总交易数={stats['total_trades']}, 币种数={stats['unique_symbols']}")
            logger.info(f"时间范围: {stats['earliest_trade']} ~ {stats['latest_trade']}")
            total_elapsed = time.perf_counter() - sync_started_at
            logger.info(
                "同步耗时汇总: "
                f"symbols={symbols_elapsed:.2f}s, "
                f"analyze={analyze_elapsed:.2f}s, "
                f"save={save_trades_elapsed:.2f}s, "
                f"open_positions={open_positions_elapsed:.2f}s, "
                f"risk_check={risk_check_elapsed:.2f}s, "
                f"total={total_elapsed:.2f}s, "
                f"symbol_count={symbol_count}, "
                f"trades_saved={trades_saved}, "
                f"open_saved={open_saved}"
            )
            self.sync_repo.log_sync_run(
                run_type='trades_sync',
                mode='full' if force_full else 'incremental',
                status='success',
                symbol_count=symbol_count,
                rows_count=len(df),
                trades_saved=trades_saved,
                open_saved=open_saved,
                elapsed_ms=int(total_elapsed * 1000),
            )
            logger.info("=" * 50)
            return True

        except Exception as e:
            error_msg = f"同步失败: {str(e)}"
            logger.error(error_msg)
            total_elapsed = time.perf_counter() - sync_started_at
            logger.error(
                "同步失败耗时汇总: "
                f"symbols={symbols_elapsed:.2f}s, "
                f"analyze={analyze_elapsed:.2f}s, "
                f"save={save_trades_elapsed:.2f}s, "
                f"open_positions={open_positions_elapsed:.2f}s, "
                f"risk_check={risk_check_elapsed:.2f}s, "
                f"total={total_elapsed:.2f}s"
            )
            self.sync_repo.update_sync_status(status='error', error_message=error_msg)
            self.sync_repo.log_sync_run(
                run_type='trades_sync',
                mode='full' if force_full else 'incremental',
                status='error',
                symbol_count=symbol_count,
                rows_count=0,
                trades_saved=trades_saved,
                open_saved=open_saved,
                elapsed_ms=int(total_elapsed * 1000),
                error_message=error_msg,
            )
            import traceback
            logger.error(traceback.format_exc())
            return False
        finally:
            self._release_api_job_slot()

    def _resolve_sync_window(self, *, force_full: bool, last_entry_time: str | None) -> tuple[int, int, bool]:
        is_full_sync_run = force_full
        if force_full:
            is_full_sync_run = True
            if self.start_date:
                try:
                    start_dt = datetime.strptime(self.start_date, '%Y-%m-%d').replace(tzinfo=UTC8)
                    start_dt = start_dt.replace(hour=23, minute=0, second=0, microsecond=0)
                    since = int(start_dt.timestamp() * 1000)
                    logger.info(f"全量更新模式(FORCE_FULL_SYNC) - 从自定义日期 {self.start_date} 开始")
                except ValueError as e:
                    logger.error(f"日期格式错误: {e}，使用默认DAYS_TO_FETCH")
                    since = int((datetime.now(UTC8) - timedelta(days=self.days_to_fetch)).timestamp() * 1000)
            else:
                logger.warning("FORCE_FULL_SYNC=1 但未设置 START_DATE，回退为 DAYS_TO_FETCH 窗口")
                since = int((datetime.now(UTC8) - timedelta(days=self.days_to_fetch)).timestamp() * 1000)
        elif last_entry_time:
            try:
                last_dt = datetime.strptime(last_entry_time, '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC8)
                since = int((last_dt - timedelta(minutes=self.sync_lookback_minutes)).timestamp() * 1000)
                logger.info(f"增量更新模式 - 从最近入场时间 {last_entry_time} 回溯 {self.sync_lookback_minutes} 分钟")
            except ValueError as e:
                logger.error(f"入场时间解析失败: {e}，使用默认DAYS_TO_FETCH")
                since = int((datetime.now(UTC8) - timedelta(days=self.days_to_fetch)).timestamp() * 1000)
        else:
            logger.info(f"增量冷启动 - 获取最近 {self.days_to_fetch} 天数据")
            since = int((datetime.now(UTC8) - timedelta(days=self.days_to_fetch)).timestamp() * 1000)

        if self.end_date:
            try:
                end_dt = datetime.strptime(self.end_date, '%Y-%m-%d').replace(tzinfo=UTC8)
                end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999000)
                until = int(end_dt.timestamp() * 1000)
                logger.info(f"使用自定义结束日期: {self.end_date}")
            except ValueError:
                until = int(datetime.now(UTC8).timestamp() * 1000)
        else:
            until = int(datetime.now(UTC8).timestamp() * 1000)
        return since, until, is_full_sync_run

    def _fetch_and_analyze_closed_trades(
        self,
        *,
        since: int,
        until: int,
        is_full_sync_run: bool,
    ):
        logger.info("从Binance API抓取数据...")
        stage_started = time.perf_counter()
        traded_symbols = self.processor.get_traded_symbols(since, until)
        symbols_elapsed = time.perf_counter() - stage_started
        symbol_count = len(traded_symbols)
        logger.info(f"拉取活跃交易币种完成: count={symbol_count}, elapsed={symbols_elapsed:.2f}s")

        stage_started = time.perf_counter()
        symbol_since_map = None
        if not is_full_sync_run and traded_symbols:
            watermarks = self.sync_repo.get_symbol_sync_watermarks(traded_symbols)
            symbol_since_map, warmed_symbols = build_symbol_since_map(
                traded_symbols=traded_symbols,
                watermarks=watermarks,
                since=since,
                overlap_minutes=self.symbol_sync_overlap_minutes,
            )
            logger.info(
                "增量水位策略: "
                f"symbols={len(traded_symbols)}, "
                f"warm={warmed_symbols}, "
                f"cold={len(traded_symbols) - warmed_symbols}, "
                f"overlap_minutes={self.symbol_sync_overlap_minutes}"
            )

        if traded_symbols:
            analysis_result = self.processor.analyze_orders(
                since=since,
                until=until,
                traded_symbols=traded_symbols,
                use_time_filter=self.use_time_filter,
                symbol_since_map=symbol_since_map,
                return_symbol_status=True,
            )
            if not isinstance(analysis_result, (tuple, list)) or len(analysis_result) != 3:
                raise RuntimeError(
                    f"analyze_orders返回结构异常: type={type(analysis_result)}, value={analysis_result}"
                )
            df, success_symbols, failure_symbols = analysis_result
        else:
            df = pd.DataFrame()
            success_symbols = []
            failure_symbols = {}
            logger.info("无活跃币种，跳过闭仓ETL分析")

        analyze_elapsed = time.perf_counter() - stage_started
        logger.info(f"闭仓ETL完成: rows={len(df)}, elapsed={analyze_elapsed:.2f}s")
        return df, success_symbols, failure_symbols, symbol_count, symbols_elapsed, analyze_elapsed

    def _persist_closed_trades_and_watermarks(
        self,
        *,
        df: pd.DataFrame,
        force_full: bool,
        success_symbols: list[str],
        failure_symbols: dict[str, str],
        until: int,
    ) -> tuple[float, int]:
        save_trades_elapsed = 0.0
        trades_saved = 0
        if df.empty:
            logger.info("没有新数据需要更新")
        else:
            is_full_sync = force_full
            logger.info(f"保存 {len(df)} 条记录到数据库 (覆盖模式={is_full_sync})...")
            stage_started = time.perf_counter()
            saved_count = self.sync_repo.save_trades(df, overwrite=is_full_sync)
            save_trades_elapsed += time.perf_counter() - stage_started
            trades_saved = saved_count

            if saved_count > 0:
                logger.info("检测到新平仓单，重算统计快照...")
                stage_started = time.perf_counter()
                self.sync_repo.recompute_trade_summary()
                save_trades_elapsed += time.perf_counter() - stage_started

        if success_symbols:
            stage_started = time.perf_counter()
            self.sync_repo.update_symbol_sync_success_batch(symbols=success_symbols, end_ms=until)
            save_trades_elapsed += time.perf_counter() - stage_started
            logger.info(f"同步水位推进: success_symbols={len(success_symbols)}")
        if failure_symbols:
            stage_started = time.perf_counter()
            self.sync_repo.update_symbol_sync_failure_batch(failures=failure_symbols, end_ms=until)
            save_trades_elapsed += time.perf_counter() - stage_started
            logger.warning(f"同步水位未推进(失败): failed_symbols={len(failure_symbols)}")
        return save_trades_elapsed, trades_saved

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
        status = "success"
        with measure_ms("scheduler.sync_balance_data") as metric:
            try:
                if not self.processor:
                    return  # 如果没有配置API密钥，则不执行
                if self._is_api_cooldown_active(source='余额同步'):
                    return
                if not self._try_enter_api_job_slot(source='余额同步'):
                    return

                try:
                    logger.info("开始同步账户余额...")
                    # balance_info returns {'margin_balance': float, 'wallet_balance': float}
                    balance_info = self.processor.get_account_balance()

                    if balance_info:
                        current_margin = balance_info['margin_balance']
                        current_wallet = balance_info['wallet_balance']

                        # --- 通过 Binance income API 直接同步出入金 ---
                        try:
                            latest_event_time_ms = self.sync_repo.get_latest_transfer_event_time()
                            if latest_event_time_ms is None:
                                lookback_days_raw = os.getenv('TRANSFER_SYNC_LOOKBACK_DAYS', '90')
                                try:
                                    lookback_days = max(1, int(lookback_days_raw))
                                except ValueError:
                                    logger.warning(
                                        f"TRANSFER_SYNC_LOOKBACK_DAYS={lookback_days_raw} 非法，回退为 90"
                                    )
                                    lookback_days = 90
                                start_time_ms = int(
                                    (datetime.now(timezone.utc) - timedelta(days=lookback_days)).timestamp() * 1000
                                )
                            else:
                                # 往前回看1分钟做边界保护，落库侧会按 source_uid 去重
                                start_time_ms = max(0, latest_event_time_ms - 60_000)

                            end_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
                            transfer_rows = self.processor.get_transfer_income_records(
                                start_time=start_time_ms,
                                end_time=end_time_ms
                            )

                            inserted_count = 0
                            for row in transfer_rows:
                                inserted = self.sync_repo.save_transfer_income(
                                    amount=row['amount'],
                                    event_time=row['event_time_ms'],
                                    asset=row.get('asset') or 'USDT',
                                    income_type=row.get('income_type') or 'TRANSFER',
                                    source_uid=row.get('source_uid'),
                                    description=row.get('description')
                                )
                                if inserted:
                                    inserted_count += 1

                            logger.info(
                                "出入金同步完成: "
                                f"fetched={len(transfer_rows)}, inserted={inserted_count}, "
                                f"window={self._format_window_with_ms(start_time_ms, end_time_ms)}"
                            )
                        except Exception as e:
                            logger.warning(f"出入金同步出错: {e}")

                        # 保存当前状态
                        self.trade_repo.save_balance_history(current_margin, current_wallet)
                        logger.info(f"余额已更新: {current_margin:.2f} USDT (Wallet: {current_wallet:.2f})")
                    else:
                        logger.warning("获取余额失败，balance为 None")
                except Exception as e:
                    status = "error"
                    logger.error(f"同步余额失败: {str(e)}")
                finally:
                    self._release_api_job_slot()
            finally:
                log_job_metric(job_name="sync_balance_data", status=status, snapshot=metric)

    def start(self):
        """启动定时任务"""
        if not self.processor:
            logger.warning("定时任务未启动: API密钥未配置")
            return

        # 立即执行一次同步
        logger.info("立即执行首次数据同步...")
        self.scheduler.add_job(partial(run_sync_trades_incremental, self), 'date')
        self.scheduler.add_job(partial(run_sync_open_positions, self), 'date')
        self.scheduler.add_job(self.sync_balance_data, 'date')

        # 增量同步任务 - 每隔N分钟执行一次
        self.scheduler.add_job(
            func=partial(run_sync_trades_incremental, self),
            trigger=IntervalTrigger(minutes=self.update_interval_minutes),
            id='sync_trades_incremental',
            name='同步交易数据(增量)',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
            replace_existing=True
        )

        # 未平仓同步任务 - 与闭仓ETL解耦
        self.scheduler.add_job(
            func=partial(run_sync_open_positions, self),
            trigger=IntervalTrigger(minutes=self.open_positions_update_interval_minutes),
            id='sync_open_positions',
            name='同步未平仓订单',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
            replace_existing=True
        )

        # 每日全量同步任务 - 默认每天 03:30 (UTC+8)
        if self.enable_daily_full_sync:
            self.scheduler.add_job(
                func=self.sync_trades_full,
                trigger=CronTrigger(
                    hour=self.daily_full_sync_hour,
                    minute=self.daily_full_sync_minute,
                    timezone=UTC8
                ),
                id='sync_trades_full_daily',
                name='同步交易数据(全量)',
                max_instances=1,
                coalesce=True,
                misfire_grace_time=600,
                replace_existing=True
            )
            logger.info(
                "全量同步任务已启动: "
                f"每天 {self.daily_full_sync_hour:02d}:{self.daily_full_sync_minute:02d} 执行"
            )
        else:
            logger.info("全量同步任务未启用: ENABLE_DAILY_FULL_SYNC=0")

        if not self.enable_user_stream:
            # 添加余额同步任务 - 每分钟执行一次
            self.scheduler.add_job(
                func=self.sync_balance_data,
                trigger=IntervalTrigger(minutes=1),
                id='sync_balance',
                name='同步账户余额',
                max_instances=1,
                coalesce=True,
                misfire_grace_time=60,
                replace_existing=True
            )
        else:
            logger.info("已启用用户数据流，跳过轮询余额同步任务")

        # 添加睡前风控检查任务 - 每天 23:00 (UTC+8) 执行
        self.scheduler.add_job(
            func=self.check_risk_before_sleep,
            trigger=CronTrigger(hour=23, minute=0, timezone=UTC8),
            id='risk_check_sleep',
            name='睡前风控检查',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
            replace_existing=True
        )

        # 添加午间止损复盘任务 - 每天 23:02 (UTC+8) 执行（默认）
        self.scheduler.add_job(
            func=self.review_noon_loss_at_night,
            trigger=CronTrigger(
                hour=self.noon_review_hour,
                minute=self.noon_review_minute,
                timezone=UTC8
            ),
            id='review_noon_loss_night',
            name='午间止损夜间复盘',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
            replace_existing=True
        )

        # 添加午间浮亏检查任务 - 默认每天 11:50 (UTC+8) 执行
        self.scheduler.add_job(
            func=partial(run_noon_loss_check, self),
            trigger=CronTrigger(
                hour=self.noon_loss_check_hour,
                minute=self.noon_loss_check_minute,
                timezone=UTC8
            ),
            id='check_losses_noon',
            name='午间浮亏检查',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
            replace_existing=True
        )

        if self.enable_leaderboard_alert:
            self.scheduler.add_job(
                func=self.send_morning_top_gainers,
                trigger=CronTrigger(
                    hour=self.leaderboard_alert_hour,
                    minute=self.leaderboard_alert_minute,
                    timezone=UTC8
                ),
                id='send_morning_top_gainers',
                name='晨间涨幅榜',
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300,
                replace_existing=True
            )
            logger.info(
                "晨间涨幅榜任务已启动: "
                f"每天 {self.leaderboard_alert_hour:02d}:{self.leaderboard_alert_minute:02d} 执行"
            )
        else:
            logger.info("晨间涨幅榜任务未启用: ENABLE_LEADERBOARD_ALERT=0")

        if self.enable_rebound_7d_snapshot:
            self.scheduler.add_job(
                func=self.snapshot_morning_rebound_7d,
                trigger=CronTrigger(
                    hour=self.rebound_7d_hour,
                    minute=self.rebound_7d_minute,
                    timezone=UTC8
                ),
                id='snapshot_morning_rebound_7d',
                name='晨间14D反弹榜',
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300,
                replace_existing=True
            )
            logger.info(
                "晨间14D反弹榜任务已启动: "
                f"每天 {self.rebound_7d_hour:02d}:{self.rebound_7d_minute:02d} 执行"
            )
        else:
            logger.info("晨间14D反弹榜任务未启用: ENABLE_REBOUND_7D_SNAPSHOT=0")

        if self.enable_rebound_30d_snapshot:
            self.scheduler.add_job(
                func=self.snapshot_morning_rebound_30d,
                trigger=CronTrigger(
                    hour=self.rebound_30d_hour,
                    minute=self.rebound_30d_minute,
                    timezone=UTC8
                ),
                id='snapshot_morning_rebound_30d',
                name='晨间30D反弹榜',
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300,
                replace_existing=True
            )
            logger.info(
                "晨间30D反弹榜任务已启动: "
                f"每天 {self.rebound_30d_hour:02d}:{self.rebound_30d_minute:02d} 执行"
            )
        else:
            logger.info("晨间30D反弹榜任务未启用: ENABLE_REBOUND_30D_SNAPSHOT=0")

        if self.enable_rebound_60d_snapshot:
            self.scheduler.add_job(
                func=self.snapshot_morning_rebound_60d,
                trigger=CronTrigger(
                    hour=self.rebound_60d_hour,
                    minute=self.rebound_60d_minute,
                    timezone=UTC8
                ),
                id='snapshot_morning_rebound_60d',
                name='晨间60D反弹榜',
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300,
                replace_existing=True
            )
            logger.info(
                "晨间60D反弹榜任务已启动: "
                f"每天 {self.rebound_60d_hour:02d}:{self.rebound_60d_minute:02d} 执行"
            )
        else:
            logger.info("晨间60D反弹榜任务未启用: ENABLE_REBOUND_60D_SNAPSHOT=0")

        self.scheduler.start()
        logger.info(f"增量交易同步任务已启动: 每 {self.update_interval_minutes} 分钟自动更新一次")
        logger.info(
            f"未平仓同步任务已启动: 每 {self.open_positions_update_interval_minutes} 分钟自动更新一次 "
            f"(lookback_days={self.open_positions_lookback_days})"
        )
        logger.info("余额监控任务已启动: 每 1 分钟自动更新一次")
        logger.info("睡前风控检查已启动: 每天 23:00 执行")
        logger.info(
            "午间浮亏检查已启动: "
            f"每天 {self.noon_loss_check_hour:02d}:{self.noon_loss_check_minute:02d} 执行"
        )
        logger.info(
            "午间止损夜间复盘已启动: "
            f"每天 {self.noon_review_hour:02d}:{self.noon_review_minute:02d} 执行, "
            f"target_day_offset={self.noon_review_target_day_offset}"
        )

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


# 全局实例
scheduler_instance = None


def _env_is_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "on")


def _resolve_worker_count() -> int:
    raw = os.getenv("WEB_CONCURRENCY") or os.getenv("UVICORN_WORKERS") or "1"
    try:
        count = int(raw)
    except (TypeError, ValueError):
        count = 1
    return max(1, count)


def should_start_scheduler() -> tuple[bool, str]:
    api_key = os.getenv("BINANCE_API_KEY")
    api_secret = os.getenv("BINANCE_API_SECRET")
    if not api_key or not api_secret:
        return False, "missing_api_keys"

    worker_count = _resolve_worker_count()
    allow_multi_worker = _env_is_truthy(os.getenv("SCHEDULER_ALLOW_MULTI_WORKER"))
    if worker_count > 1 and not allow_multi_worker:
        return False, "multi_worker_unsupported"

    return True, "ok"


def get_scheduler() -> TradeDataScheduler:
    """获取调度器单例"""
    global scheduler_instance
    if scheduler_instance is None:
        scheduler_instance = TradeDataScheduler()
    return scheduler_instance
