"""
定时任务调度器 - 自动更新交易数据
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
import time
import threading
from functools import partial
from dotenv import load_dotenv
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.trade_processor import TradeDataProcessor
from app.database import Database
from app.jobs.noon_loss_job import run_noon_loss_check, run_noon_loss_review
from app.jobs.risk_jobs import run_long_held_positions_check, run_sleep_risk_check
from app.jobs.sync_jobs import run_sync_open_positions, run_sync_trades_incremental
from app.logger import logger
from app.notifier import send_server_chan_notification
from app.binance_client import BinanceFuturesRestClient

load_dotenv()

# 定义UTC+8时区
UTC8 = ZoneInfo("Asia/Shanghai")


class TradeDataScheduler:
    """交易数据定时更新调度器"""

    def __init__(self):
        def _env_int(name: str, default: int, minimum: int | None = None) -> int:
            raw = os.getenv(name)
            if raw is None:
                value = default
            else:
                try:
                    value = int(raw)
                except ValueError:
                    logger.warning(f"环境变量 {name}={raw} 非法，使用默认值 {default}")
                    value = default
            if minimum is not None:
                value = max(minimum, value)
            return value

        def _env_float(name: str, default: float, minimum: float | None = None) -> float:
            raw = os.getenv(name)
            if raw is None:
                value = default
            else:
                try:
                    value = float(raw)
                except ValueError:
                    logger.warning(f"环境变量 {name}={raw} 非法，使用默认值 {default}")
                    value = default
            if minimum is not None:
                value = max(minimum, value)
            return value

        scheduler_tz = os.getenv('SCHEDULER_TIMEZONE', 'Asia/Shanghai')
        try:
            self.scheduler = BackgroundScheduler(timezone=ZoneInfo(scheduler_tz))
        except Exception as exc:
            logger.warning(f"无效的调度器时区 {scheduler_tz}: {exc}，使用默认时区")
            self.scheduler = BackgroundScheduler()
        self.db = Database()

        # 从环境变量获取配置
        api_key = os.getenv('BINANCE_API_KEY')
        api_secret = os.getenv('BINANCE_API_SECRET')

        if not api_key or not api_secret:
            logger.warning("未配置Binance API密钥，定时任务将无法运行")
            self.processor = None
        else:
            self.processor = TradeDataProcessor(api_key, api_secret)

        self.days_to_fetch = _env_int('DAYS_TO_FETCH', 30, minimum=1)
        self.update_interval_minutes = _env_int('UPDATE_INTERVAL_MINUTES', 10, minimum=1)
        self.open_positions_update_interval_minutes = _env_int(
            'OPEN_POSITIONS_UPDATE_INTERVAL_MINUTES',
            self.update_interval_minutes,
            minimum=1
        )
        self.start_date = os.getenv('START_DATE')  # 自定义起始日期
        self.end_date = os.getenv('END_DATE')      # 自定义结束日期
        self.sync_lookback_minutes = _env_int('SYNC_LOOKBACK_MINUTES', 1440, minimum=1)
        self.symbol_sync_overlap_minutes = _env_int('SYMBOL_SYNC_OVERLAP_MINUTES', 1440, minimum=1)
        self.open_positions_lookback_days = _env_int('OPEN_POSITIONS_LOOKBACK_DAYS', 60, minimum=1)
        self.enable_daily_full_sync = os.getenv('ENABLE_DAILY_FULL_SYNC', '1').lower() in ('1', 'true', 'yes')
        self.daily_full_sync_hour = _env_int('DAILY_FULL_SYNC_HOUR', 3, minimum=0)
        self.daily_full_sync_minute = _env_int('DAILY_FULL_SYNC_MINUTE', 30, minimum=0)
        self.use_time_filter = os.getenv('SYNC_USE_TIME_FILTER', '1').lower() in ('1', 'true', 'yes')
        self.enable_user_stream = os.getenv('ENABLE_USER_STREAM', '0').lower() in ('1', 'true', 'yes')
        self.force_full_sync = os.getenv('FORCE_FULL_SYNC', '0').lower() in ('1', 'true', 'yes')
        self.enable_leaderboard_alert = os.getenv('ENABLE_LEADERBOARD_ALERT', '1').lower() in ('1', 'true', 'yes')
        self.leaderboard_top_n = _env_int('LEADERBOARD_TOP_N', 10, minimum=1)
        self.leaderboard_min_quote_volume = _env_float('LEADERBOARD_MIN_QUOTE_VOLUME', 50_000_000, minimum=0.0)
        self.leaderboard_max_symbols = _env_int('LEADERBOARD_MAX_SYMBOLS', 120, minimum=0)
        self.leaderboard_kline_workers = _env_int('LEADERBOARD_KLINE_WORKERS', 6, minimum=1)
        self.leaderboard_weight_budget_per_minute = _env_int('LEADERBOARD_WEIGHT_BUDGET_PER_MINUTE', 900, minimum=60)
        self.leaderboard_alert_hour = _env_int('LEADERBOARD_ALERT_HOUR', 7, minimum=0)
        self.leaderboard_alert_minute = _env_int('LEADERBOARD_ALERT_MINUTE', 40, minimum=0)
        self.leaderboard_guard_before_minutes = _env_int('LEADERBOARD_GUARD_BEFORE_MINUTES', 2, minimum=0)
        self.leaderboard_guard_after_minutes = _env_int('LEADERBOARD_GUARD_AFTER_MINUTES', 5, minimum=0)
        self.enable_rebound_7d_snapshot = os.getenv('ENABLE_REBOUND_7D_SNAPSHOT', '1').lower() in ('1', 'true', 'yes')
        self.rebound_7d_top_n = _env_int('REBOUND_7D_TOP_N', 10, minimum=1)
        self.rebound_7d_kline_workers = _env_int('REBOUND_7D_KLINE_WORKERS', 6, minimum=1)
        self.rebound_7d_weight_budget_per_minute = _env_int('REBOUND_7D_WEIGHT_BUDGET_PER_MINUTE', 900, minimum=60)
        self.rebound_7d_hour = _env_int('REBOUND_7D_HOUR', 7, minimum=0)
        self.rebound_7d_minute = _env_int('REBOUND_7D_MINUTE', 30, minimum=0)
        self.enable_rebound_30d_snapshot = os.getenv('ENABLE_REBOUND_30D_SNAPSHOT', '1').lower() in ('1', 'true', 'yes')
        self.rebound_30d_top_n = _env_int('REBOUND_30D_TOP_N', 10, minimum=1)
        self.rebound_30d_kline_workers = _env_int('REBOUND_30D_KLINE_WORKERS', self.rebound_7d_kline_workers, minimum=1)
        self.rebound_30d_weight_budget_per_minute = _env_int(
            'REBOUND_30D_WEIGHT_BUDGET_PER_MINUTE', self.rebound_7d_weight_budget_per_minute, minimum=60
        )
        self.rebound_30d_hour = _env_int('REBOUND_30D_HOUR', self.rebound_7d_hour, minimum=0)
        self.rebound_30d_minute = _env_int('REBOUND_30D_MINUTE', self.rebound_7d_minute + 2, minimum=0)
        self.enable_rebound_60d_snapshot = os.getenv('ENABLE_REBOUND_60D_SNAPSHOT', '1').lower() in ('1', 'true', 'yes')
        self.rebound_60d_top_n = _env_int('REBOUND_60D_TOP_N', 10, minimum=1)
        self.rebound_60d_kline_workers = _env_int('REBOUND_60D_KLINE_WORKERS', self.rebound_7d_kline_workers, minimum=1)
        self.rebound_60d_weight_budget_per_minute = _env_int(
            'REBOUND_60D_WEIGHT_BUDGET_PER_MINUTE', self.rebound_7d_weight_budget_per_minute, minimum=60
        )
        self.rebound_60d_hour = _env_int('REBOUND_60D_HOUR', self.rebound_7d_hour, minimum=0)
        self.rebound_60d_minute = _env_int('REBOUND_60D_MINUTE', self.rebound_7d_minute + 4, minimum=0)
        self.noon_loss_check_hour = _env_int('NOON_LOSS_CHECK_HOUR', 11, minimum=0)
        self.noon_loss_check_minute = _env_int('NOON_LOSS_CHECK_MINUTE', 50, minimum=0)
        self.noon_review_hour = _env_int('NOON_REVIEW_HOUR', 23, minimum=0)
        self.noon_review_minute = _env_int('NOON_REVIEW_MINUTE', 2, minimum=0)
        self.noon_review_target_day_offset = _env_int('NOON_REVIEW_TARGET_DAY_OFFSET', 0)
        self.enable_profit_alert = os.getenv('ENABLE_PROFIT_ALERT', '1').lower() in ('1', 'true', 'yes')
        self.enable_reentry_alert = os.getenv('ENABLE_REENTRY_ALERT', '1').lower() in ('1', 'true', 'yes')
        self.profit_alert_threshold_pct = _env_float('PROFIT_ALERT_THRESHOLD_PCT', 20.0, minimum=0.0)
        self.leaderboard_alert_hour %= 24
        self.leaderboard_alert_minute %= 60
        self.rebound_7d_hour %= 24
        self.rebound_7d_minute %= 60
        self.rebound_30d_hour %= 24
        self.rebound_30d_minute %= 60
        self.rebound_60d_hour %= 24
        self.rebound_60d_minute %= 60
        self.noon_loss_check_hour %= 24
        self.noon_loss_check_minute %= 60
        self.noon_review_hour %= 24
        self.noon_review_minute %= 60
        self.api_job_lock_wait_seconds = _env_int('API_JOB_LOCK_WAIT_SECONDS', 8, minimum=0)
        self._api_job_lock = threading.Lock()

    def _is_api_cooldown_active(self, source: str) -> bool:
        remaining = BinanceFuturesRestClient.cooldown_remaining_seconds()
        if remaining > 0:
            logger.warning(
                f"Binance API冷却中，跳过{source}: remaining={remaining:.1f}s"
            )
            return True
        return False

    def _try_enter_api_job_slot(self, source: str) -> bool:
        wait_seconds = self.api_job_lock_wait_seconds
        if wait_seconds <= 0:
            return True

        acquired = self._api_job_lock.acquire(timeout=wait_seconds)

        if not acquired:
            logger.warning(
                f"{source}跳过: API任务互斥锁繁忙(等待{wait_seconds}s后超时)"
            )
            return False
        return True

    def _release_api_job_slot(self):
        if self.api_job_lock_wait_seconds <= 0:
            return
        if self._api_job_lock.locked():
            self._api_job_lock.release()

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

    def sync_trades_data(self, force_full: bool = False):
        """同步交易数据到数据库"""
        if not self.processor:
            logger.warning("无法同步: API密钥未配置")
            return
        if self._is_leaderboard_guard_window():
            logger.info(
                "跳过交易同步: 位于晨间涨幅榜保护窗口内 "
                f"({self.leaderboard_alert_hour:02d}:{self.leaderboard_alert_minute:02d} "
                f"前{self.leaderboard_guard_before_minutes}分钟至后{self.leaderboard_guard_after_minutes}分钟)"
            )
            return
        if self._is_api_cooldown_active(source='交易同步'):
            return
        if not self._try_enter_api_job_slot(source='交易同步'):
            return

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
            self.db.update_sync_status(status='syncing')

            # 获取最后一条交易时间（仅作参考，不再用于增量更新）
            # last_entry_time = self.db.get_last_entry_time()

            # 同步模式：
            # 1) force_full=True -> 全量模式（支持 START_DATE）
            # 2) force_full=False -> 增量模式（按最后入场时间回看）
            last_entry_time = self.db.get_last_entry_time()
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
                    logger.info(
                        f"增量更新模式 - 从最近入场时间 {last_entry_time} 回溯 {self.sync_lookback_minutes} 分钟"
                    )
                except ValueError as e:
                    logger.error(f"入场时间解析失败: {e}，使用默认DAYS_TO_FETCH")
                    since = int((datetime.now(UTC8) - timedelta(days=self.days_to_fetch)).timestamp() * 1000)
            else:
                # 增量模式冷启动：使用 DAYS_TO_FETCH 窗口
                logger.info(f"增量冷启动 - 获取最近 {self.days_to_fetch} 天数据")
                since = int((datetime.now(UTC8) - timedelta(days=self.days_to_fetch)).timestamp() * 1000)

            # 计算结束时间
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

            # 从Binance获取数据
            logger.info("从Binance API抓取数据...")
            stage_started = time.perf_counter()
            traded_symbols = self.processor.get_traded_symbols(since, until)
            symbols_elapsed = time.perf_counter() - stage_started
            symbol_count = len(traded_symbols)
            logger.info(f"拉取活跃交易币种完成: count={symbol_count}, elapsed={symbols_elapsed:.2f}s")

            stage_started = time.perf_counter()
            symbol_since_map = None
            if not is_full_sync_run and traded_symbols:
                watermarks = self.db.get_symbol_sync_watermarks(traded_symbols)
                overlap_ms = self.symbol_sync_overlap_minutes * 60 * 1000
                symbol_since_map = {}
                warmed_symbols = 0
                for symbol in traded_symbols:
                    symbol_watermark = watermarks.get(symbol)
                    if symbol_watermark is None:
                        symbol_since_map[symbol] = since
                    else:
                        symbol_since_map[symbol] = max(since, symbol_watermark - overlap_ms)
                        warmed_symbols += 1
                logger.info(
                    "增量水位策略: "
                    f"symbols={len(traded_symbols)}, "
                    f"warm={warmed_symbols}, "
                    f"cold={len(traded_symbols) - warmed_symbols}, "
                    f"overlap_minutes={self.symbol_sync_overlap_minutes}"
                )

            analysis_result = self.processor.analyze_orders(
                since=since,
                until=until,
                traded_symbols=traded_symbols,
                use_time_filter=self.use_time_filter,
                symbol_since_map=symbol_since_map,
                return_symbol_status=True,
            )
            df, success_symbols, failure_symbols = analysis_result
            analyze_elapsed = time.perf_counter() - stage_started
            logger.info(f"闭仓ETL完成: rows={len(df)}, elapsed={analyze_elapsed:.2f}s")

            if df.empty:
                logger.info("没有新数据需要更新")
            else:
                # 仅“本轮明确全量”时才允许覆盖写入。
                # 增量同步必须 append/upsert，避免重复删写导致日统计抖动。
                is_full_sync = force_full

                logger.info(f"保存 {len(df)} 条记录到数据库 (覆盖模式={is_full_sync})...")
                stage_started = time.perf_counter()
                saved_count = self.db.save_trades(df, overwrite=is_full_sync)
                save_trades_elapsed += time.perf_counter() - stage_started
                trades_saved = saved_count

                if saved_count > 0:
                    logger.info("检测到新平仓单，重算统计快照...")
                    stage_started = time.perf_counter()
                    self.db.recompute_trade_summary()
                    save_trades_elapsed += time.perf_counter() - stage_started

            if success_symbols:
                stage_started = time.perf_counter()
                for symbol in success_symbols:
                    self.db.update_symbol_sync_success(symbol=symbol, end_ms=until)
                save_trades_elapsed += time.perf_counter() - stage_started
                logger.info(f"同步水位推进: success_symbols={len(success_symbols)}")
            if failure_symbols:
                stage_started = time.perf_counter()
                for symbol, err in failure_symbols.items():
                    self.db.update_symbol_sync_failure(symbol=symbol, end_ms=until, error_message=err)
                save_trades_elapsed += time.perf_counter() - stage_started
                logger.warning(f"同步水位未推进(失败): failed_symbols={len(failure_symbols)}")

            # 检查持仓超时告警
            stage_started = time.perf_counter()
            self.check_long_held_positions()
            risk_check_elapsed = time.perf_counter() - stage_started

            # 更新同步状态
            self.db.update_sync_status(status='idle')

            # 显示统计信息
            stats = self.db.get_statistics()
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
            self.db.log_sync_run(
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
            self.db.update_sync_status(status='error', error_message=error_msg)
            self.db.log_sync_run(
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
        finally:
            self._release_api_job_slot()

    def sync_open_positions_data(self):
        return run_sync_open_positions(self)

    def sync_trades_incremental(self):
        """增量同步交易数据"""
        self.sync_trades_data(force_full=False)

    def sync_trades_full(self):
        """全量同步交易数据"""
        self.sync_trades_data(force_full=True)

    def _get_mark_price_map(self, symbols: list[str]) -> dict[str, float]:
        """批量获取标记价格（优先 premiumIndex，其次 ticker/price）。"""
        if not symbols:
            return {}

        unique_symbols = sorted(set(symbols))
        resolved: dict[str, float] = {}
        missing = set(unique_symbols)

        try:
            data = self.processor.client.public_get("/fapi/v1/premiumIndex")
            if isinstance(data, dict):
                data = [data]
            for item in data or []:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol", "")).upper()
                raw_price = item.get("markPrice")
                if not symbol or raw_price is None:
                    continue
                try:
                    price = float(raw_price)
                except (TypeError, ValueError):
                    continue
                if price <= 0:
                    continue
                if symbol in missing:
                    resolved[symbol] = price
                    missing.discard(symbol)
        except Exception as exc:
            logger.warning(f"获取标记价格(premiumIndex)失败: {exc}")

        if missing:
            try:
                data = self.processor.client.public_get("/fapi/v1/ticker/price")
                if isinstance(data, dict):
                    data = [data]
                for item in data or []:
                    if not isinstance(item, dict):
                        continue
                    symbol = str(item.get("symbol", "")).upper()
                    raw_price = item.get("price")
                    if symbol not in missing or raw_price is None:
                        continue
                    try:
                        price = float(raw_price)
                    except (TypeError, ValueError):
                        continue
                    if price <= 0:
                        continue
                    resolved[symbol] = price
                    missing.discard(symbol)
            except Exception as exc:
                logger.warning(f"获取标记价格(ticker/price)失败: {exc}")

        if missing:
            logger.warning(
                f"仍有{len(missing)}个symbol无法获取夜间价格: {sorted(list(missing))[:10]}"
            )

        return resolved

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
        """同币在 UTC 当天内重复开仓提醒（每笔重复开仓仅提醒一次）。"""
        if not self.enable_reentry_alert:
            return

        try:
            positions = self.db.get_open_positions()
            if len(positions) < 2:
                return

            by_symbol: dict[str, list[dict]] = {}
            for pos in positions:
                symbol = str(pos.get("symbol", "")).upper().strip()
                if not symbol:
                    continue

                entry_dt_utc8 = self._parse_entry_time_utc8(pos.get("entry_time"))
                if entry_dt_utc8 is None:
                    continue

                order_id_raw = pos.get("order_id")
                try:
                    order_id = int(order_id_raw or 0)
                except (TypeError, ValueError):
                    order_id = 0

                by_symbol.setdefault(symbol, []).append({
                    "symbol": symbol,
                    "order_id": order_id,
                    "side": str(pos.get("side", "")).upper(),
                    "entry_time": str(pos.get("entry_time", "")),
                    "entry_dt_utc8": entry_dt_utc8,
                    "entry_dt_utc": entry_dt_utc8.astimezone(timezone.utc),
                    "reentry_alerted": int(pos.get("reentry_alerted", 0) or 0),
                })

            triggered = []
            for symbol, rows in by_symbol.items():
                if len(rows) < 2:
                    continue

                rows.sort(key=lambda item: (item["entry_dt_utc8"], item["order_id"]))
                for idx in range(1, len(rows)):
                    current = rows[idx]
                    previous = rows[idx - 1]

                    if current["order_id"] <= 0:
                        continue
                    if current["reentry_alerted"] == 1:
                        continue

                    if current["entry_dt_utc"].date() == previous["entry_dt_utc"].date():
                        triggered.append({
                            "symbol": symbol,
                            "side": current["side"],
                            "order_id": current["order_id"],
                            "entry_time": current["entry_time"],
                            "previous_order_id": previous["order_id"],
                            "previous_entry_time": previous["entry_time"],
                            "utc_day": current["entry_dt_utc"].strftime("%Y-%m-%d"),
                        })

            if not triggered:
                return

            triggered.sort(key=lambda item: (item["symbol"], item["entry_time"], item["order_id"]))
            title = f"⚠️ 同币重复开仓提醒: {len(triggered)} 笔"
            content = (
                "检测到以下订单在同一 UTC 日期内重复开仓：\n"
                "（规则：首次开仓后，UTC+0 次日 00:00 前再次开同币）\n\n"
                "---\n"
            )

            preview_count = min(20, len(triggered))
            for item in triggered[:preview_count]:
                content += (
                    f"**{item['symbol']}** ({item['side']})\n"
                    f"- 重复开仓: #{item['order_id']} @ {item['entry_time']}\n"
                    f"- 上一笔: #{item['previous_order_id']} @ {item['previous_entry_time']}\n"
                    f"- UTC日期: {item['utc_day']}\n\n"
                )
            if len(triggered) > preview_count:
                content += f"... 其余 {len(triggered) - preview_count} 笔未展示。\n"

            send_server_chan_notification(title, content)

            for item in triggered:
                self.db.set_position_reentry_alerted(item["symbol"], item["order_id"])

            logger.info(
                "同币重复开仓提醒已发送: "
                f"count={len(triggered)}, symbols={sorted(set(item['symbol'] for item in triggered))}"
            )
        except Exception as exc:
            logger.error(f"同币重复开仓提醒检查失败: {exc}")

    def check_open_positions_profit_alert(self, threshold_pct: float):
        """检查未平仓订单浮盈阈值提醒（单档，单笔只提醒一次）。"""
        if not self.enable_profit_alert:
            return

        try:
            positions = self.db.get_open_positions()
            if not positions:
                return

            candidates = [p for p in positions if int(p.get("profit_alerted", 0) or 0) == 0]
            if not candidates:
                return

            symbols_full = [self._normalize_futures_symbol(p.get("symbol")) for p in candidates if p.get("symbol")]
            mark_prices = self._get_mark_price_map(symbols_full)
            if not mark_prices:
                logger.warning("盈利提醒检查跳过: 无法获取标记价格")
                return

            triggered = []
            for pos in candidates:
                symbol = str(pos.get("symbol", "")).upper()
                side = str(pos.get("side", "")).upper()
                qty = float(pos.get("qty", 0.0) or 0.0)
                entry_price = float(pos.get("entry_price", 0.0) or 0.0)
                entry_amount = float(pos.get("entry_amount", 0.0) or 0.0)
                order_id = int(pos.get("order_id", 0) or 0)
                entry_time = str(pos.get("entry_time", ""))

                if not symbol or qty <= 0 or entry_price <= 0 or entry_amount <= 0 or order_id <= 0:
                    continue

                symbol_full = self._normalize_futures_symbol(symbol)
                mark_price = mark_prices.get(symbol_full)
                if mark_price is None:
                    continue

                if side == "SHORT":
                    unrealized_pnl = (entry_price - mark_price) * qty
                else:
                    unrealized_pnl = (mark_price - entry_price) * qty

                unrealized_pct = (unrealized_pnl / entry_amount) * 100
                if unrealized_pct >= threshold_pct:
                    triggered.append({
                        "symbol": symbol,
                        "side": side,
                        "order_id": order_id,
                        "entry_time": entry_time,
                        "entry_price": entry_price,
                        "mark_price": mark_price,
                        "unrealized_pnl": unrealized_pnl,
                        "unrealized_pct": unrealized_pct
                    })

            if not triggered:
                return

            triggered.sort(key=lambda item: item["unrealized_pct"], reverse=True)
            title = f"🎯 浮盈提醒: {len(triggered)} 笔持仓超过 {threshold_pct:.0f}%"
            content = (
                f"以下未平仓订单浮盈已达到阈值 **{threshold_pct:.0f}%**（每笔仅提醒一次）:\n\n"
                "--- \n"
            )
            for item in triggered:
                content += (
                    f"**{item['symbol']}** ({item['side']})\n"
                    f"- 浮盈: {item['unrealized_pnl']:+.2f} U ({item['unrealized_pct']:.2f}%)\n"
                    f"- 开仓: {item['entry_price']:.6g}\n"
                    f"- 现价: {item['mark_price']:.6g}\n"
                    f"- 时间: {item['entry_time']}\n\n"
                )
            send_server_chan_notification(title, content)

            for item in triggered:
                self.db.set_position_profit_alerted(item["symbol"], item["order_id"])

            logger.info(
                "浮盈提醒已发送: "
                f"threshold={threshold_pct:.2f}%, "
                f"count={len(triggered)}, "
                f"symbols={[item['symbol'] for item in triggered]}"
            )
        except Exception as exc:
            logger.error(f"浮盈提醒检查失败: {exc}")

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
        """构建涨跌幅榜快照（不处理锁与冷却）。"""
        stage_started_at = time.perf_counter()
        now_utc = datetime.now(timezone.utc)
        midnight_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_utc_ms = int(midnight_utc.timestamp() * 1000)

        exchange_info = self.processor.get_exchange_info(client=self.processor.client)
        if not exchange_info or 'symbols' not in exchange_info:
            raise RuntimeError("无法获取 exchangeInfo")

        usdt_perpetual_symbols = {
            item.get('symbol')
            for item in exchange_info.get('symbols', [])
            if (
                item.get('contractType') == 'PERPETUAL'
                and item.get('quoteAsset') == 'USDT'
                and str(item.get('status', '')).upper() == 'TRADING'
            )
        }
        if not usdt_perpetual_symbols:
            raise RuntimeError("无可用USDT永续交易对")

        ticker_data = self.processor.client.public_get('/fapi/v1/ticker/24hr')
        if not ticker_data or not isinstance(ticker_data, list):
            raise RuntimeError("无法获取 24hr ticker")

        candidates = []
        for item in ticker_data:
            symbol = item.get('symbol')
            if not symbol or symbol not in usdt_perpetual_symbols:
                continue
            try:
                last_price = float(item.get('lastPrice', 0.0))
                quote_volume = float(item.get('quoteVolume', 0.0))
            except (TypeError, ValueError):
                continue

            if last_price <= 0:
                continue
            if quote_volume < self.leaderboard_min_quote_volume:
                continue

            candidates.append({
                'symbol': symbol,
                'last_price': last_price,
                'quote_volume': quote_volume,
            })

        # 先按成交额排序；如配置了上限则截断，避免K线请求过多
        candidates.sort(key=lambda x: x['quote_volume'], reverse=True)
        if self.leaderboard_max_symbols > 0:
            candidates = candidates[:self.leaderboard_max_symbols]
        logger.info(
            "晨间涨幅榜候选统计: "
            f"candidates={len(candidates)}, "
            f"min_quote_volume={self.leaderboard_min_quote_volume:.0f}, "
            f"max_symbols={self.leaderboard_max_symbols}"
        )

        leaderboard = []
        progress_step = 20
        total_candidates = len(candidates)
        if total_candidates > 0:
            # Binance Futures REST REQUEST_WEIGHT limit: 2400/min.
            # Klines(limit=1) weight=1; we reserve part of budget for other jobs and keep a conservative cap.
            min_interval = max(0.05, float(os.getenv("BINANCE_MIN_REQUEST_INTERVAL", "0.3")))
            per_worker_rpm = max(1.0, 60.0 / min_interval)  # each request here costs weight=1
            workers_by_budget = max(1, int(self.leaderboard_weight_budget_per_minute // per_worker_rpm))
            worker_count = min(total_candidates, self.leaderboard_kline_workers, workers_by_budget)
            estimated_peak_weight_per_min = int(worker_count * per_worker_rpm)
            estimated_total_weight = 1 + 40 + total_candidates  # exchangeInfo + 24hr ticker + per-symbol klines
            logger.info(
                "晨间涨幅榜并发计划: "
                f"workers={worker_count}, "
                f"min_interval={min_interval:.2f}s, "
                f"budget={self.leaderboard_weight_budget_per_minute}/min, "
                f"est_peak={estimated_peak_weight_per_min}/min, "
                f"est_total_weight={estimated_total_weight}"
            )

            thread_local = threading.local()

            def _kline_task(item: dict):
                if self._is_api_cooldown_active(source='涨幅榜-逐币种计算'):
                    return item, None
                worker_client = getattr(thread_local, "client", None)
                if worker_client is None:
                    worker_client = self.processor._create_worker_client()
                    thread_local.client = worker_client
                open_price = self.processor.get_price_change_from_utc_start(
                    symbol=item['symbol'],
                    timestamp=midnight_utc_ms,
                    client=worker_client
                )
                return item, open_price

            processed = 0
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [executor.submit(_kline_task, item) for item in candidates]
                for future in as_completed(futures):
                    processed += 1
                    try:
                        item, open_price = future.result()
                    except Exception as exc:
                        logger.warning(f"涨幅榜逐币种计算异常: {exc}")
                        if processed % progress_step == 0 or processed == total_candidates:
                            logger.info(
                                "晨间涨幅榜进度: "
                                f"{processed}/{total_candidates}, "
                                f"effective={len(leaderboard)}, "
                                f"elapsed={time.perf_counter() - stage_started_at:.1f}s"
                            )
                        continue

                    if open_price is not None and open_price > 0:
                        pct_change = (item['last_price'] / open_price - 1) * 100
                        leaderboard.append({
                            'symbol': item['symbol'],
                            'change': pct_change,
                            'volume': item['quote_volume'],
                            'last_price': item['last_price'],
                        })

                    if processed % progress_step == 0 or processed == total_candidates:
                        logger.info(
                            "晨间涨幅榜进度: "
                            f"{processed}/{total_candidates}, "
                            f"effective={len(leaderboard)}, "
                            f"elapsed={time.perf_counter() - stage_started_at:.1f}s"
                        )

        leaderboard.sort(key=lambda x: x['change'], reverse=True)
        top_list = leaderboard[:self.leaderboard_top_n]
        losers_list = sorted(leaderboard, key=lambda x: x['change'])[:self.leaderboard_top_n]

        snapshot = {
            "snapshot_date": datetime.now(UTC8).strftime('%Y-%m-%d'),
            "snapshot_time": datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S'),
            "window_start_utc": midnight_utc.strftime('%Y-%m-%d %H:%M:%S'),
            "candidates": len(candidates),
            "effective": len(leaderboard),
            "top": len(top_list),
            "rows": top_list,
            "losers_rows": losers_list,
            "all_rows": leaderboard,
        }
        logger.info(
            "晨间涨幅榜快照构建完成: "
            f"candidates={snapshot['candidates']}, "
            f"effective={snapshot['effective']}, "
            f"top={snapshot['top']}, "
            f"elapsed={time.perf_counter() - stage_started_at:.1f}s"
        )
        return snapshot

    def get_top_gainers_snapshot(self, source: str = "涨幅榜接口"):
        """获取涨幅榜快照（带冷却与互斥保护），供API或任务复用。"""
        if not self.processor:
            return {"ok": False, "reason": "api_keys_missing", "message": "API密钥未配置"}
        if self._is_api_cooldown_active(source=source):
            return {"ok": False, "reason": "cooldown_active", "message": "Binance API处于冷却中"}
        if not self._try_enter_api_job_slot(source=source):
            return {"ok": False, "reason": "lock_busy", "message": "任务槽位繁忙"}

        try:
            snapshot = self._build_top_gainers_snapshot()
            if snapshot["top"] <= 0:
                return {"ok": False, "reason": "no_data", "message": "未生成有效榜单", **snapshot}
            return {"ok": True, **snapshot}
        except Exception as e:
            logger.error(f"{source}失败: {e}")
            return {"ok": False, "reason": "exception", "message": str(e)}
        finally:
            self._release_api_job_slot()

    def send_morning_top_gainers(self):
        """每天早上发送币安合约涨跌幅榜（按UTC当日开盘到当前涨跌幅）"""
        started_at = time.perf_counter()
        logger.info(
            "晨间涨幅榜任务开始执行: "
            f"schedule={self.leaderboard_alert_hour:02d}:{self.leaderboard_alert_minute:02d}"
        )
        result = self.get_top_gainers_snapshot(source="晨间涨幅榜")
        logger.info(
            "晨间涨幅榜快照结果: "
            f"ok={result.get('ok')}, "
            f"reason={result.get('reason', '')}, "
            f"candidates={result.get('candidates', 0)}, "
            f"effective={result.get('effective', 0)}, "
            f"top={result.get('top', 0)}"
        )
        if not result.get("ok"):
            logger.warning(
                f"晨间涨幅榜任务跳过: reason={result.get('reason')}, message={result.get('message', '')}"
            )
            return

        try:
            self.db.save_leaderboard_snapshot(result)
            logger.info(
                f"涨幅榜快照已保存: date={result.get('snapshot_date')}, top={result.get('top')}"
            )
        except Exception as e:
            logger.error(f"保存涨幅榜快照失败: {e}")

        try:
            metrics_payload = self.db.upsert_leaderboard_daily_metrics_for_date(
                str(result.get("snapshot_date"))
            )
            if metrics_payload:
                logger.info(
                    "涨跌幅指标已保存: "
                    f"date={result.get('snapshot_date')}, "
                    f"m1={metrics_payload.get('metric1', {}).get('probability_pct')}, "
                    f"m2={metrics_payload.get('metric2', {}).get('probability_pct')}, "
                    f"m3_eval={metrics_payload.get('metric3', {}).get('evaluated_count')}"
                )
        except Exception as e:
            logger.error(f"保存涨跌幅指标失败: {e}")

        title = f"【币安合约市场涨跌幅榜 Top {result['top']}】"
        content = (
            "### 币安合约市场晨间涨跌幅榜\n\n"
            f"**更新时间:** {result['snapshot_time']} (UTC+8)\n"
            f"**计算区间:** {result['window_start_utc']} UTC 至当前\n\n"
            "#### 涨幅榜 Top10\n\n"
            "| 排名 | 币种 | 涨幅 | 24h成交额 |\n"
            "|:---:|:---:|:---:|:---:|\n"
        )

        for i, row in enumerate(result["rows"], start=1):
            symbol = row['symbol']
            change = f"{row['change']:.2f}%"
            volume = f"{int(row['volume'] / 1_000_000)}M"
            content += f"| {i} | {symbol} | {change} | {volume} |\n"

        losers_rows = result.get("losers_rows", [])
        if losers_rows:
            content += (
                "\n#### 跌幅榜 Top10\n\n"
                "| 排名 | 币种 | 跌幅 | 24h成交额 |\n"
                "|:---:|:---:|:---:|:---:|\n"
            )
            for i, row in enumerate(losers_rows, start=1):
                symbol = row['symbol']
                change = f"{row['change']:.2f}%"
                volume = f"{int(row['volume'] / 1_000_000)}M"
                content += f"| {i} | {symbol} | {change} | {volume} |\n"

        send_server_chan_notification(title, content)
        logger.info(
            "晨间涨幅榜已发送: "
            f"candidates={result['candidates']}, "
            f"effective={result['effective']}, "
            f"top={result['top']}, "
            f"losers_top={len(result.get('losers_rows', []))}, "
            f"elapsed={time.perf_counter() - started_at:.2f}s"
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
        """构建反弹幅度榜快照（不处理锁与冷却）。"""
        stage_started_at = time.perf_counter()
        now_utc = datetime.now(timezone.utc)
        window_start_utc = now_utc - timedelta(days=window_days)

        exchange_info = self.processor.get_exchange_info(client=self.processor.client)
        if not exchange_info or 'symbols' not in exchange_info:
            raise RuntimeError("无法获取 exchangeInfo")

        usdt_perpetual_symbols = {
            item.get('symbol')
            for item in exchange_info.get('symbols', [])
            if (
                item.get('contractType') == 'PERPETUAL'
                and item.get('quoteAsset') == 'USDT'
                and str(item.get('status', '')).upper() == 'TRADING'
            )
        }
        if not usdt_perpetual_symbols:
            raise RuntimeError("无可用USDT永续交易对")

        ticker_data = self.processor.client.public_get('/fapi/v1/ticker/price')
        if not ticker_data:
            raise RuntimeError("无法获取 ticker/price")
        if isinstance(ticker_data, dict):
            ticker_data = [ticker_data]

        candidates = []
        for item in ticker_data:
            symbol = item.get('symbol')
            if not symbol or symbol not in usdt_perpetual_symbols:
                continue
            try:
                current_price = float(item.get('price', 0.0))
            except (TypeError, ValueError):
                continue
            if current_price <= 0:
                continue
            candidates.append({
                'symbol': symbol,
                'current_price': current_price,
            })

        # 稳定排序，确保同等条件下输出一致
        candidates.sort(key=lambda x: x['symbol'])
        logger.info(
            f"{label}候选统计: "
            f"candidates={len(candidates)}, "
            f"top_n={top_n}"
        )

        rebound_rows = []
        progress_step = 20
        total_candidates = len(candidates)
        if total_candidates > 0:
            min_interval = max(0.05, float(os.getenv("BINANCE_MIN_REQUEST_INTERVAL", "0.3")))
            per_worker_rpm = max(1.0, 60.0 / min_interval)
            workers_by_budget = max(1, int(weight_budget_per_minute // per_worker_rpm))
            worker_count = min(total_candidates, kline_workers, workers_by_budget)
            estimated_peak_weight_per_min = int(worker_count * per_worker_rpm)
            estimated_total_weight = 1 + 1 + total_candidates  # exchangeInfo + ticker/price + per-symbol klines
            logger.info(
                f"{label}并发计划: "
                f"workers={worker_count}, "
                f"min_interval={min_interval:.2f}s, "
                f"budget={weight_budget_per_minute}/min, "
                f"est_peak={estimated_peak_weight_per_min}/min, "
                f"est_total_weight={estimated_total_weight}"
            )

            thread_local = threading.local()
            metric_field = f"rebound_{window_days}d_pct"
            low_field = f"low_{window_days}d"
            low_time_field = f"low_{window_days}d_at_utc"
            kline_limit = max(14, int(window_days))

            def _kline_task(item: dict):
                if self._is_api_cooldown_active(source=f'{label}-逐币种计算'):
                    return item, None

                worker_client = getattr(thread_local, "client", None)
                if worker_client is None:
                    worker_client = self.processor._create_worker_client()
                    thread_local.client = worker_client

                try:
                    klines = worker_client.public_get('/fapi/v1/klines', {
                        'symbol': item['symbol'],
                        'interval': '1d',
                        'limit': kline_limit
                    }) or []
                except Exception:
                    return item, None

                lows = []
                for kline in klines:
                    if not isinstance(kline, list) or len(kline) < 4:
                        continue
                    try:
                        low_price = float(kline[3])
                        open_time = int(kline[0])
                    except (TypeError, ValueError):
                        continue
                    if low_price <= 0:
                        continue
                    lows.append((low_price, open_time))

                if not lows:
                    return item, None

                low_price, low_ts = min(lows, key=lambda entry: entry[0])
                rebound_pct = (item['current_price'] / low_price - 1.0) * 100.0
                low_at_utc = datetime.fromtimestamp(
                    low_ts / 1000, tz=timezone.utc
                ).strftime('%Y-%m-%d %H:%M:%S')
                return item, {
                    low_field: low_price,
                    low_time_field: low_at_utc,
                    metric_field: rebound_pct,
                }

            processed = 0
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                futures = [executor.submit(_kline_task, item) for item in candidates]
                for future in as_completed(futures):
                    processed += 1
                    try:
                        item, payload = future.result()
                    except Exception as exc:
                        logger.warning(f"{label}逐币种计算异常: {exc}")
                        payload = None
                        item = None

                    if item and payload:
                        rebound_rows.append({
                            'symbol': item['symbol'],
                            'current_price': item['current_price'],
                            low_field: payload[low_field],
                            low_time_field: payload[low_time_field],
                            metric_field: payload[metric_field],
                        })

                    if processed % progress_step == 0 or processed == total_candidates:
                        logger.info(
                            f"{label}进度: "
                            f"{processed}/{total_candidates}, "
                            f"effective={len(rebound_rows)}, "
                            f"elapsed={time.perf_counter() - stage_started_at:.1f}s"
                        )

        metric_field = f"rebound_{window_days}d_pct"
        rebound_rows.sort(key=lambda x: x[metric_field], reverse=True)
        top_list = rebound_rows[:top_n]

        snapshot = {
            "snapshot_date": datetime.now(UTC8).strftime('%Y-%m-%d'),
            "snapshot_time": datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S'),
            "window_start_utc": window_start_utc.strftime('%Y-%m-%d %H:%M:%S'),
            "candidates": len(candidates),
            "effective": len(rebound_rows),
            "top": len(top_list),
            "rows": top_list,
            "all_rows": rebound_rows,
        }
        logger.info(
            f"{label}快照构建完成: "
            f"candidates={snapshot['candidates']}, "
            f"effective={snapshot['effective']}, "
            f"top={snapshot['top']}, "
            f"elapsed={time.perf_counter() - stage_started_at:.1f}s"
        )
        return snapshot

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
        if not self.processor:
            return {"ok": False, "reason": "api_keys_missing", "message": "API密钥未配置"}
        if self._is_api_cooldown_active(source=source):
            return {"ok": False, "reason": "cooldown_active", "message": "Binance API处于冷却中"}
        if not self._try_enter_api_job_slot(source=source):
            return {"ok": False, "reason": "lock_busy", "message": "任务槽位繁忙"}

        try:
            snapshot = self._build_rebound_7d_snapshot()
            if snapshot["top"] <= 0:
                return {"ok": False, "reason": "no_data", "message": "未生成有效榜单", **snapshot}
            return {"ok": True, **snapshot}
        except Exception as e:
            logger.error(f"{source}失败: {e}")
            return {"ok": False, "reason": "exception", "message": str(e)}
        finally:
            self._release_api_job_slot()

    def snapshot_morning_rebound_7d(self):
        """每天早上07:30生成14D反弹幅度Top榜快照并入库。"""
        started_at = time.perf_counter()
        logger.info(
            "晨间14D反弹榜任务开始执行: "
            f"schedule={self.rebound_7d_hour:02d}:{self.rebound_7d_minute:02d}"
        )
        result = self.get_rebound_7d_snapshot(source="晨间14D反弹榜")
        logger.info(
            "晨间14D反弹榜快照结果: "
            f"ok={result.get('ok')}, "
            f"reason={result.get('reason', '')}, "
            f"candidates={result.get('candidates', 0)}, "
            f"effective={result.get('effective', 0)}, "
            f"top={result.get('top', 0)}"
        )
        if not result.get("ok"):
            logger.warning(
                f"晨间14D反弹榜任务跳过: reason={result.get('reason')}, message={result.get('message', '')}"
            )
            return

        try:
            self.db.save_rebound_7d_snapshot(result)
            logger.info(
                f"14D反弹榜快照已保存: date={result.get('snapshot_date')}, top={result.get('top')}"
            )
        except Exception as e:
            logger.error(f"保存14D反弹榜快照失败: {e}")

        logger.info(
            "晨间14D反弹榜任务完成: "
            f"elapsed={time.perf_counter() - started_at:.2f}s"
        )

    def get_rebound_30d_snapshot(self, source: str = "30D反弹榜接口"):
        """获取30D反弹榜快照（带冷却与互斥保护），供API或任务复用。"""
        if not self.processor:
            return {"ok": False, "reason": "api_keys_missing", "message": "API密钥未配置"}
        if self._is_api_cooldown_active(source=source):
            return {"ok": False, "reason": "cooldown_active", "message": "Binance API处于冷却中"}
        if not self._try_enter_api_job_slot(source=source):
            return {"ok": False, "reason": "lock_busy", "message": "任务槽位繁忙"}

        try:
            snapshot = self._build_rebound_30d_snapshot()
            if snapshot["top"] <= 0:
                return {"ok": False, "reason": "no_data", "message": "未生成有效榜单", **snapshot}
            return {"ok": True, **snapshot}
        except Exception as e:
            logger.error(f"{source}失败: {e}")
            return {"ok": False, "reason": "exception", "message": str(e)}
        finally:
            self._release_api_job_slot()

    def snapshot_morning_rebound_30d(self):
        """每天早上生成30D反弹幅度Top榜快照并入库。"""
        started_at = time.perf_counter()
        logger.info(
            "晨间30D反弹榜任务开始执行: "
            f"schedule={self.rebound_30d_hour:02d}:{self.rebound_30d_minute:02d}"
        )
        result = self.get_rebound_30d_snapshot(source="晨间30D反弹榜")
        logger.info(
            "晨间30D反弹榜快照结果: "
            f"ok={result.get('ok')}, "
            f"reason={result.get('reason', '')}, "
            f"candidates={result.get('candidates', 0)}, "
            f"effective={result.get('effective', 0)}, "
            f"top={result.get('top', 0)}"
        )
        if not result.get("ok"):
            logger.warning(
                f"晨间30D反弹榜任务跳过: reason={result.get('reason')}, message={result.get('message', '')}"
            )
            return

        try:
            self.db.save_rebound_30d_snapshot(result)
            logger.info(
                f"30D反弹榜快照已保存: date={result.get('snapshot_date')}, top={result.get('top')}"
            )
        except Exception as e:
            logger.error(f"保存30D反弹榜快照失败: {e}")

        logger.info(
            "晨间30D反弹榜任务完成: "
            f"elapsed={time.perf_counter() - started_at:.2f}s"
        )

    def get_rebound_60d_snapshot(self, source: str = "60D反弹榜接口"):
        """获取60D反弹榜快照（带冷却与互斥保护），供API或任务复用。"""
        if not self.processor:
            return {"ok": False, "reason": "api_keys_missing", "message": "API密钥未配置"}
        if self._is_api_cooldown_active(source=source):
            return {"ok": False, "reason": "cooldown_active", "message": "Binance API处于冷却中"}
        if not self._try_enter_api_job_slot(source=source):
            return {"ok": False, "reason": "lock_busy", "message": "任务槽位繁忙"}

        try:
            snapshot = self._build_rebound_60d_snapshot()
            if snapshot["top"] <= 0:
                return {"ok": False, "reason": "no_data", "message": "未生成有效榜单", **snapshot}
            return {"ok": True, **snapshot}
        except Exception as e:
            logger.error(f"{source}失败: {e}")
            return {"ok": False, "reason": "exception", "message": str(e)}
        finally:
            self._release_api_job_slot()

    def snapshot_morning_rebound_60d(self):
        """每天早上生成60D反弹幅度Top榜快照并入库。"""
        started_at = time.perf_counter()
        logger.info(
            "晨间60D反弹榜任务开始执行: "
            f"schedule={self.rebound_60d_hour:02d}:{self.rebound_60d_minute:02d}"
        )
        result = self.get_rebound_60d_snapshot(source="晨间60D反弹榜")
        logger.info(
            "晨间60D反弹榜快照结果: "
            f"ok={result.get('ok')}, "
            f"reason={result.get('reason', '')}, "
            f"candidates={result.get('candidates', 0)}, "
            f"effective={result.get('effective', 0)}, "
            f"top={result.get('top', 0)}"
        )
        if not result.get("ok"):
            logger.warning(
                f"晨间60D反弹榜任务跳过: reason={result.get('reason')}, message={result.get('message', '')}"
            )
            return

        try:
            self.db.save_rebound_60d_snapshot(result)
            logger.info(
                f"60D反弹榜快照已保存: date={result.get('snapshot_date')}, top={result.get('top')}"
            )
        except Exception as e:
            logger.error(f"保存60D反弹榜快照失败: {e}")

        logger.info(
            "晨间60D反弹榜任务完成: "
            f"elapsed={time.perf_counter() - started_at:.2f}s"
        )

    @staticmethod
    def _normalize_futures_symbol(symbol: str) -> str:
        """将库内symbol规范化为Binance USDT交易对symbol"""
        sym = str(symbol or "").upper().strip()
        if not sym:
            return sym
        if sym.endswith("USDT") or sym.endswith("BUSD"):
            return sym
        return f"{sym}USDT"

    def sync_balance_data(self):
        """同步账户余额数据到数据库"""
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
                    latest_event_time_ms = self.db.get_latest_transfer_event_time()
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
                        inserted = self.db.save_transfer_income(
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
                self.db.save_balance_history(current_margin, current_wallet)
                logger.info(f"余额已更新: {current_margin:.2f} USDT (Wallet: {current_wallet:.2f})")
            else:
                logger.warning("获取余额失败，balance为 None")
        except Exception as e:
            logger.error(f"同步余额失败: {str(e)}")
        finally:
            self._release_api_job_slot()

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
