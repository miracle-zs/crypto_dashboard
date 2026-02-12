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
import threading
from dotenv import load_dotenv
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.trade_processor import TradeDataProcessor
from app.database import Database
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
        self.start_date = os.getenv('START_DATE')  # 自定义起始日期
        self.end_date = os.getenv('END_DATE')      # 自定义结束日期
        self.sync_lookback_minutes = _env_int('SYNC_LOOKBACK_MINUTES', 1440, minimum=1)
        self.use_time_filter = os.getenv('SYNC_USE_TIME_FILTER', '1').lower() in ('1', 'true', 'yes')
        self.enable_user_stream = os.getenv('ENABLE_USER_STREAM', '0').lower() in ('1', 'true', 'yes')
        self.force_full_sync = os.getenv('FORCE_FULL_SYNC', '0').lower() in ('1', 'true', 'yes')
        self.enable_leaderboard_alert = os.getenv('ENABLE_LEADERBOARD_ALERT', '1').lower() in ('1', 'true', 'yes')
        self.leaderboard_top_n = _env_int('LEADERBOARD_TOP_N', 10, minimum=1)
        self.leaderboard_min_quote_volume = _env_float('LEADERBOARD_MIN_QUOTE_VOLUME', 50_000_000, minimum=0.0)
        self.leaderboard_max_symbols = _env_int('LEADERBOARD_MAX_SYMBOLS', 120, minimum=0)
        self.leaderboard_alert_hour = _env_int('LEADERBOARD_ALERT_HOUR', 7, minimum=0)
        self.leaderboard_alert_minute = _env_int('LEADERBOARD_ALERT_MINUTE', 40, minimum=0)
        self.leaderboard_alert_hour %= 24
        self.leaderboard_alert_minute %= 60
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
        window_start = leaderboard_dt - timedelta(minutes=2)
        window_end = leaderboard_dt + timedelta(minutes=5)
        return window_start <= now <= window_end

    def sync_trades_data(self):
        """同步交易数据到数据库"""
        if not self.processor:
            logger.warning("无法同步: API密钥未配置")
            return
        if self._is_leaderboard_guard_window():
            logger.info(
                "跳过交易同步: 位于晨间涨幅榜保护窗口内 "
                f"({self.leaderboard_alert_hour:02d}:{self.leaderboard_alert_minute:02d} 前2分钟至后5分钟)"
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
            logger.info("开始同步交易数据...")
            if self.force_full_sync:
                logger.info("同步策略: FORCE_FULL_SYNC=ON (始终走全量模式)")
            elif self.start_date:
                logger.info("同步策略: START_DATE 全量模式")
            else:
                logger.info("同步策略: 增量模式(带回溯窗口)")

            # 更新同步状态为进行中
            self.db.update_sync_status(status='syncing')

            # 获取最后一条交易时间（仅作参考，不再用于增量更新）
            # last_entry_time = self.db.get_last_entry_time()

            # 同步模式：
            # 1) 如果配置 START_DATE -> 全量
            # 2) 否则如果数据库已有最后入场时间 -> 增量(带回溯窗口)
            # 3) 否则 -> DAYS_TO_FETCH 天全量
            last_entry_time = self.db.get_last_entry_time()
            if self.force_full_sync:
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
            elif self.start_date:
                # 使用自定义起始日期
                try:
                    start_dt = datetime.strptime(self.start_date, '%Y-%m-%d').replace(tzinfo=UTC8)
                    start_dt = start_dt.replace(hour=23, minute=0, second=0, microsecond=0)
                    since = int(start_dt.timestamp() * 1000)
                    logger.info(f"全量更新模式 - 从自定义日期 {self.start_date} 开始")
                except ValueError as e:
                    logger.error(f"日期格式错误: {e}，使用默认DAYS_TO_FETCH")
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
                # 使用DAYS_TO_FETCH
                logger.info(f"全量更新模式 - 获取最近 {self.days_to_fetch} 天数据")
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
            df = self.processor.analyze_orders(
                since=since,
                until=until,
                traded_symbols=traded_symbols,
                use_time_filter=self.use_time_filter
            )
            analyze_elapsed = time.perf_counter() - stage_started
            logger.info(f"闭仓ETL完成: rows={len(df)}, elapsed={analyze_elapsed:.2f}s")

            if df.empty:
                logger.info("没有新数据需要更新")
            else:
                # 保存到数据库
                # 如果是全量更新模式（start_date 或无 last_entry_time），建议使用覆盖模式防止重复
                # 这里简单起见，只要有新数据计算出来，我们就认为这批数据是最新的真理
                # 尤其是当重新计算了历史盈亏时，覆盖旧数据是必须的
                is_full_sync = self.force_full_sync or self.start_date is not None or self.db.get_last_entry_time() is None

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

            # 同步未平仓订单
            logger.info("同步未平仓订单...")
            stage_started = time.perf_counter()
            open_positions = self.processor.get_open_positions(since, until, traded_symbols=traded_symbols)
            if open_positions is None:
                logger.warning("未平仓同步跳过：PositionRisk请求失败，保留数据库现有持仓")
            elif open_positions:
                open_count = self.db.save_open_positions(open_positions)
                open_saved = open_count
                logger.info(f"保存 {open_count} 条未平仓订单")
            else:
                # 清空未平仓记录（如果没有未平仓订单）
                self.db.save_open_positions([])
                logger.info("当前无未平仓订单")
            open_positions_elapsed = time.perf_counter() - stage_started

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
            import traceback
            logger.error(traceback.format_exc())
        finally:
            self._release_api_job_slot()

    def check_long_held_positions(self):
        """检查持仓时间超过48小时的订单并发送合并通知 (每24小时复提)"""
        try:
            positions = self.db.get_open_positions()
            now = datetime.now(UTC8)
            now_utc = datetime.now(timezone.utc)
            stale_positions = []

            for pos in positions:
                # 跳过用户标记为长期持仓的订单
                if pos.get('is_long_term'):
                    continue

                entry_time_str = pos['entry_time']
                try:
                    entry_dt = datetime.strptime(entry_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC8)
                except ValueError:
                    logger.warning(f"无法解析时间: {entry_time_str}")
                    continue

                duration = now - entry_dt

                # 48小时 = 48 * 3600 秒
                if duration.total_seconds() > 48 * 3600:
                    should_alert = False

                    # 检查是否需要报警
                    if pos.get('alerted', 0) == 0:
                        should_alert = True
                    else:
                        # 如果已报警，检查距离上次报警是否超过24小时
                        last_alert_str = pos.get('last_alert_time')
                        if last_alert_str:
                            try:
                                # SQLite CURRENT_TIMESTAMP 是 UTC 时间
                                last_alert_dt = datetime.strptime(last_alert_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc)
                                time_since_last = now_utc - last_alert_dt
                                if time_since_last.total_seconds() > 24 * 3600:
                                    should_alert = True
                            except ValueError:
                                # 解析失败，为安全起见再次报警
                                should_alert = True
                        else:
                            # 有alerted标志但无时间，视为需要更新
                            should_alert = True

                    if should_alert:
                        hours = int(duration.total_seconds() / 3600)
                        pos['hours_held'] = hours

                        # 获取实时价格计算浮盈
                        try:
                            symbol_for_quote = self._normalize_futures_symbol(pos['symbol'])
                            ticker = self.processor.client.public_get('/fapi/v1/ticker/price', {'symbol': symbol_for_quote})
                            if ticker and ticker.get('price') is not None:
                                current_price = float(ticker['price'])
                                entry_price = float(pos['entry_price'])
                                qty = float(pos['qty'])
                                side = pos['side']

                                if side == 'LONG':
                                    pnl = (current_price - entry_price) * qty
                                else:
                                    pnl = (entry_price - current_price) * qty

                                pos['current_pnl'] = pnl
                                pos['current_price'] = current_price
                            else:
                                pos['current_pnl'] = None
                                pos['current_price'] = None

                        except Exception as e:
                            logger.warning(f"获取实时价格失败: {e}")
                            pos['current_pnl'] = None
                            pos['current_price'] = None

                        stale_positions.append(pos)

            if stale_positions:
                count = len(stale_positions)
                title = f"⚠️ 持仓超时告警: {count}个订单"

                content = f"监测到 **{count}** 个订单持仓超过 48 小时 (复提周期: 24h)。\n\n"
                content += "--- \n"

                for pos in stale_positions:
                    pnl_str = "N/A"
                    if pos.get('current_pnl') is not None:
                        pnl_val = pos['current_pnl']
                        emoji = "🟢" if pnl_val >= 0 else "🔴"
                        pnl_str = f"{emoji} {pnl_val:+.2f} U"
                    current_price = pos.get('current_price')
                    current_price_str = f"{current_price:.6g}" if current_price is not None else "--"

                    content += (
                        f"**{pos['symbol']}** ({pos['side']})\n"
                        f"- 盈亏: {pnl_str}\n"
                        f"- 时长: {pos['hours_held']} 小时\n"
                        f"- 开仓: {pos['entry_price']}\n"
                        f"- 现价: {current_price_str}\n\n"
                    )

                content += "请关注风险，及时处理。"

                send_server_chan_notification(title, content)

                # 批量标记为已通知
                for pos in stale_positions:
                    self.db.set_position_alerted(pos['symbol'], pos['order_id'])
                    logger.info(f"已发送持仓超时告警: {pos['symbol']} ({pos['hours_held']}h)")

        except Exception as e:
            logger.error(f"检查持仓超时失败: {e}")

    def check_risk_before_sleep(self):
        """每晚11点检查持仓风险"""
        try:
            positions = self.db.get_open_positions()
            # 统计持仓币种数量 (去重)
            unique_symbols = set(p['symbol'] for p in positions)
            count = len(unique_symbols)

            if count > 5:
                title = f"🌙 睡前风控提醒: 持仓过重 ({count}个)"
                content = (
                    f"当前持有 **{count}** 个币种，超过建议的 5 个。\n\n"
                    f"**持仓列表**:\n"
                    f"{', '.join(sorted(unique_symbols))}\n\n"
                    f"建议睡前检查风险，考虑减仓或设置止损。"
                )
                send_server_chan_notification(title, content)
                logger.info(f"已发送睡前风控提醒: 持仓 {count} 个币种")
            else:
                logger.info(f"睡前风控检查通过: 持仓 {count} 个币种")

        except Exception as e:
            logger.error(f"睡前风控检查失败: {e}")

    def check_recent_losses_at_noon(self):
        """每天中午11:50检查24小时内开仓且当前浮亏的订单"""
        try:
            positions = self.db.get_open_positions()
            now = datetime.now(UTC8)
            loss_positions = []

            for pos in positions:
                # 跳过长期持仓
                if pos.get('is_long_term'):
                    continue

                entry_time_str = pos['entry_time']
                try:
                    entry_dt = datetime.strptime(entry_time_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=UTC8)
                except ValueError:
                    continue

                # 检查是否在24小时内开仓
                if (now - entry_dt).total_seconds() <= 24 * 3600:
                    # 获取实时价格计算浮盈
                    try:
                        symbol_for_quote = self._normalize_futures_symbol(pos['symbol'])
                        ticker = self.processor.client.public_get('/fapi/v1/ticker/price', {'symbol': symbol_for_quote})
                        if ticker and ticker.get('price') is not None:
                            current_price = float(ticker['price'])
                            entry_price = float(pos['entry_price'])
                            qty = float(pos['qty'])
                            side = pos['side']

                            if side == 'LONG':
                                pnl = (current_price - entry_price) * qty
                            else:
                                pnl = (entry_price - current_price) * qty

                            pos['current_pnl'] = pnl
                            pos['current_price'] = current_price

                            # 如果浮亏
                            if pnl < 0:
                                loss_positions.append(pos)
                    except Exception as e:
                        logger.warning(f"获取实时价格失败: {e}")

            if loss_positions:
                count = len(loss_positions)
                title = f"⚠️ 午间浮亏警报: {count}个新订单"
                content = f"北京时间 11:50 监测到 **{count}** 个24小时内开仓的订单出现浮亏。\n\n"
                content += "--- \n"

                # 按亏损金额排序 (从小到大，即亏损最多的在前)
                loss_positions.sort(key=lambda x: x['current_pnl'])

                for pos in loss_positions:
                    pnl_val = pos['current_pnl']
                    current_price = pos.get('current_price')
                    current_price_str = f"{current_price:.6g}" if current_price is not None else "--"
                    content += (
                        f"**{pos['symbol']}** ({pos['side']})\n"
                        f"- 浮亏: 🔴 {pnl_val:.2f} U\n"
                        f"- 开仓: {pos['entry_price']}\n"
                        f"- 现价: {current_price_str}\n"
                        f"- 时间: {pos['entry_time']}\n\n"
                    )

                content += "请及时关注风险。"
                send_server_chan_notification(title, content)
                logger.info(f"已发送午间浮亏提醒: {count} 个订单")

        except Exception as e:
            logger.error(f"午间风控检查失败: {e}")

    def _build_top_gainers_snapshot(self):
        """构建涨跌幅榜快照（不处理锁与冷却）。"""
        now_utc = datetime.now(timezone.utc)
        midnight_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        midnight_utc_ms = int(midnight_utc.timestamp() * 1000)

        exchange_info = self.processor.get_exchange_info(client=self.processor.client)
        if not exchange_info or 'symbols' not in exchange_info:
            raise RuntimeError("无法获取 exchangeInfo")

        usdt_perpetual_symbols = {
            item.get('symbol')
            for item in exchange_info.get('symbols', [])
            if item.get('contractType') == 'PERPETUAL' and item.get('quoteAsset') == 'USDT'
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

        leaderboard = []
        for item in candidates:
            if self._is_api_cooldown_active(source='涨幅榜-逐币种计算'):
                break

            symbol = item['symbol']
            open_price = self.processor.get_price_change_from_utc_start(
                symbol=symbol,
                timestamp=midnight_utc_ms,
                client=self.processor.client
            )
            if open_price is None or open_price <= 0:
                continue

            pct_change = (item['last_price'] / open_price - 1) * 100
            leaderboard.append({
                'symbol': symbol,
                'change': pct_change,
                'volume': item['quote_volume'],
                'last_price': item['last_price'],
            })

        leaderboard.sort(key=lambda x: x['change'], reverse=True)
        top_list = leaderboard[:self.leaderboard_top_n]
        losers_list = sorted(leaderboard, key=lambda x: x['change'])[:self.leaderboard_top_n]

        return {
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
        result = self.get_top_gainers_snapshot(source="晨间涨幅榜")
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
            f"losers_top={len(result.get('losers_rows', []))}"
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

                # --- 自动检测出入金逻辑 ---
                try:
                    logger.info("开始检测出入金...")
                    # 获取最近一条记录进行对比
                    history = self.db.get_balance_history(limit=1)
                    if history:
                        last_record = history[0]
                        # 只有当上一条记录也有wallet_balance时才进行对比
                        # 注意：数据库中新加的列默认为0，需排除0的情况(除非真的破产)或根据逻辑判断
                        last_wallet = last_record.get('wallet_balance', 0)
                        last_ts_str = last_record.get('timestamp')

                        if last_wallet > 0:
                            # 解析时间 (兼容带微秒和不带微秒的格式)
                            try:
                                last_ts = datetime.strptime(last_ts_str, '%Y-%m-%d %H:%M:%S')
                            except ValueError:
                                # 尝试解析带微秒的格式
                                try:
                                    last_ts = datetime.strptime(last_ts_str, '%Y-%m-%d %H:%M:%S.%f')
                                except ValueError:
                                    logger.warning(f"无法解析时间戳格式: {last_ts_str}")
                                    raise ValueError("Invalid timestamp format")

                            # 转为毫秒时间戳 (视为UTC)
                            last_ts = last_ts.replace(tzinfo=timezone.utc)
                            last_ts_ms = int(last_ts.timestamp() * 1000)

                            # 1. 计算钱包余额变化
                            wallet_diff = current_wallet - last_wallet

                            # 2. 获取该时间段内的交易资金流 (PnL + Fees)
                            # 额外往前多取1秒，防止边界遗漏
                            trading_flow = self.processor.get_recent_financial_flow(start_time=last_ts_ms - 1000)

                            # 3. 计算"无法解释的差额" (疑似出入金)
                            transfer_est = wallet_diff - trading_flow

                            # 4. 阈值判断 (> 1000 USDT)
                            if abs(transfer_est) > 1000:
                                logger.warning(f"监测到资金异动: 钱包变动 {wallet_diff:.2f}, 交易流 {trading_flow:.2f}, 差额 {transfer_est:.2f}")
                                self.db.save_transfer(amount=transfer_est, type='auto', description="Auto-detected > 1000U")
                            else:
                                logger.info(f"未发现明显出入金: 差额 {transfer_est:.2f}")

                except Exception as e:
                    logger.warning(f"出入金检测出错: {e}")

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
        self.scheduler.add_job(self.sync_trades_data, 'date')
        self.scheduler.add_job(self.sync_balance_data, 'date')

        # 添加定时任务 - 每隔N分钟执行一次
        self.scheduler.add_job(
            func=self.sync_trades_data,
            trigger=IntervalTrigger(minutes=self.update_interval_minutes),
            id='sync_trades',
            name='同步交易数据',
            max_instances=1,
            coalesce=True,
            misfire_grace_time=120,
            replace_existing=True
        )

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

        # 添加午间浮亏检查任务 - 每天 11:50 (UTC+8) 执行
        self.scheduler.add_job(
            func=self.check_recent_losses_at_noon,
            trigger=CronTrigger(hour=11, minute=50, timezone=UTC8),
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

        self.scheduler.start()
        logger.info(f"交易数据同步任务已启动: 每 {self.update_interval_minutes} 分钟自动更新一次")
        logger.info("余额监控任务已启动: 每 1 分钟自动更新一次")
        logger.info("睡前风控检查已启动: 每天 23:00 执行")

    def stop(self):
        """停止定时任务"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("定时任务已停止")

    def get_next_run_time(self):
        """获取下次运行时间"""
        job = self.scheduler.get_job('sync_trades')
        if job:
            return job.next_run_time
        return None


# 全局实例
scheduler_instance = None


def get_scheduler() -> TradeDataScheduler:
    """获取调度器单例"""
    global scheduler_instance
    if scheduler_instance is None:
        scheduler_instance = TradeDataScheduler()
    return scheduler_instance
