"""
定时任务调度器 - 自动更新交易数据
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os
from dotenv import load_dotenv
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from trade_analyzer import BinanceOrderAnalyzer
from app.database import Database
from app.logger import logger
from app.notifier import send_server_chan_notification

load_dotenv()

# 定义UTC+8时区
UTC8 = ZoneInfo("Asia/Shanghai")


class TradeDataScheduler:
    """交易数据定时更新调度器"""

    def __init__(self):
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
            self.analyzer = None
        else:
            self.analyzer = BinanceOrderAnalyzer(api_key, api_secret)

        self.days_to_fetch = int(os.getenv('DAYS_TO_FETCH', 30))
        self.update_interval_minutes = int(os.getenv('UPDATE_INTERVAL_MINUTES', 10))
        self.start_date = os.getenv('START_DATE')  # 自定义起始日期
        self.end_date = os.getenv('END_DATE')      # 自定义结束日期
        self.sync_lookback_minutes = int(os.getenv('SYNC_LOOKBACK_MINUTES', 1440))
        self.use_time_filter = os.getenv('SYNC_USE_TIME_FILTER', '1').lower() in ('1', 'true', 'yes')
        self.enable_user_stream = os.getenv('ENABLE_USER_STREAM', '0').lower() in ('1', 'true', 'yes')

    def sync_trades_data(self):
        """同步交易数据到数据库"""
        if not self.analyzer:
            logger.warning("无法同步: API密钥未配置")
            return

        try:
            logger.info("=" * 50)
            logger.info("开始同步交易数据...")

            # 更新同步状态为进行中
            self.db.update_sync_status(status='syncing')

            # 获取最后一条交易时间（仅作参考，不再用于增量更新）
            # last_entry_time = self.db.get_last_entry_time()

            # 同步模式：
            # 1) 如果配置 START_DATE -> 全量
            # 2) 否则如果数据库已有最后入场时间 -> 增量(带回溯窗口)
            # 3) 否则 -> DAYS_TO_FETCH 天全量
            last_entry_time = self.db.get_last_entry_time()
            if self.start_date:
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
            traded_symbols = self.analyzer.get_traded_symbols(since, until)
            df = self.analyzer.analyze_orders(
                since=since,
                until=until,
                traded_symbols=traded_symbols,
                use_time_filter=self.use_time_filter
            )

            if df.empty:
                logger.info("没有新数据需要更新")
            else:
                # 保存到数据库
                # 如果是全量更新模式（start_date 或无 last_entry_time），建议使用覆盖模式防止重复
                # 这里简单起见，只要有新数据计算出来，我们就认为这批数据是最新的真理
                # 尤其是当重新计算了历史盈亏时，覆盖旧数据是必须的
                is_full_sync = self.start_date is not None or self.db.get_last_entry_time() is None

                logger.info(f"保存 {len(df)} 条记录到数据库 (覆盖模式={is_full_sync})...")
                saved_count = self.db.save_trades(df, overwrite=is_full_sync)

                if saved_count > 0:
                    logger.info("检测到新平仓单，重算统计快照...")
                    self.db.recompute_trade_summary()

            # 同步未平仓订单
            logger.info("同步未平仓订单...")
            open_positions = self.analyzer.get_open_positions(since, until, traded_symbols=traded_symbols)
            if open_positions:
                open_count = self.db.save_open_positions(open_positions)
                logger.info(f"保存 {open_count} 条未平仓订单")
            else:
                # 清空未平仓记录（如果没有未平仓订单）
                self.db.save_open_positions([])
                logger.info("当前无未平仓订单")

            # 检查持仓超时告警
            self.check_long_held_positions()

            # 更新同步状态
            self.db.update_sync_status(status='idle')

            # 显示统计信息
            stats = self.db.get_statistics()
            logger.info("同步完成!")
            logger.info(f"数据库统计: 总交易数={stats['total_trades']}, 币种数={stats['unique_symbols']}")
            logger.info(f"时间范围: {stats['earliest_trade']} ~ {stats['latest_trade']}")
            logger.info("=" * 50)

        except Exception as e:
            error_msg = f"同步失败: {str(e)}"
            logger.error(error_msg)
            self.db.update_sync_status(status='error', error_message=error_msg)
            import traceback
            logger.error(traceback.format_exc())

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

                        # 获取实时标记价格计算浮盈
                        try:
                            # 注意：scheduler中没有public_rest实例，需临时创建或直接调analyzer的client
                            # 简单起见，这里复用analyzer的client，它有signed_get，也可以用来获取mark price
                            # /fapi/v1/premiumIndex?symbol=...
                            mark_price = pos.get('mark_price')
                            # 如果DB没存mark_price(目前没存)，尝试实时获取或估算
                            # 为了不阻塞主线程太多，这里尝试快速获取，如果拿不到就显示'--'
                            # 实际上在analyze_open_positions时已经拿过一次了，但没存进DB...
                            # 更好的方式是analyze时就把unrealized_pnl算好存进DB(目前只存了entry_price/qty)
                            # 既然现在无法轻易拿到实时pnl，我们临时调一次API获取最新价格

                            # 临时获取当前价格
                            ticker = self.analyzer.client.public_get('/fapi/v1/ticker/price', {'symbol': pos['symbol']})
                            if ticker:
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
                                pos['current_pnl'] = 0.0
                                pos['current_price'] = 0.0

                        except Exception as e:
                            logger.warning(f"获取实时价格失败: {e}")
                            pos['current_pnl'] = 0.0
                            pos['current_price'] = 0.0

                        stale_positions.append(pos)

            if stale_positions:
                count = len(stale_positions)
                title = f"⚠️ 持仓超时告警: {count}个订单"

                content = f"监测到 **{count}** 个订单持仓超过 48 小时 (复提周期: 24h)。\n\n"
                content += "--- \n"

                for pos in stale_positions:
                    pnl_str = "N/A"
                    if 'current_pnl' in pos:
                        pnl_val = pos['current_pnl']
                        emoji = "🟢" if pnl_val >= 0 else "🔴"
                        pnl_str = f"{emoji} {pnl_val:+.2f} U"

                    content += (
                        f"**{pos['symbol']}** ({pos['side']})\n"
                        f"- 盈亏: {pnl_str}\n"
                        f"- 时长: {pos['hours_held']} 小时\n"
                        f"- 开仓: {pos['entry_price']}\n"
                        f"- 现价: {pos.get('current_price', '--')}\n\n"
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

    def sync_balance_data(self):
        """同步账户余额数据到数据库"""
        if not self.analyzer:
            return  # 如果没有配置API密钥，则不执行

        try:
            logger.info("开始同步账户余额...")
            # balance_info returns {'margin_balance': float, 'wallet_balance': float}
            balance_info = self.analyzer.get_account_balance()

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
                            trading_flow = self.analyzer.get_recent_financial_flow(start_time=last_ts_ms - 1000)

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

    def start(self):
        """启动定时任务"""
        if not self.analyzer:
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
            replace_existing=True
        )

        if not self.enable_user_stream:
            # 添加余额同步任务 - 每分钟执行一次
            self.scheduler.add_job(
                func=self.sync_balance_data,
                trigger=IntervalTrigger(minutes=1),
                id='sync_balance',
                name='同步账户余额',
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
            replace_existing=True
        )

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
