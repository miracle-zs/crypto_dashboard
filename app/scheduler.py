"""
定时任务调度器 - 自动更新交易数据
"""
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from trade_analyzer import BinanceOrderAnalyzer
from app.database import Database

load_dotenv()


class TradeDataScheduler:
    """交易数据定时更新调度器"""

    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.db = Database()

        # 从环境变量获取配置
        api_key = os.getenv('BINANCE_API_KEY')
        api_secret = os.getenv('BINANCE_API_SECRET')

        if not api_key or not api_secret:
            print("警告: 未配置Binance API密钥，定时任务将无法运行")
            self.analyzer = None
        else:
            self.analyzer = BinanceOrderAnalyzer(api_key, api_secret)

        self.days_to_fetch = int(os.getenv('DAYS_TO_FETCH', 30))
        self.update_interval_minutes = int(os.getenv('UPDATE_INTERVAL_MINUTES', 10))
        self.start_date = os.getenv('START_DATE')  # 自定义起始日期
        self.end_date = os.getenv('END_DATE')      # 自定义结束日期

    def sync_trades_data(self):
        """同步交易数据到数据库"""
        if not self.analyzer:
            print("无法同步: API密钥未配置")
            return

        try:
            print(f"\n{'='*60}")
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 开始同步交易数据...")
            print(f"{'='*60}")

            # 更新同步状态为进行中
            self.db.update_sync_status(status='syncing')

            # 获取最后一条交易时间（仅作参考，不再用于增量更新）
            # last_entry_time = self.db.get_last_entry_time()

            # 强制使用全量更新模式
            # 如果配置了 START_DATE，则从 START_DATE 开始
            # 否则从 DAYS_TO_FETCH 天前开始
            if self.start_date:
                # 使用自定义起始日期
                try:
                    start_dt = datetime.strptime(self.start_date, '%Y-%m-%d')
                    start_dt = start_dt.replace(hour=23, minute=0, second=0, microsecond=0)
                    since = int(start_dt.timestamp() * 1000)
                    print(f"全量更新模式 - 从自定义日期 {self.start_date} 开始")
                except ValueError as e:
                    print(f"日期格式错误: {e}，使用默认DAYS_TO_FETCH")
                    since = int((datetime.now() - timedelta(days=self.days_to_fetch)).timestamp() * 1000)
            else:
                # 使用DAYS_TO_FETCH
                print(f"全量更新模式 - 获取最近 {self.days_to_fetch} 天数据")
                since = int((datetime.now() - timedelta(days=self.days_to_fetch)).timestamp() * 1000)

            # 计算结束时间
            if self.end_date:
                try:
                    end_dt = datetime.strptime(self.end_date, '%Y-%m-%d')
                    end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=999000)
                    until = int(end_dt.timestamp() * 1000)
                    print(f"使用自定义结束日期: {self.end_date}")
                except ValueError:
                    until = int(datetime.now().timestamp() * 1000)
            else:
                until = int(datetime.now().timestamp() * 1000)

            # 从Binance获取数据
            print(f"从Binance API抓取数据...")
            df = self.analyzer.analyze_orders(since=since, until=until)

            if df.empty:
                print("没有新数据需要更新")
                self.db.update_sync_status(status='idle')
                return

            # 保存到数据库
            print(f"保存 {len(df)} 条记录到数据库...")
            saved_count = self.db.save_trades(df)

            # 更新同步状态
            self.db.update_sync_status(status='idle')

            # 显示统计信息
            stats = self.db.get_statistics()
            print(f"\n同步完成!")
            print(f"数据库统计:")
            print(f"  - 总交易数: {stats['total_trades']}")
            print(f"  - 不同币种: {stats['unique_symbols']}")
            print(f"  - 时间范围: {stats['earliest_trade']} ~ {stats['latest_trade']}")
            print(f"{'='*60}\n")

        except Exception as e:
            error_msg = f"同步失败: {str(e)}"
            print(f"错误: {error_msg}")
            self.db.update_sync_status(status='error', error_message=error_msg)
            import traceback
            traceback.print_exc()

    def sync_balance_data(self):
        """同步账户余额数据到数据库"""
        if not self.analyzer:
            return  # 如果没有配置API密钥，则不执行

        try:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 尝试同步账户余额...")
            balance = self.analyzer.get_account_balance()
            if balance is not None:
                self.db.save_balance_history(balance)
                print(f"  → 成功获取并存储余额: {balance:.2f} USDT")
            else:
                print("  → 获取余额失败，balance为 None。检查API连接或响应。")
        except Exception as e:
            print(f"错误: 同步余额失败: {str(e)}")

    def start(self):
        """启动定时任务"""
        if not self.analyzer:
            print("定时任务未启动: API密钥未配置")
            return

        # 立即执行一次同步
        print(f"立即执行首次数据同步...")
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

        # 添加余额同步任务 - 每分钟执行一次
        self.scheduler.add_job(
            func=self.sync_balance_data,
            trigger=IntervalTrigger(minutes=1),
            id='sync_balance',
            name='同步账户余额',
            replace_existing=True
        )

        self.scheduler.start()
        print(f"交易数据同步任务已启动: 每 {self.update_interval_minutes} 分钟自动更新一次")
        print(f"余额监控任务已启动: 每 1 分钟自动更新一次")

    def stop(self):
        """停止定时任务"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            print("定时任务已停止")

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