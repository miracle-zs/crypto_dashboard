from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from app.logger import logger
from app.notifier import send_server_chan_notification

UTC8 = ZoneInfo("Asia/Shanghai")


def run_long_held_positions_check(scheduler):
    """检查持仓时间超过48小时的订单并发送合并通知 (每24小时复提)"""
    try:
        now = datetime.now(UTC8)
        now_utc = datetime.now(timezone.utc)
        entry_before = (now - timedelta(hours=48)).strftime("%Y-%m-%d %H:%M:%S")
        re_alert_before_utc = (now_utc - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
        if hasattr(scheduler.risk_repo, "get_long_held_alert_candidates"):
            positions = scheduler.risk_repo.get_long_held_alert_candidates(
                entry_before=entry_before,
                re_alert_before_utc=re_alert_before_utc,
            )
        else:
            positions = scheduler.risk_repo.get_open_positions()
        stale_positions = []

        for pos in positions:
            if pos.get("is_long_term"):
                continue

            entry_time_str = pos["entry_time"]
            try:
                entry_dt = datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC8)
            except ValueError:
                logger.warning(f"无法解析时间: {entry_time_str}")
                continue

            duration = now - entry_dt

            if duration.total_seconds() > 48 * 3600:
                should_alert = False

                if pos.get("alerted", 0) == 0:
                    should_alert = True
                else:
                    last_alert_str = pos.get("last_alert_time")
                    if last_alert_str:
                        try:
                            last_alert_dt = datetime.strptime(last_alert_str, "%Y-%m-%d %H:%M:%S").replace(
                                tzinfo=timezone.utc
                            )
                            time_since_last = now_utc - last_alert_dt
                            if time_since_last.total_seconds() > 24 * 3600:
                                should_alert = True
                        except ValueError:
                            should_alert = True
                    else:
                        should_alert = True

                if should_alert:
                    hours = int(duration.total_seconds() / 3600)
                    pos["hours_held"] = hours
                    stale_positions.append(pos)

        if stale_positions:
            symbols_full = [
                scheduler._normalize_futures_symbol(pos.get("symbol"))
                for pos in stale_positions
                if pos.get("symbol")
            ]
            mark_prices = scheduler._get_mark_price_map(symbols_full)

            for pos in stale_positions:
                pos["current_pnl"] = None
                pos["current_price"] = None
                symbol = str(pos.get("symbol", "")).upper()
                if not symbol:
                    continue
                symbol_full = scheduler._normalize_futures_symbol(symbol)
                current_price = mark_prices.get(symbol_full)
                if current_price is None:
                    continue

                try:
                    entry_price = float(pos["entry_price"])
                    qty = float(pos["qty"])
                    side = str(pos.get("side", "")).upper()
                except Exception:
                    continue

                pnl = (current_price - entry_price) * qty if side == "LONG" else (entry_price - current_price) * qty
                pos["current_pnl"] = pnl
                pos["current_price"] = current_price

        if stale_positions:
            count = len(stale_positions)
            title = f"⚠️ 持仓超时告警: {count}个订单"
            content = f"监测到 **{count}** 个订单持仓超过 48 小时 (复提周期: 24h)。\n\n"
            content += "--- \n"

            for pos in stale_positions:
                pnl_str = "N/A"
                if pos.get("current_pnl") is not None:
                    pnl_val = pos["current_pnl"]
                    emoji = "🟢" if pnl_val >= 0 else "🔴"
                    pnl_str = f"{emoji} {pnl_val:+.2f} U"
                current_price = pos.get("current_price")
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

            scheduler.risk_repo.set_positions_alerted_batch(
                [(pos["symbol"], pos["order_id"]) for pos in stale_positions]
            )
            for pos in stale_positions:
                logger.info(f"已发送持仓超时告警: {pos['symbol']} ({pos['hours_held']}h)")
    except Exception as e:
        logger.error(f"检查持仓超时失败: {e}")


def run_sleep_risk_check(scheduler):
    """每晚11点检查持仓风险"""
    try:
        positions = scheduler.risk_repo.get_open_positions()
        unique_symbols = {p["symbol"] for p in positions}
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
