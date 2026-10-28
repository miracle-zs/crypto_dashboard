from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.logger import logger
from app.notifier import send_server_chan_notification

UTC8 = ZoneInfo("Asia/Shanghai")


def _report_incomplete(task: str, reason: str, *, notify: bool = True):
    logger.error(f"{task}未完成：{reason}")
    if notify:
        send_server_chan_notification(f"⚠️ {task}未完成", f"{reason}。本次未生成风控结论，请检查数据后重试。")


def run_noon_loss_check(scheduler):
    """每天中午检查全部非长期仓位中当前浮亏的订单"""
    try:
        positions = scheduler.risk_repo.get_open_positions()
        candidate_positions = []

        for pos in positions:
            if pos.get("is_long_term"):
                continue
            candidate_positions.append(pos)

        if any(pos.get("entry_price") is None for pos in candidate_positions):
            _report_incomplete("午间浮亏检查", "部分持仓缺少开仓价格")
            return

        symbol_fulls = [
            scheduler._normalize_futures_symbol(pos.get("symbol"))
            for pos in candidate_positions
            if pos.get("symbol")
        ]
        mark_prices = scheduler._get_mark_price_map(symbol_fulls) if symbol_fulls else {}
        missing_symbols = sorted(set(symbol_fulls) - set(mark_prices))
        if missing_symbols:
            _report_incomplete("午间浮亏检查", f"缺少行情 {', '.join(missing_symbols)}")
            return
        loss_positions = []

        for pos in candidate_positions:
            try:
                symbol_for_quote = scheduler._normalize_futures_symbol(pos["symbol"])
                current_price = mark_prices.get(symbol_for_quote)
                current_price = float(current_price)
                if current_price <= 0:
                    raise ValueError(f"无效价格: {symbol_for_quote}")
                entry_price = float(pos["entry_price"])
                qty = float(pos["qty"])
                side = pos["side"]
                if entry_price <= 0 or qty <= 0 or side not in ("LONG", "SHORT"):
                    raise ValueError(f"持仓数据无效: {symbol_for_quote}")

                if side == "LONG":
                    pnl = (current_price - entry_price) * qty
                else:
                    pnl = (entry_price - current_price) * qty

                pos["current_pnl"] = pnl
                pos["current_price"] = current_price

                if pnl < 0:
                    loss_positions.append(pos)
            except Exception as e:
                _report_incomplete("午间浮亏检查", str(e))
                return

        count = len(loss_positions)
        total_stop_loss = sum(abs(float(pos.get("current_pnl", 0.0))) for pos in loss_positions)
        balance_history = scheduler.trade_repo.get_balance_history(limit=1)
        if not balance_history or balance_history[-1].get("balance") is None:
            _report_incomplete("午间浮亏检查", "缺少账户余额")
            return
        latest_balance = float(balance_history[-1]["balance"])
        if latest_balance <= 0:
            _report_incomplete("午间浮亏检查", "账户余额不大于零，无法计算占比")
            return
        stop_loss_pct_of_balance = total_stop_loss / latest_balance * 100

        loss_positions.sort(key=lambda x: x.get("current_pnl", 0.0))
        snapshot_rows = []
        for pos in loss_positions:
            current_price = pos.get("current_price")
            snapshot_rows.append(
                {
                    "symbol": pos.get("symbol"),
                    "order_id": pos.get("order_id"),
                    "side": pos.get("side"),
                    "qty": float(pos.get("qty", 0.0)),
                    "entry_time": pos.get("entry_time"),
                    "entry_price": float(pos.get("entry_price", 0.0)),
                    "current_price": (float(current_price) if current_price is not None else None),
                    "current_pnl": float(pos.get("current_pnl", 0.0)),
                }
            )

        scheduler.risk_repo.save_noon_loss_snapshot(
            {
                "snapshot_date": datetime.now(UTC8).strftime("%Y-%m-%d"),
                "snapshot_time": datetime.now(UTC8).strftime("%Y-%m-%d %H:%M:%S"),
                "loss_count": count,
                "total_stop_loss": total_stop_loss,
                "pct_of_balance": stop_loss_pct_of_balance,
                "balance": latest_balance,
                "rows": snapshot_rows,
            }
        )
        logger.info(
            f"午间浮亏快照已保存: date={datetime.now(UTC8).strftime('%Y-%m-%d')}, "
            f"count={count}, total_stop_loss={total_stop_loss:.2f} U, "
            f"pct_of_balance={stop_loss_pct_of_balance:.2f}%"
        )

        if loss_positions:
            title = f"⚠️ 午间浮亏警报: {count}个新订单"
            content = (
                f"北京时间 {scheduler.noon_loss_check_hour:02d}:{scheduler.noon_loss_check_minute:02d} "
                f"监测到 **{count}** 个非长期未平仓订单出现浮亏。\n\n"
            )
            content += (
                f"**总结**\n"
                f"- 若全部执行止损，预计总计亏损: {total_stop_loss:.2f} U\n"
                f"- 占账户余额: {stop_loss_pct_of_balance:.2f}%\n"
                f"- 建议: 考虑全部止损\n\n"
            )
            content += "--- \n"
            content += "**明细**\n\n"

            for pos in loss_positions:
                pnl_val = pos["current_pnl"]
                current_price = pos.get("current_price")
                current_price_str = f"{current_price:.6g}" if current_price is not None else "--"
                content += (
                    f"**{pos['symbol']}** ({pos['side']})\n"
                    f"- 浮亏: 🔴 {pnl_val:.2f} U\n"
                    f"- 开仓: {pos['entry_price']}\n"
                    f"- 现价: {current_price_str}\n"
                    f"- 时间: {pos['entry_time']}\n\n"
                )

            send_server_chan_notification(title, content)
            logger.info(
                f"已发送午间浮亏提醒: {count} 个订单，"
                f"总止损亏损 {total_stop_loss:.2f} U，"
                f"占账户余额 {stop_loss_pct_of_balance:.2f}%"
            )
    except Exception as e:
        logger.error(f"午间风控检查失败: {e}")


def run_noon_loss_review(scheduler, snapshot_date: str | None = None, send_notification: bool = True):
    """每晚复盘午间止损建议：按午间快照推演夜间价格下的亏损。"""
    try:
        now = datetime.now(UTC8)
        if snapshot_date is None:
            target_date = (now + timedelta(days=scheduler.noon_review_target_day_offset)).date()
            snapshot_date = target_date.strftime("%Y-%m-%d")
        else:
            try:
                parsed_date = datetime.strptime(snapshot_date, "%Y-%m-%d").date()
                snapshot_date = parsed_date.strftime("%Y-%m-%d")
            except ValueError:
                logger.error(f"午间止损复盘失败: 非法日期 {snapshot_date}，期望 YYYY-MM-DD")
                return

        noon_snapshot = scheduler.risk_repo.get_noon_loss_snapshot_by_date(snapshot_date)

        if not noon_snapshot:
            _report_incomplete("午间止损复盘", f"{snapshot_date} 的午间快照缺失", notify=send_notification)
            return

        noon_rows = noon_snapshot.get("rows", []) or []
        if not noon_rows and int(noon_snapshot.get("loss_count", 0)) > 0:
            _report_incomplete("午间止损复盘", f"{snapshot_date} 的午间亏损明细缺失", notify=send_notification)
            return

        symbol_fulls = []
        for item in noon_rows:
            symbol = str(item.get("symbol", "")).upper().strip()
            if not symbol:
                continue
            symbol_fulls.append(scheduler._normalize_futures_symbol(symbol))
        mark_prices = scheduler._get_mark_price_map(symbol_fulls) if symbol_fulls else {}
        missing_symbols = sorted(set(symbol_fulls) - set(mark_prices))
        if missing_symbols:
            _report_incomplete("午间止损复盘", f"缺少行情 {', '.join(missing_symbols)}", notify=send_notification)
            return

        review_rows = []
        noon_cut_loss_total = 0.0
        hold_loss_total = 0.0
        evaluated_count = 0
        night_loss_count = 0
        night_profit_count = 0
        night_flat_count = 0

        for item in noon_rows:
            symbol = str(item.get("symbol", "")).upper().strip()
            if not symbol:
                continue

            evaluated_count += 1
            row_order_id = item.get("order_id")

            side = str(item.get("side", "")).upper()
            try:
                noon_pnl = float(item["current_pnl"])
                qty = float(item["qty"])
                entry_price = float(item["entry_price"])
                if qty <= 0 or entry_price <= 0 or side not in ("LONG", "SHORT"):
                    raise ValueError("持仓数据无效")
            except (KeyError, TypeError, ValueError):
                _report_incomplete("午间止损复盘", f"{symbol} 的午间持仓数据不完整", notify=send_notification)
                return
            noon_cut_pnl = noon_pnl

            symbol_for_quote = scheduler._normalize_futures_symbol(symbol)
            current_price = mark_prices.get(symbol_for_quote)
            if current_price is not None and current_price <= 0:
                current_price = None

            if current_price is None:
                _report_incomplete("午间止损复盘", f"行情无效 {symbol_for_quote}", notify=send_notification)
                return

            if side == "SHORT":
                night_pnl = (entry_price - current_price) * qty
            else:
                night_pnl = (current_price - entry_price) * qty

            if night_pnl < -1e-9:
                night_loss_count += 1
            elif night_pnl > 1e-9:
                night_profit_count += 1
            else:
                night_flat_count += 1

            delta_pnl = noon_cut_pnl - night_pnl

            noon_cut_loss_total += noon_cut_pnl
            hold_loss_total += night_pnl

            review_rows.append(
                {
                    "symbol": symbol,
                    "order_id": row_order_id,
                    "status": "not_cut",
                    "side": side,
                    "qty": qty,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "price_source": "mark_price",
                    "noon_pnl": noon_pnl,
                    "night_pnl": night_pnl,
                    "noon_loss": noon_cut_pnl,
                    "night_loss": night_pnl,
                    "delta_loss": delta_pnl,
                }
            )

        delta_loss_total = noon_cut_loss_total - hold_loss_total
        balance_history = scheduler.trade_repo.get_balance_history(limit=1)
        if not balance_history or balance_history[-1].get("balance") is None:
            _report_incomplete("午间止损复盘", "缺少账户余额", notify=send_notification)
            return
        latest_balance = float(balance_history[-1]["balance"])
        if latest_balance <= 0:
            _report_incomplete("午间止损复盘", "账户余额不大于零，无法计算占比", notify=send_notification)
            return
        pct_of_balance = delta_loss_total / latest_balance * 100

        scheduler.risk_repo.save_noon_loss_review_snapshot(
            {
                "snapshot_date": snapshot_date,
                "review_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                "noon_loss_count": len(noon_rows),
                "not_cut_count": evaluated_count,
                "noon_cut_loss_total": noon_cut_loss_total,
                "hold_loss_total": hold_loss_total,
                "delta_loss_total": delta_loss_total,
                "pct_of_balance": pct_of_balance,
                "balance": latest_balance,
                "rows": review_rows,
            }
        )
        logger.info(
            f"午间止损复盘完成: date={snapshot_date}, "
            f"noon_loss_count={len(noon_rows)}, evaluated_count={evaluated_count}, "
            f"noon_cut_loss_total={noon_cut_loss_total:.2f} U, "
            f"hold_loss_total={hold_loss_total:.2f} U, "
            f"delta_loss_total={delta_loss_total:.2f} U"
        )
        logger.info(
            "午间止损复盘取价统计: "
            f"mark_price={evaluated_count}, "
            f"night_loss_count={night_loss_count}, "
            f"night_profit_count={night_profit_count}, "
            f"night_flat_count={night_flat_count}"
        )

        if evaluated_count <= 0:
            return

        review_rows.sort(key=lambda x: abs(float(x.get("delta_loss", 0.0))), reverse=True)

        if delta_loss_total > 0:
            summary_text = f"结论：今晚看，不砍仓更差，午间止损更优（Delta {delta_loss_total:+.2f} U）。"
        elif delta_loss_total < 0:
            summary_text = f"结论：今晚看，不砍仓更优（PnL更高，Delta {delta_loss_total:+.2f} U），但仍需遵守纪律。"
        else:
            summary_text = "结论：两种处理结果接近。"

        title = f"🌙 午间止损复盘: {evaluated_count}个币种"
        content = (
            f"{summary_text}\n\n"
            f"北京时间 {now.strftime('%H:%M')} 复盘结果（{snapshot_date}）\n\n"
            f"- 午间止损PnL: {noon_cut_loss_total:+.2f} U\n"
            f"- 持有到夜间PnL: {hold_loss_total:+.2f} U\n"
            f"- Delta PnL(午间-夜间): {delta_loss_total:+.2f} U\n"
            f"- Delta PnL占账户余额: {pct_of_balance:+.2f}%\n\n"
            "---\n"
        )
        for row in review_rows[:10]:
            content += (
                f"**{row['symbol']}** ({row['side']})\n"
                f"- 夜间PnL: {row['night_loss']:+.2f} U\n"
                f"- 午间止损PnL: {row['noon_loss']:+.2f} U\n"
                f"- Delta PnL: {row['delta_loss']:+.2f} U\n\n"
            )

        if send_notification:
            send_server_chan_notification(title, content)
        else:
            logger.info(f"午间止损复盘已跳过通知发送: snapshot_date={snapshot_date}")
    except Exception as e:
        logger.error(f"午间止损夜间复盘失败: {e}")
