from datetime import datetime, timezone

from app.logger import logger
from app.notifier import send_server_chan_notification


def run_reentry_alert_check(scheduler):
    """同币在 UTC 当天内重复开仓提醒（每笔重复开仓仅提醒一次）。"""
    try:
        positions = scheduler.risk_repo.get_open_positions()
        if not positions:
            return

        by_symbol = {}
        for pos in positions:
            symbol = str(pos.get("symbol", "")).upper().strip()
            order_id = int(pos.get("order_id", 0) or 0)
            side = str(pos.get("side", "")).upper()
            entry_time = str(pos.get("entry_time", ""))
            if not symbol or order_id <= 0 or not entry_time:
                continue

            try:
                entry_dt_utc8 = datetime.strptime(entry_time, "%Y-%m-%d %H:%M:%S").replace(
                    tzinfo=scheduler.scheduler.timezone
                )
            except ValueError:
                continue

            by_symbol.setdefault(symbol, []).append(
                {
                    "symbol": symbol,
                    "order_id": order_id,
                    "side": side,
                    "entry_time": entry_time,
                    "entry_dt_utc8": entry_dt_utc8,
                    "entry_dt_utc": entry_dt_utc8.astimezone(timezone.utc),
                    "reentry_alerted": int(pos.get("reentry_alerted", 0) or 0),
                }
            )

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
                    triggered.append(
                        {
                            "symbol": symbol,
                            "side": current["side"],
                            "order_id": current["order_id"],
                            "entry_time": current["entry_time"],
                            "previous_order_id": previous["order_id"],
                            "previous_entry_time": previous["entry_time"],
                            "utc_day": current["entry_dt_utc"].strftime("%Y-%m-%d"),
                        }
                    )

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

        notify_res = send_server_chan_notification(title, content)
        if notify_res is not False:
            scheduler.risk_repo.set_positions_reentry_alerted_batch(
                [(item["symbol"], item["order_id"]) for item in triggered]
            )
            logger.info(
                "同币重复开仓提醒已发送: "
                f"count={len(triggered)}, symbols={sorted(set(item['symbol'] for item in triggered))}"
            )
        else:
            logger.warning("同币重复开仓提醒发送失败或未配置 Key，保留未提醒状态以便后续重试")
    except Exception as exc:
        logger.error(f"同币重复开仓提醒检查失败: {exc}")


def run_profit_alert_check(scheduler, threshold_pct: float):
    """检查未平仓订单浮盈阈值提醒（单档，单笔只提醒一次）。"""
    if not scheduler.enable_profit_alert:
        return

    try:
        if hasattr(scheduler.risk_repo, "get_profit_alert_candidates"):
            candidates = scheduler.risk_repo.get_profit_alert_candidates()
        else:
            positions = scheduler.risk_repo.get_open_positions()
            candidates = [p for p in positions if int(p.get("profit_alerted", 0) or 0) == 0]
        if not candidates:
            return

        symbols_full = [scheduler._normalize_futures_symbol(p.get("symbol")) for p in candidates if p.get("symbol")]
        mark_prices = scheduler._get_mark_price_map(symbols_full)
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

            symbol_full = scheduler._normalize_futures_symbol(symbol)
            mark_price = mark_prices.get(symbol_full)
            if mark_price is None:
                continue

            if side == "SHORT":
                unrealized_pnl = (entry_price - mark_price) * qty
            else:
                unrealized_pnl = (mark_price - entry_price) * qty

            unrealized_pct = (unrealized_pnl / entry_amount) * 100
            if unrealized_pct >= threshold_pct:
                triggered.append(
                    {
                        "symbol": symbol,
                        "side": side,
                        "order_id": order_id,
                        "entry_time": entry_time,
                        "entry_price": entry_price,
                        "mark_price": mark_price,
                        "unrealized_pnl": unrealized_pnl,
                        "unrealized_pct": unrealized_pct,
                    }
                )

        if not triggered:
            return

        triggered.sort(key=lambda item: item["unrealized_pct"], reverse=True)
        title = f"🎯 浮盈提醒: {len(triggered)} 笔持仓超过 {threshold_pct:.0f}%"
        content = f"以下未平仓订单浮盈已达到阈值 **{threshold_pct:.0f}%**（每笔仅提醒一次）:\n\n--- \n"
        for item in triggered:
            content += (
                f"**{item['symbol']}** ({item['side']})\n"
                f"- 浮盈: {item['unrealized_pnl']:+.2f} U ({item['unrealized_pct']:.2f}%)\n"
                f"- 开仓: {item['entry_price']:.6g}\n"
                f"- 现价: {item['mark_price']:.6g}\n"
                f"- 时间: {item['entry_time']}\n\n"
            )
        notify_res = send_server_chan_notification(title, content)
        if notify_res is not False:
            scheduler.risk_repo.set_positions_profit_alerted_batch(
                [(item["symbol"], item["order_id"]) for item in triggered]
            )
            logger.info(
                "浮盈提醒已发送: "
                f"threshold={threshold_pct:.2f}%, "
                f"count={len(triggered)}, "
                f"symbols={[item['symbol'] for item in triggered]}"
            )
        else:
            logger.warning("浮盈提醒发送失败或未配置 Key，保留未提醒状态以便后续重试")
    except Exception as exc:
        logger.error(f"浮盈提醒检查失败: {exc}")
