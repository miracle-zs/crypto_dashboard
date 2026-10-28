"""Invalidate cached trade metrics when their calculation order changes."""


def apply_v6_trade_summary_recalculation(conn, logger):
    conn.execute(
        "UPDATE trade_summary SET max_single_loss = NULL, max_drawdown = NULL WHERE id = 1"
    )
    logger.info("已标记交易统计缓存过期，将按平仓时间重新计算")
