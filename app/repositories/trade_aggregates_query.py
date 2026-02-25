import json
from datetime import datetime, timedelta, timezone


def fetch_trade_aggregates(db, window: str = "all"):
    conn = db._get_connection()
    cursor = conn.cursor()
    utc8 = timezone(timedelta(hours=8))
    now = datetime.now(utc8)
    window = str(window or "all").lower()
    if window not in {"all", "7d", "30d"}:
        window = "all"
    window_since = None
    if window == "7d":
        window_since = (now - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
    elif window == "30d":
        window_since = (now - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")

    if window_since is not None:
        source_where = " WHERE entry_time IS NOT NULL AND entry_time >= ? "
        source_params = (window_since,)
    else:
        source_where = ""
        source_params = ()

    cursor.execute(
        """
        SELECT
            COUNT(*) AS trades_count,
            COALESCE(MAX(updated_at), '') AS latest_trade_updated_at
        FROM trades
        """
        + source_where,
        source_params,
    )
    source_row = cursor.fetchone()
    source_trades_count = int(source_row["trades_count"] or 0) if source_row else 0
    source_latest_updated_at = str(source_row["latest_trade_updated_at"] or "")

    if window == "all":
        cursor.execute(
            """
            SELECT trades_count, latest_trade_updated_at, payload_json
            FROM trade_aggregates_cache
            WHERE id = 1
            """
        )
        cache_row = cursor.fetchone()
        if cache_row:
            cached_payload = cache_row["payload_json"]
            cache_trades_count = int(cache_row["trades_count"] or 0)
            cache_latest_updated_at = str(cache_row["latest_trade_updated_at"] or "")
            if (
                cached_payload
                and cache_trades_count == source_trades_count
                and cache_latest_updated_at == source_latest_updated_at
            ):
                try:
                    payload = json.loads(cached_payload)
                    conn.close()
                    return payload
                except Exception:
                    pass

    # Hourly net pnl (0-23)
    hourly_pnl = [0.0] * 24
    hourly_sql = """
        SELECT CAST(strftime('%H', entry_time) AS INTEGER) AS hour, COALESCE(SUM(pnl_net), 0) AS total_pnl
        FROM trades
        WHERE entry_time IS NOT NULL
        """
    hourly_params = []
    if window_since is not None:
        hourly_sql += " AND entry_time >= ? "
        hourly_params.append(window_since)
    hourly_sql += " GROUP BY hour "
    cursor.execute(hourly_sql, tuple(hourly_params))

    for row in cursor.fetchall():
        hour = row["hour"]
        if hour is None:
            continue
        hour = int(hour)
        if 0 <= hour <= 23:
            hourly_pnl[hour] = float(row["total_pnl"] or 0.0)

    # Duration buckets via SQL-calculated minutes.
    duration_labels = ["0-5m", "5-15m", "15-30m", "30-60m", "1-2h", "2h+"]
    bucket_map = {
        label: {"label": label, "trade_count": 0, "win_pnl": 0.0, "loss_pnl": 0.0}
        for label in duration_labels
    }
    duration_bucket_sql = """
        SELECT
            CASE
                WHEN duration_minutes < 5 THEN '0-5m'
                WHEN duration_minutes < 15 THEN '5-15m'
                WHEN duration_minutes < 30 THEN '15-30m'
                WHEN duration_minutes < 60 THEN '30-60m'
                WHEN duration_minutes < 120 THEN '1-2h'
                ELSE '2h+'
            END AS bucket,
            COUNT(*) AS trade_count,
            COALESCE(SUM(CASE WHEN pnl_net >= 0 THEN pnl_net ELSE 0 END), 0) AS win_pnl,
            COALESCE(SUM(CASE WHEN pnl_net < 0 THEN pnl_net ELSE 0 END), 0) AS loss_pnl
        FROM (
            SELECT
                pnl_net,
                MAX(
                    0.0,
                    (julianday(exit_time) - julianday(entry_time)) * 24.0 * 60.0
                ) AS duration_minutes
            FROM trades
            WHERE entry_time IS NOT NULL AND exit_time IS NOT NULL
    """
    duration_bucket_params = []
    if window_since is not None:
        duration_bucket_sql += " AND entry_time >= ? "
        duration_bucket_params.append(window_since)
    duration_bucket_sql += """
        ) t
        GROUP BY bucket
    """
    cursor.execute(duration_bucket_sql, tuple(duration_bucket_params))
    for row in cursor.fetchall():
        bucket = str(row["bucket"] or "")
        if bucket not in bucket_map:
            continue
        bucket_map[bucket] = {
            "label": bucket,
            "trade_count": int(row["trade_count"] or 0),
            "win_pnl": float(row["win_pnl"] or 0.0),
            "loss_pnl": float(row["loss_pnl"] or 0.0),
        }

    # Duration scatter points (sample recent records for rendering performance).
    duration_scatter_sql = """
        SELECT
            symbol,
            holding_time,
            pnl_net,
            MAX(
                0.0,
                (julianday(exit_time) - julianday(entry_time)) * 24.0 * 60.0
            ) AS duration_minutes
        FROM trades
        WHERE entry_time IS NOT NULL
          AND exit_time IS NOT NULL
    """
    duration_scatter_params = []
    if window_since is not None:
        duration_scatter_sql += " AND entry_time >= ? "
        duration_scatter_params.append(window_since)
    duration_scatter_sql += """
        ORDER BY entry_time DESC
        LIMIT 1200
    """
    cursor.execute(duration_scatter_sql, tuple(duration_scatter_params))
    duration_points = [
        {
            "x": round(float(row["duration_minutes"] or 0.0), 1),
            "y": float(row["pnl_net"] or 0.0),
            "symbol": str(row["symbol"] or "--"),
            "time": str(row["holding_time"] or "--"),
        }
        for row in cursor.fetchall()
    ]

    symbol_rank_sql = """
        SELECT
            symbol,
            COALESCE(SUM(pnl_net), 0) AS pnl,
            COUNT(*) AS trade_count,
            SUM(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END) AS win_count
        FROM trades
        WHERE entry_time IS NOT NULL
    """
    symbol_rank_params = []
    if window_since is not None:
        symbol_rank_sql += " AND entry_time >= ? "
        symbol_rank_params.append(window_since)
    symbol_rank_sql += """
        GROUP BY symbol
    """
    cursor.execute(symbol_rank_sql, tuple(symbol_rank_params))
    rows = cursor.fetchall()

    symbol_rows = []
    total_abs_pnl = 0.0
    for row in rows:
        symbol = str(row["symbol"] or "--")
        pnl = float(row["pnl"] or 0.0)
        trade_count = int(row["trade_count"] or 0)
        win_count = int(row["win_count"] or 0)
        win_rate = (win_count / trade_count * 100.0) if trade_count > 0 else 0.0
        total_abs_pnl += abs(pnl)
        symbol_rows.append(
            {
                "symbol": symbol,
                "pnl": pnl,
                "trade_count": trade_count,
                "win_rate": round(win_rate, 1),
            }
        )

    total_abs_pnl = total_abs_pnl or 1.0
    for item in symbol_rows:
        item["share"] = round(abs(item["pnl"]) / total_abs_pnl * 100.0, 1)

    winners = sorted(
        [row for row in symbol_rows if row["pnl"] > 0],
        key=lambda x: x["pnl"],
        reverse=True,
    )[:5]
    losers = sorted(
        [row for row in symbol_rows if row["pnl"] < 0],
        key=lambda x: x["pnl"],
    )[:5]

    payload = {
        "duration_buckets": [bucket_map[label] for label in duration_labels],
        "duration_points": duration_points,
        "hourly_pnl": hourly_pnl,
        "symbol_rank": {
            "winners": winners,
            "losers": losers,
        },
    }
    if window == "all":
        cursor.execute(
            """
            INSERT INTO trade_aggregates_cache (
                id, trades_count, latest_trade_updated_at, payload_json, updated_at
            ) VALUES (1, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                trades_count = excluded.trades_count,
                latest_trade_updated_at = excluded.latest_trade_updated_at,
                payload_json = excluded.payload_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                source_trades_count,
                source_latest_updated_at,
                json.dumps(payload, ensure_ascii=False),
            ),
        )
    conn.commit()
    conn.close()
    return payload
