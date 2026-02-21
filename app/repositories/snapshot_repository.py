import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class SnapshotRepository:
    def __init__(self, db):
        self.db = db

    def _today_snapshot_date_utc8(self) -> str:
        helper = getattr(self.db, "_today_snapshot_date_utc8", None)
        if callable(helper):
            return helper()
        return datetime.now().strftime("%Y-%m-%d")

    @staticmethod
    def _parse_snapshot_dt(value: str | None) -> Optional[datetime]:
        if not value:
            return None
        try:
            return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def save_leaderboard_snapshot(self, snapshot):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        rows_json = json.dumps(snapshot.get("rows", []), ensure_ascii=False)
        losers_rows_json = json.dumps(snapshot.get("losers_rows", []), ensure_ascii=False)
        all_rows_json = json.dumps(snapshot.get("all_rows", []), ensure_ascii=False)
        cursor.execute(
            """
            INSERT INTO leaderboard_snapshots (
                snapshot_date, snapshot_time, window_start_utc,
                candidates, effective, top_count, rows_json, losers_rows_json, all_rows_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                snapshot_time = excluded.snapshot_time,
                window_start_utc = excluded.window_start_utc,
                candidates = excluded.candidates,
                effective = excluded.effective,
                top_count = excluded.top_count,
                rows_json = excluded.rows_json,
                losers_rows_json = excluded.losers_rows_json,
                all_rows_json = excluded.all_rows_json,
                created_at = CURRENT_TIMESTAMP
            """,
            (
                str(snapshot.get("snapshot_date")),
                str(snapshot.get("snapshot_time")),
                str(snapshot.get("window_start_utc", "")),
                int(snapshot.get("candidates", 0)),
                int(snapshot.get("effective", 0)),
                int(snapshot.get("top", 0)),
                rows_json,
                losers_rows_json,
                all_rows_json,
            ),
        )
        conn.commit()
        conn.close()

    @staticmethod
    def _row_to_leaderboard_snapshot(row) -> Dict:
        data = dict(row)
        rows_json = data.get("rows_json")
        losers_rows_json = data.get("losers_rows_json")
        all_rows_json = data.get("all_rows_json")
        try:
            data["rows"] = json.loads(rows_json) if rows_json else []
        except Exception:
            data["rows"] = []
        try:
            data["losers_rows"] = json.loads(losers_rows_json) if losers_rows_json else []
        except Exception:
            data["losers_rows"] = []
        try:
            data["all_rows"] = json.loads(all_rows_json) if all_rows_json else []
        except Exception:
            data["all_rows"] = []
        data.pop("rows_json", None)
        data.pop("losers_rows_json", None)
        data.pop("all_rows_json", None)
        data["top"] = data.pop("top_count", 0)
        return data

    def get_latest_leaderboard_snapshot(self):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute(
            """
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, losers_rows_json, all_rows_json
            FROM leaderboard_snapshots
            WHERE snapshot_date <= ?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT 1
            """,
            (today,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_leaderboard_snapshot(row)

    def get_leaderboard_snapshot_by_date(self, snapshot_date: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, losers_rows_json, all_rows_json
            FROM leaderboard_snapshots
            WHERE snapshot_date = ?
            LIMIT 1
            """,
            (snapshot_date,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_leaderboard_snapshot(row)

    def list_leaderboard_snapshot_dates(self, limit: int):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute(
            """
            SELECT snapshot_date
            FROM leaderboard_snapshots
            WHERE snapshot_date <= ?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT ?
            """,
            (today, int(limit)),
        )
        rows = cursor.fetchall()
        conn.close()
        return [str(row["snapshot_date"]) for row in rows]

    def get_leaderboard_snapshots_between(self, start_date: str, end_date: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, losers_rows_json, all_rows_json
            FROM leaderboard_snapshots
            WHERE snapshot_date >= ? AND snapshot_date <= ?
            ORDER BY snapshot_date DESC
            """,
            (start_date, end_date),
        )
        rows = cursor.fetchall()
        conn.close()
        return [self._row_to_leaderboard_snapshot(row) for row in rows]

    @staticmethod
    def _lb_safe_float(value) -> Optional[float]:
        try:
            num = float(value)
        except (TypeError, ValueError):
            return None
        if num != num:
            return None
        return num

    def _extract_all_rows(self, snapshot: Dict) -> List[Dict]:
        all_rows = snapshot.get("all_rows", [])
        if all_rows:
            return all_rows
        merged = {}
        for row in snapshot.get("rows", []) + snapshot.get("losers_rows", []):
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            merged[symbol] = row
        return list(merged.values())

    def _build_symbol_maps(self, snapshot: Dict) -> tuple[Dict[str, float], Dict[str, float]]:
        change_map: Dict[str, float] = {}
        price_map: Dict[str, float] = {}
        for row in self._extract_all_rows(snapshot):
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue

            change_val = self._lb_safe_float(row.get("change"))
            if change_val is not None:
                change_map[symbol] = change_val

            price_val = self._lb_safe_float(row.get("last_price"))
            if price_val is None:
                price_val = self._lb_safe_float(row.get("price"))
            if price_val is not None and price_val > 0:
                price_map[symbol] = price_val
        return change_map, price_map

    def build_leaderboard_daily_metrics(self, snapshot_date: str, drop_threshold_pct: float = -10.0) -> Optional[Dict]:
        current_snapshot = self.get_leaderboard_snapshot_by_date(snapshot_date)
        if not current_snapshot:
            return None

        try:
            snap_date = datetime.strptime(snapshot_date, "%Y-%m-%d").date()
        except Exception:
            return None

        prev_date = (snap_date - timedelta(days=1)).strftime("%Y-%m-%d")
        prev2_date = (snap_date - timedelta(days=2)).strftime("%Y-%m-%d")
        prev_snapshot = self.get_leaderboard_snapshot_by_date(prev_date)
        prev2_snapshot = self.get_leaderboard_snapshot_by_date(prev2_date)

        prev_rows = prev_snapshot.get("rows", []) if prev_snapshot else []
        current_losers_rows = current_snapshot.get("losers_rows", [])

        prev_rank_map = {}
        for idx, row in enumerate(prev_rows, start=1):
            symbol = str(row.get("symbol", "")).upper()
            if symbol:
                prev_rank_map[symbol] = idx

        metric1_hits_details = []
        for idx, row in enumerate(current_losers_rows, start=1):
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            prev_rank = prev_rank_map.get(symbol)
            if prev_rank is None:
                continue
            metric1_hits_details.append(
                {
                    "symbol": symbol,
                    "current_loser_rank": idx,
                    "prev_gainer_rank": prev_rank,
                }
            )

        metric1_hits = len(metric1_hits_details)
        metric1_base_count = len(prev_rows)
        metric1_prob = None
        if metric1_base_count > 0:
            metric1_prob = round(metric1_hits * 100.0 / metric1_base_count, 2)
        metric1 = {
            "prev_snapshot_date": prev_snapshot.get("snapshot_date") if prev_snapshot else None,
            "hits": metric1_hits,
            "base_count": metric1_base_count,
            "probability_pct": metric1_prob,
            "symbols": [item["symbol"] for item in metric1_hits_details],
            "details": metric1_hits_details,
        }

        current_change_map, current_price_map = self._build_symbol_maps(current_snapshot)

        metric2_details = []
        metric2_hit_symbols = []
        for idx, row in enumerate(prev_rows, start=1):
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue
            next_change = current_change_map.get(symbol)
            prev_change = self._lb_safe_float(row.get("change"))
            is_hit = next_change is not None and next_change <= drop_threshold_pct
            metric2_details.append(
                {
                    "prev_rank": idx,
                    "symbol": symbol,
                    "prev_change_pct": prev_change,
                    "next_change_pct": next_change,
                    "is_hit": is_hit,
                }
            )
            if is_hit:
                metric2_hit_symbols.append({"symbol": symbol, "next_change_pct": round(next_change, 2)})

        metric2_evaluated_count = sum(1 for item in metric2_details if item.get("next_change_pct") is not None)
        metric2_hits = sum(1 for item in metric2_details if item.get("is_hit"))
        metric2_prob = None
        if metric2_evaluated_count > 0:
            metric2_prob = round(metric2_hits * 100.0 / metric2_evaluated_count, 2)
        metric2 = {
            "base_snapshot_date": prev_snapshot.get("snapshot_date") if prev_snapshot else None,
            "target_snapshot_date": current_snapshot.get("snapshot_date"),
            "threshold_pct": drop_threshold_pct,
            "sample_size": len(metric2_details),
            "evaluated_count": metric2_evaluated_count,
            "hits": metric2_hits,
            "probability_pct": metric2_prob,
            "hit_symbols": metric2_hit_symbols,
            "details": metric2_details,
        }

        prev2_rows = prev2_snapshot.get("rows", []) if prev2_snapshot else []
        metric3_details = []
        metric3_changes = []
        for idx, row in enumerate(prev2_rows, start=1):
            symbol = str(row.get("symbol", "")).upper()
            if not symbol:
                continue

            entry_price = self._lb_safe_float(row.get("last_price"))
            if entry_price is None:
                entry_price = self._lb_safe_float(row.get("price"))
            current_price = current_price_map.get(symbol)
            change_pct = None
            if entry_price is not None and entry_price > 0 and current_price is not None and current_price > 0:
                change_pct = (current_price / entry_price - 1.0) * 100.0
                metric3_changes.append(change_pct)

            metric3_details.append(
                {
                    "prev_rank": idx,
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "change_pct": None if change_pct is None else round(change_pct, 4),
                }
            )

        metric3_evaluated_count = len(metric3_changes)
        dist_lt_neg10 = 0
        dist_mid = 0
        dist_gt_pos10 = 0
        if metric3_evaluated_count > 0:
            dist_lt_neg10 = sum(1 for val in metric3_changes if val < -10.0)
            dist_gt_pos10 = sum(1 for val in metric3_changes if val > 10.0)
            dist_mid = metric3_evaluated_count - dist_lt_neg10 - dist_gt_pos10

        metric3 = {
            "base_snapshot_date": prev2_snapshot.get("snapshot_date") if prev2_snapshot else None,
            "target_snapshot_date": current_snapshot.get("snapshot_date"),
            "hold_hours": 48,
            "sample_size": len(metric3_details),
            "evaluated_count": metric3_evaluated_count,
            "distribution": {"lt_neg10": dist_lt_neg10, "mid": dist_mid, "gt_pos10": dist_gt_pos10},
            "details": metric3_details,
        }

        return {
            "snapshot_date": snapshot_date,
            "computed_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "metric1": metric1,
            "metric2": metric2,
            "metric3": metric3,
        }

    def upsert_leaderboard_daily_metrics_for_date(self, snapshot_date: str):
        payload = self.build_leaderboard_daily_metrics(snapshot_date)
        if not payload:
            return None

        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO leaderboard_daily_metrics (
                snapshot_date, metric1_json, metric2_json, metric3_json, updated_at
            ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                metric1_json = excluded.metric1_json,
                metric2_json = excluded.metric2_json,
                metric3_json = excluded.metric3_json,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                str(payload.get("snapshot_date")),
                json.dumps(payload.get("metric1", {}), ensure_ascii=False),
                json.dumps(payload.get("metric2", {}), ensure_ascii=False),
                json.dumps(payload.get("metric3", {}), ensure_ascii=False),
            ),
        )
        conn.commit()
        conn.close()
        return payload

    def upsert_leaderboard_daily_metrics_for_dates(self, dates):
        result: Dict[str, Dict] = {}
        for d in dates:
            payload = self.upsert_leaderboard_daily_metrics_for_date(str(d))
            if payload:
                result[str(d)] = payload
        return result

    def get_leaderboard_daily_metrics(self, snapshot_date: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT snapshot_date, created_at AS computed_at, metric1_json, metric2_json, metric3_json
            FROM leaderboard_daily_metrics
            WHERE snapshot_date = ?
            LIMIT 1
            """,
            (snapshot_date,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        data = dict(row)
        for key in ("metric1_json", "metric2_json", "metric3_json"):
            try:
                data[key.replace("_json", "")] = json.loads(data.get(key) or "{}")
            except Exception:
                data[key.replace("_json", "")] = {}
            data.pop(key, None)
        return data

    def get_leaderboard_daily_metrics_by_dates(self, dates):
        normalized = [str(d) for d in dates if str(d).strip()]
        if not normalized:
            return {}
        placeholders = ",".join(["?"] * len(normalized))
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT snapshot_date, created_at AS computed_at, metric1_json, metric2_json, metric3_json
            FROM leaderboard_daily_metrics
            WHERE snapshot_date IN ({placeholders})
            """,
            tuple(normalized),
        )
        rows = cursor.fetchall()
        conn.close()

        result: Dict[str, Dict] = {}
        for row in rows:
            item = dict(row)
            for key in ("metric1_json", "metric2_json", "metric3_json"):
                try:
                    item[key.replace("_json", "")] = json.loads(item.get(key) or "{}")
                except Exception:
                    item[key.replace("_json", "")] = {}
                item.pop(key, None)
            snapshot_date = str(item.get("snapshot_date", ""))
            if snapshot_date:
                result[snapshot_date] = item
        return result

    @staticmethod
    def _row_to_noon_loss_snapshot(row) -> Dict:
        data = dict(row)
        rows_json = data.get("rows_json")
        try:
            data["rows"] = json.loads(rows_json) if rows_json else []
        except Exception:
            data["rows"] = []
        data.pop("rows_json", None)
        return data

    @staticmethod
    def _row_to_noon_loss_review_snapshot(row) -> Dict:
        data = dict(row)
        rows_json = data.get("rows_json")
        try:
            data["rows"] = json.loads(rows_json) if rows_json else []
        except Exception:
            data["rows"] = []
        data.pop("rows_json", None)
        return data

    def get_noon_loss_snapshot_by_date(self, snapshot_date: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT snapshot_date, snapshot_time, loss_count, total_stop_loss, pct_of_balance, balance, rows_json
            FROM noon_loss_snapshots
            WHERE snapshot_date = ?
            LIMIT 1
            """,
            (snapshot_date,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_noon_loss_snapshot(row)

    def get_noon_loss_review_snapshot_by_date(self, snapshot_date: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                snapshot_date, review_time, noon_loss_count, not_cut_count,
                noon_cut_loss_total, hold_loss_total, delta_loss_total,
                pct_of_balance, balance, rows_json
            FROM noon_loss_review_snapshots
            WHERE snapshot_date = ?
            LIMIT 1
            """,
            (snapshot_date,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_noon_loss_review_snapshot(row)

    def list_noon_loss_review_history(self, limit: int = 7) -> List[Dict]:
        conn = self.db._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute(
            """
            WITH all_dates AS (
                SELECT snapshot_date FROM noon_loss_snapshots
                UNION
                SELECT snapshot_date FROM noon_loss_review_snapshots
            )
            SELECT
                d.snapshot_date,
                n.snapshot_time AS noon_snapshot_time,
                COALESCE(n.loss_count, 0) AS noon_loss_count,
                COALESCE(r.noon_cut_loss_total, -COALESCE(n.total_stop_loss, 0.0)) AS noon_cut_loss_total,
                COALESCE(n.pct_of_balance, 0.0) AS noon_pct_of_balance,
                r.review_time,
                COALESCE(r.not_cut_count, 0) AS not_cut_count,
                COALESCE(r.hold_loss_total, 0.0) AS hold_loss_total,
                COALESCE(r.delta_loss_total, 0.0) AS delta_loss_total,
                COALESCE(r.pct_of_balance, 0.0) AS review_pct_of_balance,
                r.rows_json
            FROM all_dates d
            LEFT JOIN noon_loss_snapshots n ON n.snapshot_date = d.snapshot_date
            LEFT JOIN noon_loss_review_snapshots r ON r.snapshot_date = d.snapshot_date
            WHERE d.snapshot_date <= ?
            ORDER BY d.snapshot_date DESC
            LIMIT ?
            """,
            (today, int(limit)),
        )
        rows = cursor.fetchall()
        conn.close()

        result = []
        for row in rows:
            item = dict(row)
            snapshot_time_dt = self._parse_snapshot_dt(item.get("noon_snapshot_time"))
            review_time_dt = self._parse_snapshot_dt(item.get("review_time"))
            review_is_stale = (
                snapshot_time_dt is not None
                and review_time_dt is not None
                and review_time_dt < snapshot_time_dt
            )

            if review_is_stale:
                item["review_time"] = None
                item["not_cut_count"] = 0
                item["hold_loss_total"] = 0.0
                item["delta_loss_total"] = 0.0
                item["review_pct_of_balance"] = 0.0
                item["noon_cut_loss_total"] = -abs(float(item.get("noon_cut_loss_total", 0.0)))

            rows_json = item.get("rows_json")
            try:
                item["rows"] = [] if review_is_stale else (json.loads(rows_json) if rows_json else [])
            except Exception:
                item["rows"] = []
            item.pop("rows_json", None)
            result.append(item)
        return result

    def get_noon_loss_review_history_summary(self) -> Dict:
        rows = self.list_noon_loss_review_history(limit=10000)
        reviewed_rows = [row for row in rows if row.get("review_time")]
        delta_sum_all = sum(float(row.get("delta_loss_total") or 0.0) for row in reviewed_rows)
        return {
            "reviewed_count_all": len(reviewed_rows),
            "delta_sum_all": float(delta_sum_all),
        }

    def _save_rebound_snapshot(self, table_name: str, snapshot: Dict):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        rows_json = json.dumps(snapshot.get("rows", []), ensure_ascii=False)
        all_rows_json = json.dumps(snapshot.get("all_rows", []), ensure_ascii=False)
        cursor.execute(
            f"""
            INSERT INTO {table_name} (
                snapshot_date, snapshot_time, window_start_utc,
                candidates, effective, top_count, rows_json, all_rows_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(snapshot_date) DO UPDATE SET
                snapshot_time = excluded.snapshot_time,
                window_start_utc = excluded.window_start_utc,
                candidates = excluded.candidates,
                effective = excluded.effective,
                top_count = excluded.top_count,
                rows_json = excluded.rows_json,
                all_rows_json = excluded.all_rows_json,
                created_at = CURRENT_TIMESTAMP
            """,
            (
                str(snapshot.get("snapshot_date")),
                str(snapshot.get("snapshot_time")),
                str(snapshot.get("window_start_utc", "")),
                int(snapshot.get("candidates", 0)),
                int(snapshot.get("effective", 0)),
                int(snapshot.get("top", 0)),
                rows_json,
                all_rows_json,
            ),
        )
        conn.commit()
        conn.close()

    def save_rebound_7d_snapshot(self, snapshot):
        return self._save_rebound_snapshot("rebound_7d_snapshots", snapshot)

    def save_rebound_30d_snapshot(self, snapshot):
        return self._save_rebound_snapshot("rebound_30d_snapshots", snapshot)

    def save_rebound_60d_snapshot(self, snapshot):
        return self._save_rebound_snapshot("rebound_60d_snapshots", snapshot)

    @staticmethod
    def _row_to_rebound_snapshot(row) -> Dict:
        data = dict(row)
        rows_json = data.get("rows_json")
        all_rows_json = data.get("all_rows_json")
        try:
            data["rows"] = json.loads(rows_json) if rows_json else []
        except Exception:
            data["rows"] = []
        try:
            data["all_rows"] = json.loads(all_rows_json) if all_rows_json else []
        except Exception:
            data["all_rows"] = []
        data.pop("rows_json", None)
        data.pop("all_rows_json", None)
        data["top"] = data.pop("top_count", 0)
        return data

    def _get_latest_rebound_snapshot(self, table_name: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute(
            f"""
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, all_rows_json
            FROM {table_name}
            WHERE snapshot_date <= ?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT 1
            """,
            (today,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_rebound_snapshot(row)

    def get_latest_rebound_7d_snapshot(self):
        return self._get_latest_rebound_snapshot("rebound_7d_snapshots")

    def get_latest_rebound_30d_snapshot(self):
        return self._get_latest_rebound_snapshot("rebound_30d_snapshots")

    def get_latest_rebound_60d_snapshot(self):
        return self._get_latest_rebound_snapshot("rebound_60d_snapshots")

    def _get_rebound_snapshot_by_date(self, table_name: str, snapshot_date: str):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            SELECT snapshot_date, snapshot_time, window_start_utc, candidates, effective, top_count, rows_json, all_rows_json
            FROM {table_name}
            WHERE snapshot_date = ?
            LIMIT 1
            """,
            (snapshot_date,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return self._row_to_rebound_snapshot(row)

    def get_rebound_7d_snapshot_by_date(self, snapshot_date: str):
        return self._get_rebound_snapshot_by_date("rebound_7d_snapshots", snapshot_date)

    def get_rebound_30d_snapshot_by_date(self, snapshot_date: str):
        return self._get_rebound_snapshot_by_date("rebound_30d_snapshots", snapshot_date)

    def get_rebound_60d_snapshot_by_date(self, snapshot_date: str):
        return self._get_rebound_snapshot_by_date("rebound_60d_snapshots", snapshot_date)

    def _list_rebound_snapshot_dates(self, table_name: str, limit: int = 90):
        conn = self.db._get_connection()
        cursor = conn.cursor()
        today = self._today_snapshot_date_utc8()
        cursor.execute(
            f"""
            SELECT snapshot_date
            FROM {table_name}
            WHERE snapshot_date <= ?
            ORDER BY snapshot_date DESC, snapshot_time DESC
            LIMIT ?
            """,
            (today, int(limit)),
        )
        rows = cursor.fetchall()
        conn.close()
        return [str(row["snapshot_date"]) for row in rows]

    def list_rebound_7d_snapshot_dates(self, limit: int):
        return self._list_rebound_snapshot_dates("rebound_7d_snapshots", limit)

    def list_rebound_30d_snapshot_dates(self, limit: int):
        return self._list_rebound_snapshot_dates("rebound_30d_snapshots", limit)

    def list_rebound_60d_snapshot_dates(self, limit: int):
        return self._list_rebound_snapshot_dates("rebound_60d_snapshots", limit)
