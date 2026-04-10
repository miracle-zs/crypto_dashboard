from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from app.binance_client import BinanceFuturesRestClient
from app.logger import logger
from app.repositories.crash_risk_repository import CrashRiskRepository


class CrashRiskService:
    _COMPONENT_KEYS = (
        "oi_divergence",
        "price_extension",
        "momentum_fade",
        "support_fragility",
        "trigger_confirmation",
    )
    _PRICE_WINDOW_WEIGHTS = {
        6: 0.6,
        12: 0.45,
        24: 0.3,
    }
    _PRICE_WINDOW_CAPS = {
        6: 12.0,
        12: 7.0,
        24: 5.0,
    }
    _OI_WINDOW_WEIGHTS = {
        6: 1.2,
        12: 1.0,
        24: 0.8,
    }
    _OI_WINDOW_CAPS = {
        6: 24.0,
        12: 10.0,
        24: 8.0,
    }

    def __init__(
        self,
        client: Optional[BinanceFuturesRestClient] = None,
        repository_cls=None,
    ):
        self.client = client or BinanceFuturesRestClient()
        self.repository_cls = repository_cls or CrashRiskRepository

    def build_empty_response(self):
        return {
            "as_of": None,
            "summary": {
                "total": 0,
                "high_risk": 0,
                "warning": 0,
                "watch": 0,
            },
            "rows": [],
        }

    def _get_latest_snapshot(self, db):
        repo = self.repository_cls(db)
        getter = getattr(repo, "get_candidate_symbols_snapshot_union", None)
        if callable(getter):
            return getter()
        return repo.get_latest_leaderboard_snapshot()

    @staticmethod
    def _clamp(value, low, high):
        return max(low, min(high, value))

    @staticmethod
    def _pct_change(start, end):
        if start in (None, 0):
            return 0.0
        return (float(end) - float(start)) / float(start) * 100.0

    @staticmethod
    def _mean(values):
        if not values:
            return 0.0
        return sum(values) / len(values)

    @staticmethod
    def _window_pct_change(values, window):
        if len(values) < window or window < 2:
            return 0.0
        return CrashRiskService._pct_change(values[-window], values[-1])

    @staticmethod
    def _ending_decline_streak(values):
        if len(values) < 2:
            return 0
        streak = 0
        for index in range(len(values) - 1, 0, -1):
            if values[index] < values[index - 1]:
                streak += 1
            else:
                break
        return streak

    @staticmethod
    def _extract_numeric_series(rows: Iterable[Any], *, value_key: Optional[str] = None) -> list[float]:
        values: list[float] = []
        for row in rows or []:
            raw_value = None
            if isinstance(row, dict):
                if value_key is not None:
                    raw_value = row.get(value_key)
                else:
                    for candidate in ("sumOpenInterest", "openInterest", "value", "amount"):
                        raw_value = row.get(candidate)
                        if raw_value is not None:
                            break
            elif isinstance(row, (list, tuple)):
                if value_key is not None and len(row) >= 2:
                    raw_value = row[1]
                elif len(row) >= 1:
                    raw_value = row[-1]
            if raw_value is None:
                continue
            try:
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            values.append(value)
        return values

    def fetch_symbol_inputs(self, symbol: str, *, limit: int = 24) -> Dict[str, list[float]]:
        klines = self.client.get_klines_1h(symbol, limit=limit) or []
        open_interest_rows = self.client.get_open_interest_history_1h(symbol, limit=limit) or []

        closes: list[float] = []
        highs: list[float] = []
        lows: list[float] = []
        volumes: list[float] = []
        for row in klines:
            if not isinstance(row, (list, tuple)) or len(row) < 6:
                continue
            try:
                highs.append(float(row[2]))
                lows.append(float(row[3]))
                closes.append(float(row[4]))
                volumes.append(float(row[5]))
            except (TypeError, ValueError):
                continue

        open_interests = self._extract_numeric_series(open_interest_rows)

        return {
            "closes": closes,
            "highs": highs,
            "lows": lows,
            "volumes": volumes,
            "open_interests": open_interests,
        }

    def build_from_leaderboard_snapshot(self, db):
        snapshot = self._get_latest_snapshot(db)
        if not snapshot:
            return self.build_empty_response()

        rows = []
        stage_counts = {"高危": 0, "警惕": 0, "观察": 0}
        source_symbols = snapshot.get("symbols")
        if source_symbols is None:
            source_rows = snapshot.get("rows", []) or []
            source_symbols = [
                str(row.get("symbol", "")).upper().strip()
                for row in source_rows
                if str(row.get("symbol", "")).upper().strip()
            ]
        deduped_symbols = []
        seen_symbols = set()
        for symbol in source_symbols:
            if not symbol or symbol in seen_symbols:
                continue
            seen_symbols.add(symbol)
            deduped_symbols.append(symbol)
        for symbol in deduped_symbols:
            market_series = self.fetch_symbol_inputs(symbol)
            score = self.score_symbol(market_series)
            row = {
                "symbol": symbol,
                **score,
            }
            rows.append(row)
            stage_counts[row["stage"]] = stage_counts.get(row["stage"], 0) + 1

        rows.sort(key=lambda item: item.get("risk_score", 0), reverse=True)
        as_of = snapshot.get("snapshot_time") or snapshot.get("snapshot_date")
        return {
            "as_of": as_of,
            "summary": {
                "total": len(rows),
                "high_risk": stage_counts.get("高危", 0),
                "warning": stage_counts.get("警惕", 0),
                "watch": stage_counts.get("观察", 0),
            },
            "source_snapshot": {
                "source": snapshot.get("source", "leaderboard_snapshot"),
                "snapshot_date": snapshot.get("snapshot_date"),
                "snapshot_time": snapshot.get("snapshot_time"),
                "window_start_utc": snapshot.get("window_start_utc"),
            },
            "rows": rows,
        }

    refresh_from_leaderboard_snapshot = build_from_leaderboard_snapshot

    @classmethod
    def _stage_from_score(cls, risk_score):
        if risk_score >= 70:
            return "高危"
        if risk_score >= 40:
            return "警惕"
        return "观察"

    @classmethod
    def score_symbol(cls, market_series):
        closes = [float(x) for x in market_series.get("closes", []) if x is not None]
        highs = [float(x) for x in market_series.get("highs", []) if x is not None]
        lows = [float(x) for x in market_series.get("lows", []) if x is not None]
        open_interests = [float(x) for x in market_series.get("open_interests", []) if x is not None]
        volumes = [float(x) for x in market_series.get("volumes", []) if x is not None]

        component_scores = {key: 0 for key in cls._COMPONENT_KEYS}

        if len(closes) < 2 or len(open_interests) < 2 or len(lows) < 2 or len(highs) < 2 or len(volumes) < 2:
            return {
                "risk_score": 0,
                "stage": "观察",
                "drivers": [],
                "component_scores": component_scores,
            }

        half_index = max(1, len(closes) // 2)
        first_close = closes[0]
        mid_close = closes[half_index]
        last_close = closes[-1]

        price_return = cls._pct_change(first_close, last_close)
        oi_return = cls._pct_change(open_interests[0], open_interests[-1])

        recent_window = min(4, len(highs))
        recent_high = max(highs[-recent_window:])
        recent_low = min(lows[-recent_window:])

        window_signals = []
        for window in (6, 12, 24):
            if len(closes) >= window and len(open_interests) >= window:
                window_signals.append(
                    (
                        window,
                        cls._window_pct_change(closes, window),
                        cls._window_pct_change(open_interests, window),
                    )
                )

        if window_signals:
            price_extension = 0.0
            oi_divergence = 0.0
            oi_decline_streak = cls._ending_decline_streak(open_interests)
            positive_window_count = 0

            for window, price_change, oi_change in window_signals:
                if price_change > 0:
                    price_extension += min(
                        cls._PRICE_WINDOW_CAPS[window],
                        price_change * cls._PRICE_WINDOW_WEIGHTS[window],
                    )
                if price_change > 0 and oi_change < 0:
                    positive_window_count += 1
                    oi_divergence += min(
                        cls._OI_WINDOW_CAPS[window],
                        (price_change * 1.1 + abs(oi_change) * 1.5) * cls._OI_WINDOW_WEIGHTS[window],
                    )

            if positive_window_count > 0 and oi_decline_streak > 0:
                oi_divergence += min(12.0, oi_decline_streak * 2.0)
            if last_close >= recent_high * 0.98 and any(price_change > 0 for _, price_change, _ in window_signals):
                price_extension += 2.0
                if oi_decline_streak > 0:
                    oi_divergence += min(5.0, float(oi_decline_streak))

            component_scores["oi_divergence"] = int(cls._clamp(round(oi_divergence), 0, 35))
            component_scores["price_extension"] = int(cls._clamp(round(price_extension), 0, 20))
        elif price_return > 0 and oi_return < 0:
            component_scores["oi_divergence"] = int(
                cls._clamp(round(min(35.0, price_return * 2.0 + abs(oi_return) * 2.5)), 0, 35)
            )
            if last_close >= recent_high * 0.95:
                component_scores["price_extension"] = int(
                    cls._clamp(round(min(20.0, max(0.0, cls._pct_change(recent_low, recent_high)) * 1.2)), 0, 20)
                )

        early_volume = cls._mean(volumes[:half_index])
        late_volume = cls._mean(volumes[half_index:])
        volume_fade_pct = 0.0
        if early_volume > 0 and late_volume < early_volume:
            volume_fade_pct = max(0.0, (early_volume - late_volume) / early_volume * 100.0)
        momentum_fade = 0
        if volume_fade_pct > 0:
            momentum_fade = round(
                min(
                    20.0,
                    volume_fade_pct * 0.25 + max(0.0, cls._pct_change(mid_close, last_close)) * 0.5,
                )
            )
        component_scores["momentum_fade"] = int(momentum_fade)

        support_fragility = 0
        if len(lows) >= 4 and price_return > 0 and last_close < recent_high:
            drawdown_from_high = max(0.0, -cls._pct_change(recent_high, last_close))
            support_fragility = round(
                min(
                    15.0,
                    drawdown_from_high * 3.0 + max(0.0, -cls._pct_change(mid_close, last_close)) * 0.75,
                )
            )
        component_scores["support_fragility"] = int(support_fragility)

        trigger_confirmation = 0
        if last_close < lows[-2]:
            trigger_confirmation += 6
        if len(closes) >= 3 and closes[-1] < closes[-2] < closes[-3]:
            trigger_confirmation += 4
        component_scores["trigger_confirmation"] = int(min(10, trigger_confirmation))

        risk_score = int(cls._clamp(sum(component_scores.values()), 0, 100))

        driver_labels = {
            "oi_divergence": "OI divergence is widening",
            "price_extension": "Price is pressing into recent highs",
            "momentum_fade": "Momentum is fading on thinner volume",
            "support_fragility": "Support is weakening",
            "trigger_confirmation": "Breakdown trigger is confirming",
        }
        drivers = [
            driver_labels[key]
            for key in cls._COMPONENT_KEYS
            if component_scores[key] > 0
        ]

        logger.debug(
            "Crash-risk score computed",
            extra={
                "symbol": market_series.get("symbol"),
                "risk_score": risk_score,
                "drivers": drivers,
            },
        )

        return {
            "risk_score": risk_score,
            "stage": cls._stage_from_score(risk_score),
            "drivers": drivers,
            "component_scores": component_scores,
        }
