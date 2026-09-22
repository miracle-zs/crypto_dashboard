import time
from typing import Dict, Tuple

from app.logger import logger


class MarketPriceService:
    _cached_prices: Dict[str, float] = {}
    _cached_at: float = 0.0

    @classmethod
    def get_latest_cached_prices(cls) -> Tuple[Dict[str, float], float]:
        return dict(cls._cached_prices), cls._cached_at

    @classmethod
    def set_cached_prices(cls, prices: Dict[str, float], timestamp: float | None = None):
        cls._cached_prices.update(prices)
        cls._cached_at = timestamp or time.time()

    @classmethod
    def get_mark_price_map(cls, symbols, client):
        if not symbols:
            return {}

        unique_symbols = sorted(set(symbols))
        resolved = {}
        missing = set(unique_symbols)

        try:
            data = client.public_get("/fapi/v1/premiumIndex")
            if isinstance(data, dict):
                data = [data]
            for item in data or []:
                if not isinstance(item, dict):
                    continue
                symbol = str(item.get("symbol", "")).upper()
                raw_price = item.get("markPrice")
                if not symbol or raw_price is None:
                    continue
                try:
                    price = float(raw_price)
                except (TypeError, ValueError):
                    continue
                if price <= 0:
                    continue
                if symbol in missing:
                    resolved[symbol] = price
                    missing.discard(symbol)
        except Exception as exc:
            logger.warning(f"Failed to fetch mark prices via premiumIndex: {exc}")

        if missing:
            try:
                data = client.public_get("/fapi/v1/ticker/price")
                if isinstance(data, dict):
                    data = [data]
                for item in data or []:
                    if not isinstance(item, dict):
                        continue
                    symbol = str(item.get("symbol", "")).upper()
                    raw_price = item.get("price")
                    if symbol not in missing or raw_price is None:
                        continue
                    try:
                        price = float(raw_price)
                    except (TypeError, ValueError):
                        continue
                    if price <= 0:
                        continue
                    resolved[symbol] = price
                    missing.discard(symbol)
            except Exception as exc:
                logger.warning(f"Failed to fetch mark prices via ticker/price: {exc}")

        if resolved:
            cls.set_cached_prices(resolved)
        return resolved
