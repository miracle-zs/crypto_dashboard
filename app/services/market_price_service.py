from app.logger import logger


class MarketPriceService:
    @staticmethod
    def get_mark_price_map(symbols, client):
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

        return resolved
