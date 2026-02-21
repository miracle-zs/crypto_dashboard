def normalize_futures_symbol(symbol: str, *, preserve_busd: bool = False) -> str:
    value = str(symbol or "").upper().strip()
    if not value:
        return ""
    if preserve_busd and value.endswith("BUSD"):
        return value
    return value if value.endswith("USDT") else f"{value}USDT"
