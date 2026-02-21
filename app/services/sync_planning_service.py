def build_symbol_since_map(traded_symbols, watermarks, since: int, overlap_minutes: int):
    if not traded_symbols:
        return {}, 0

    overlap_ms = int(overlap_minutes) * 60 * 1000
    symbol_since_map = {}
    warmed_symbols = 0

    for symbol in traded_symbols:
        symbol_watermark = watermarks.get(symbol)
        if symbol_watermark is None:
            symbol_since_map[symbol] = since
        else:
            symbol_since_map[symbol] = max(since, int(symbol_watermark) - overlap_ms)
            warmed_symbols += 1

    return symbol_since_map, warmed_symbols
