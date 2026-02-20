from app.core.cache import TTLCache


def test_short_ttl_cache_hits_second_request():
    c = TTLCache()
    c.set("k", 1, ttl_seconds=1)
    assert c.get("k") == 1
