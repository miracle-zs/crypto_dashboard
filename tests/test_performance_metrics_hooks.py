def test_metrics_timer_records_elapsed_ms():
    from app.core.metrics import measure_ms

    with measure_ms("unit-test-metric") as snapshot:
        pass

    assert snapshot.elapsed_ms >= 0
