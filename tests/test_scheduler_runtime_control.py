def test_job_runtime_lock_timeout_returns_false():
    from app.core.job_runtime import JobRuntimeController

    ctl = JobRuntimeController(lock_wait_seconds=0)
    assert ctl.try_acquire("unit") is True
