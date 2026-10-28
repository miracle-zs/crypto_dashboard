import pytest

from app.core.job_runtime import JobCategory, JobRuntimeController, try_enter_slot


def test_job_runtime_controller_categories():
    controller = JobRuntimeController(lock_wait_seconds=2)

    # Explicit category acquisition
    assert controller.try_acquire(source="custom_light_job", category=JobCategory.LIGHT)
    assert controller._thread_local.held_lock == "light"
    controller.release(category=JobCategory.LIGHT)

    assert controller.try_acquire(source="custom_heavy_job", category=JobCategory.HEAVY)
    assert controller._thread_local.held_lock == "heavy"
    controller.release(category=JobCategory.HEAVY)


def test_job_runtime_controller_backward_compatibility():
    controller = JobRuntimeController(lock_wait_seconds=2)

    # Legacy Chinese source strings automatically classified
    assert controller.try_acquire(source="余额同步")
    assert controller._thread_local.held_lock == "light"
    controller.release(source="余额同步")

    assert controller.try_acquire(source="交易同步")
    assert controller._thread_local.held_lock == "heavy"
    controller.release(source="交易同步")


def test_try_enter_slot_passes_category_and_does_not_retry_business_type_errors():
    calls = []

    class Scheduler:
        def _try_enter_api_job_slot(self, *, source, category):
            calls.append((source, category))
            raise TypeError("raised inside slot handler")

    with pytest.raises(TypeError, match="raised inside slot handler"):
        try_enter_slot(Scheduler(), "交易同步", JobCategory.HEAVY)

    assert calls == [("交易同步", JobCategory.HEAVY)]
