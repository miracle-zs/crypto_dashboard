import threading
import time

import pytest

from app.core.job_runtime import JobRuntimeController


def test_heavy_jobs_are_mutually_exclusive():
    controller = JobRuntimeController(lock_wait_seconds=1)

    # Thread 1 acquires heavy lock
    acquired_1 = controller.try_acquire(source="交易同步")
    assert acquired_1 is True

    # Thread 2 tries to acquire another heavy job
    results = []

    def run_thread_2():
        res = controller.try_acquire(source="交易补偿同步")
        results.append(res)
        if res:
            controller.release(source="交易补偿同步")

    t = threading.Thread(target=run_thread_2)
    t.start()
    t.join(timeout=2.0)

    assert len(results) == 1
    assert results[0] is False, "Second heavy job should have timed out and failed"

    # Thread 1 releases
    controller.release(source="交易同步")

    # Now heavy job can be acquired again
    acquired_after = controller.try_acquire(source="日K同步")
    assert acquired_after is True
    controller.release(source="日K同步")


def test_light_job_runs_concurrently_with_heavy_job():
    controller = JobRuntimeController(lock_wait_seconds=2)

    # Thread 1 acquires heavy lock
    acquired_heavy = controller.try_acquire(source="交易同步")
    assert acquired_heavy is True

    # Thread 2 tries to acquire light job while heavy lock is held
    light_results = []

    def run_light():
        res = controller.try_acquire(source="未平仓同步")
        light_results.append(res)
        if res:
            controller.release(source="未平仓同步")

    t = threading.Thread(target=run_light)
    t.start()
    t.join(timeout=1.0)

    assert len(light_results) == 1
    assert light_results[0] is True, "Light job should acquire lock without waiting on heavy job"

    controller.release(source="交易同步")


def test_lock_wait_seconds_zero_bypasses_locks():
    controller = JobRuntimeController(lock_wait_seconds=0)

    assert controller.try_acquire("交易同步") is True
    assert controller.try_acquire("交易补偿同步") is True
    controller.release()
