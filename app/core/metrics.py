from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter

from app.logger import logger


@dataclass
class MetricSnapshot:
    name: str
    started_at: float
    tags: dict[str, str] = field(default_factory=dict)
    elapsed_ms: float = 0.0


@contextmanager
def measure_ms(name: str, **tags: object):
    snapshot = MetricSnapshot(
        name=name,
        started_at=perf_counter(),
        tags={k: str(v) for k, v in tags.items()},
    )
    try:
        yield snapshot
    finally:
        snapshot.elapsed_ms = max((perf_counter() - snapshot.started_at) * 1000.0, 0.0)


def log_api_metric(*, path: str, method: str, status_code: int, snapshot: MetricSnapshot):
    logger.info(
        "perf api metric | path=%s method=%s status=%s elapsed_ms=%.2f",
        path,
        method,
        status_code,
        snapshot.elapsed_ms,
    )


def log_job_metric(*, job_name: str, status: str, snapshot: MetricSnapshot):
    logger.info(
        "perf job metric | job=%s status=%s elapsed_ms=%.2f",
        job_name,
        status,
        snapshot.elapsed_ms,
    )
