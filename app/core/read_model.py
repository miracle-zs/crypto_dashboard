"""OperationalReadModel: 内存优先的读写分离专用读模型。

写路径继续以 SQLite 为权威存储；写成功后 publish/invalidate 对应 section。
读路径优先命中内存聚合，未命中或过期才懒加载回源，从而把热点查询的
数据库锁竞争压到接近 0。
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any


@dataclass
class _Section:
    value: Any = None
    loaded: bool = False
    version: int = 0
    loaded_at: float = 0.0
    load_count: int = 0
    hit_count: int = 0
    lock: threading.Lock = field(default_factory=threading.Lock)


class OperationalReadModel:
    """线程安全的轻量聚合只读模型。

    - ``get_or_load``: 懒加载；首次访问才回源，之后纯内存命中。
    - ``publish``: 写路径推送最新聚合，读侧立即可见。
    - ``invalidate``: 写路径声明 section 已失效，下次读再懒加载。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._sections: dict[str, _Section] = {}

    def _section(self, key: str) -> _Section:
        with self._lock:
            section = self._sections.get(key)
            if section is None:
                section = _Section()
                self._sections[key] = section
            return section

    def get(self, key: str) -> Any | None:
        """仅读内存，不触发回源。未加载时返回 None。"""
        section = self._section(key)
        if not section.loaded:
            return None
        section.hit_count += 1
        return section.value

    def get_or_load(
        self,
        key: str,
        loader: Callable[[], Any],
        *,
        max_age_seconds: float | None = None,
    ) -> Any:
        """懒加载读取：内存新鲜则直接返回，否则回源并缓存。

        ``max_age_seconds`` 为 None 时，数据一直有效直到 publish/invalidate；
        设置后超过该秒数会在下次读时重新加载（避免陈旧）。
        """
        section = self._section(key)
        now = time.monotonic()

        if section.loaded:
            fresh = max_age_seconds is None or (now - section.loaded_at) <= max_age_seconds
            if fresh:
                section.hit_count += 1
                return section.value

        with section.lock:
            now = time.monotonic()
            if section.loaded:
                fresh = max_age_seconds is None or (now - section.loaded_at) <= max_age_seconds
                if fresh:
                    section.hit_count += 1
                    return section.value

            value = loader()
            section.value = value
            section.loaded = True
            section.loaded_at = now
            section.version += 1
            section.load_count += 1
            return value

    def publish(self, key: str, value: Any) -> int:
        """写路径推送聚合结果，返回新版本号。"""
        section = self._section(key)
        with section.lock:
            section.value = value
            section.loaded = True
            section.loaded_at = time.monotonic()
            section.version += 1
            return section.version

    def invalidate(self, key: str | None = None, prefix: str | None = None) -> None:
        """写路径失效：清除缓存值，下次读懒加载。"""
        with self._lock:
            if key is not None:
                section = self._sections.get(key)
                if section is not None:
                    with section.lock:
                        section.value = None
                        section.loaded = False
                        section.loaded_at = 0.0
                return
            if prefix is not None:
                targets = [s for k, s in self._sections.items() if k.startswith(prefix)]
            else:
                targets = list(self._sections.values())
            for section in targets:
                with section.lock:
                    section.value = None
                    section.loaded = False
                    section.loaded_at = 0.0

    async def get_or_load_async(
        self,
        key: str,
        loader: Callable[[], Any],
        *,
        max_age_seconds: float | None = None,
    ) -> Any:
        """异步懒加载：loader 可返回协程（如回源 DB / 聚合）。

        注意：绝不能在 ``await`` 期间持有 threading.Lock，否则会阻塞事件循环。
        冷路径允许短暂重复回源，发布时以 section.lock 原子覆盖。
        """
        section = self._section(key)
        now = time.monotonic()

        if section.loaded:
            fresh = max_age_seconds is None or (now - section.loaded_at) <= max_age_seconds
            if fresh:
                section.hit_count += 1
                return section.value

        value = loader()
        if hasattr(value, "__await__"):
            value = await value

        with section.lock:
            now = time.monotonic()
            if section.loaded:
                fresh = max_age_seconds is None or (now - section.loaded_at) <= max_age_seconds
                if fresh:
                    section.hit_count += 1
                    return section.value
            section.value = value
            section.loaded = True
            section.loaded_at = now
            section.version += 1
            section.load_count += 1
            return value

    def age_seconds(self, key: str) -> float | None:
        """已加载 section 的年龄；未加载返回 None。"""
        section = self._section(key)
        if not section.loaded:
            return None
        return time.monotonic() - section.loaded_at

    def version(self, key: str) -> int:
        return self._section(key).version

    def stats(self) -> dict[str, dict[str, int | bool]]:
        with self._lock:
            return {
                key: {
                    "loaded": section.loaded,
                    "version": section.version,
                    "load_count": section.load_count,
                    "hit_count": section.hit_count,
                }
                for key, section in self._sections.items()
            }


_READ_MODEL: OperationalReadModel | None = None
_READ_MODEL_LOCK = threading.Lock()


def get_read_model() -> OperationalReadModel:
    """进程内共享单例（每个 worker 一份）。"""
    global _READ_MODEL
    if _READ_MODEL is None:
        with _READ_MODEL_LOCK:
            if _READ_MODEL is None:
                _READ_MODEL = OperationalReadModel()
    return _READ_MODEL


def reset_read_model() -> OperationalReadModel:
    """测试辅助：重置单例。"""
    global _READ_MODEL
    with _READ_MODEL_LOCK:
        _READ_MODEL = OperationalReadModel()
        return _READ_MODEL
