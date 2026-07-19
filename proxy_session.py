"""
proxy_session.py — 强制代理 Session 与抓取侧统一代理加载。

禁止静默直连：PROXY_REQUIRED=1 时池空/无效/过期一律失败。
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from queue import Queue

from config import (
    ALLOW_DIRECT_FALLBACK,
    PROXY_ENABLED,
    PROXY_POOL_FILE,
    PROXY_POOL_MAX_AGE,
    PROXY_REQUIRED,
    PROXY_VERIFY,
)

log = logging.getLogger("proxy_session")


class ProxyRequiredError(RuntimeError):
    """强制代理模式下不可继续。"""

    def __init__(self, message: str, code: str = "PROXY_REQUIRED"):
        super().__init__(message)
        self.code = code


@dataclass
class PoolLoadResult:
    ok: bool
    entries: list[dict]
    error: str = ""
    error_code: str = ""
    allow_direct: bool = False

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "count": len(self.entries),
            "error": self.error,
            "error_code": self.error_code,
            "allow_direct": self.allow_direct,
        }


def _pool_age_sec(path: str) -> float:
    return time.time() - os.path.getmtime(path)


def load_proxy_pool(
    path: str | None = None,
    *,
    required: bool | None = None,
    allow_direct: bool | None = None,
    max_age: int | None = None,
) -> PoolLoadResult:
    pool_path = path or PROXY_POOL_FILE
    must = PROXY_REQUIRED if required is None else required
    direct_ok = ALLOW_DIRECT_FALLBACK if allow_direct is None else allow_direct
    age_limit = PROXY_POOL_MAX_AGE if max_age is None else max_age

    if not PROXY_ENABLED and not must:
        return PoolLoadResult(ok=True, entries=[], allow_direct=True)

    if not os.path.isfile(pool_path):
        msg = f"代理池文件不存在: {pool_path}"
        if must and not direct_ok:
            return PoolLoadResult(ok=False, entries=[], error=msg, error_code="POOL_MISSING")
        if direct_ok:
            return PoolLoadResult(ok=True, entries=[], allow_direct=True, error=msg, error_code="POOL_MISSING")
        return PoolLoadResult(ok=False, entries=[], error=msg, error_code="POOL_MISSING")

    try:
        with open(pool_path, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        msg = f"代理池文件无效: {e}"
        if must and not direct_ok:
            return PoolLoadResult(ok=False, entries=[], error=msg, error_code="POOL_INVALID")
        return PoolLoadResult(ok=False, entries=[], error=msg, error_code="POOL_INVALID")

    if isinstance(data, dict) and "entries" in data:
        entries = data.get("entries") or []
        meta = data
    elif isinstance(data, list):
        entries = data
        meta = {}
    else:
        msg = "代理池格式非法"
        return PoolLoadResult(ok=False, entries=[], error=msg, error_code="POOL_INVALID")

    if not isinstance(entries, list):
        return PoolLoadResult(ok=False, entries=[], error="entries 非列表", error_code="POOL_INVALID")

    age = _pool_age_sec(pool_path)
    if age_limit > 0 and age > age_limit:
        msg = f"代理池已过期 ({int(age)}s > {age_limit}s)"
        if must and not direct_ok:
            return PoolLoadResult(ok=False, entries=[], error=msg, error_code="POOL_EXPIRED")
        return PoolLoadResult(ok=False, entries=[], error=msg, error_code="POOL_EXPIRED")

    clean = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        proxy = e.get("proxy")
        if not proxy:
            continue
        clean.append(e)

    if not clean:
        msg = "代理池为空"
        if must and not direct_ok:
            return PoolLoadResult(ok=False, entries=[], error=msg, error_code="POOL_EMPTY")
        if direct_ok:
            log.warning("[proxy] ALLOW_DIRECT_FALLBACK=1，空池将直连（已标注）")
            return PoolLoadResult(ok=True, entries=[], allow_direct=True, error=msg, error_code="POOL_EMPTY")
        return PoolLoadResult(ok=False, entries=[], error=msg, error_code="POOL_EMPTY")

    if meta.get("profile_uid"):
        log.info(
            "[proxy] 加载池 %s entries=%d uid=%s",
            pool_path, len(clean), meta.get("profile_uid"),
        )
    else:
        log.info("[proxy] 加载池 %s entries=%d", pool_path, len(clean))
    return PoolLoadResult(ok=True, entries=clean, allow_direct=False)


class ForcedProxyPool:
    """线程安全代理队列；强制模式下不可降级直连。"""

    def __init__(self, entries: list[dict] | None = None, *, required: bool | None = None):
        self._q: Queue = Queue()
        self._all: list[dict] = []
        self.required = PROXY_REQUIRED if required is None else required
        self.allow_direct = False
        if entries is None:
            loaded = load_proxy_pool(required=self.required)
            if not loaded.ok:
                raise ProxyRequiredError(loaded.error, loaded.error_code)
            entries = loaded.entries
            self.allow_direct = loaded.allow_direct
        for p in entries:
            self._q.put(p)
            self._all.append(p)
        if self.required and not self._all and not self.allow_direct:
            raise ProxyRequiredError("代理池为空，拒绝启动", "POOL_EMPTY")

    @property
    def size(self) -> int:
        return len(self._all)

    def acquire(self, timeout: float = 30):
        if not self._all:
            if self.allow_direct and not self.required:
                return None
            raise ProxyRequiredError("无可用代理", "POOL_EMPTY")
        return self._q.get(timeout=timeout)

    def release(self, entry):
        if entry is not None:
            self._q.put(entry)

    def unique_exit_ips(self) -> set[str]:
        return {e.get("exit_ip") for e in self._all if e.get("exit_ip")}


def make_forced_session(
    proxy_entry: dict | None,
    *,
    headers: dict | None = None,
    required: bool | None = None,
    impersonate: str = "chrome124",
):
    """创建 Session；强制模式下无代理视为程序错误。"""
    must = PROXY_REQUIRED if required is None else required
    try:
        from curl_cffi import requests as cr
    except ImportError as e:
        raise ProxyRequiredError(f"缺少 curl_cffi: {e}", "DEP_MISSING") from e

    if proxy_entry is None or not proxy_entry.get("proxy"):
        if must and not ALLOW_DIRECT_FALLBACK:
            raise ProxyRequiredError("Session 未配置代理", "SESSION_NO_PROXY")
        proxies = {}
        log.warning("[proxy] Session 直连（ALLOW_DIRECT_FALLBACK=1）")
    else:
        proxies = {"http": proxy_entry["proxy"], "https": proxy_entry["proxy"]}

    session = cr.Session(
        impersonate=impersonate,
        headers=headers or {},
        proxies=proxies,
        verify=PROXY_VERIFY,
    )
    session._amz_proxy_entry = proxy_entry  # type: ignore[attr-defined]
    return session


def assert_session_has_proxy(session, *, required: bool | None = None) -> None:
    must = PROXY_REQUIRED if required is None else required
    if not must:
        return
    proxies = getattr(session, "proxies", None) or {}
    if not proxies.get("http") and not proxies.get("https"):
        raise ProxyRequiredError("运行中 Session 丢失代理", "SESSION_NO_PROXY")
