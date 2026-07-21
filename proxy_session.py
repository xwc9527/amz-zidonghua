"""
proxy_session.py — 强制代理 Session 与抓取侧统一代理加载。

禁止静默直连：PROXY_REQUIRED=1 时池空/无效/过期一律失败。
"""
from __future__ import annotations

import json
import ipaddress
import logging
import os
import random
import threading
import time
from dataclasses import dataclass, field

from config import (
    ALLOW_DIRECT_FALLBACK,
    PROXY_ENABLED,
    PROXY_POOL_FILE,
    PROXY_POOL_LIVE_REFRESH_SEC,
    PROXY_POOL_MAX_AGE,
    PROXY_POOL_WAIT_FOR_REPLENISH_SEC,
    PROXY_REQUIRED,
    PROXY_VERIFY,
    PROXY_CAPTCHA_COOLDOWN,
    PROXY_ERROR_COOLDOWN,
    PROXY_MAX_ACTIVE_PER_PREFIX,
    PROXY_RATE_LIMIT_COOLDOWN,
)
from proxy_events import report_proxy_event

log = logging.getLogger("proxy_session")


def _network_prefix(ip: str) -> str:
    try:
        addr = ipaddress.ip_address(str(ip or "").strip())
        network = ipaddress.ip_network(f"{addr}/{24 if addr.version == 4 else 48}", strict=False)
        return str(network)
    except ValueError:
        return ""


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
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "count": len(self.entries),
            "error": self.error,
            "error_code": self.error_code,
            "allow_direct": self.allow_direct,
            "metadata": self.metadata,
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
    return PoolLoadResult(ok=True, entries=clean, allow_direct=False, metadata=meta)


class ForcedProxyPool:
    """线程安全运行时代理池；支持冷却、淘汰和独立出口轮换。"""

    def __init__(
        self,
        entries: list[dict] | None = None,
        *,
        required: bool | None = None,
        min_usable: int = 1,
        enable_live_reload: bool = False,
        pool_path: str | None = None,
        live_reload_interval: float | None = None,
        wait_for_replenish_sec: float | None = None,
        target_site: str | None = None,
        feedback_enabled: bool | None = None,
    ):
        self._all: list[dict] = []
        self.required = PROXY_REQUIRED if required is None else required
        self.allow_direct = False
        self.min_usable = max(1, int(min_usable or 1))
        self.wait_for_replenish_sec = (
            PROXY_POOL_WAIT_FOR_REPLENISH_SEC if wait_for_replenish_sec is None else wait_for_replenish_sec
        )
        self._pool_path = pool_path or PROXY_POOL_FILE
        self._live_reload_interval = (
            PROXY_POOL_LIVE_REFRESH_SEC if live_reload_interval is None else live_reload_interval
        )
        self._live_reload_stop = threading.Event()
        self._live_reload_thread: threading.Thread | None = None
        self.target_site = str(target_site or os.getenv("AMZ_SITE") or "US").upper()
        self.feedback_enabled = bool(enable_live_reload if feedback_enabled is None else feedback_enabled)
        self._target_pause_until = 0.0
        self._cv = threading.Condition(threading.RLock())
        self._cursor = 0
        self._states: dict[str, dict] = {}
        if entries is None:
            loaded = load_proxy_pool(self._pool_path, required=self.required)
            if not loaded.ok:
                raise ProxyRequiredError(loaded.error, loaded.error_code)
            entries = loaded.entries
            self.allow_direct = loaded.allow_direct
            self._target_pause_until = float(
                (loaded.metadata.get("target_pause_until") or {}).get(self.target_site) or 0.0
            )
        for p in entries:
            self._add_entry_locked(dict(p))
        if self.required and not self._all and not self.allow_direct:
            raise ProxyRequiredError("代理池为空，拒绝启动", "POOL_EMPTY")
        if enable_live_reload:
            self.start_live_reload()

    def _add_entry_locked(self, entry: dict) -> str:
        """假定调用方已持有 self._cv（或处于单线程构造阶段）。"""
        key = entry.get("proxy") or f"entry-{len(self._all)}"
        entry["_pool_key"] = key
        self._all.append(entry)
        self._states[key] = {
            "entry": entry,
            "in_use": False,
            "disabled": False,
            "cooldown_until": 0.0,
            "captcha_strikes": 0,
            "rate_limit_strikes": 0,
            "network_errors": 0,
            "consecutive_errors": 0,
            "successes": 0,
            "leases": 0,
            "last_error": "",
            "last_used_at": 0.0,
            "pending_removal": False,
        }
        return key

    def _remove_state_locked(self, key: str) -> None:
        """假定调用方已持有 self._cv。"""
        state = self._states.pop(key, None)
        if state is None:
            return
        try:
            self._all.remove(state["entry"])
        except ValueError:
            pass

    @property
    def size(self) -> int:
        return len(self._all)

    @staticmethod
    def _entry_key(entry: dict | None) -> str:
        return (entry or {}).get("_pool_key") or (entry or {}).get("proxy") or ""

    def _usable_states_locked(self, now: float | None = None) -> list[dict]:
        now = time.time() if now is None else now
        return [
            state for state in self._states.values()
            if not state["disabled"] and state["cooldown_until"] <= now
        ]

    @property
    def usable_count(self) -> int:
        with self._cv:
            return len(self._usable_states_locked())

    def wait_if_target_paused(self) -> None:
        """多出口同时报错时的全局保护闸。对已持有短租约的 worker 也生效。"""
        with self._cv:
            while self._target_pause_until > time.time():
                self._cv.wait(timeout=min(0.5, self._target_pause_until - time.time()))

    def _prefix_load_locked(self) -> dict[str, int]:
        loads: dict[str, int] = {}
        for state in self._states.values():
            if not state["in_use"]:
                continue
            prefix = state["entry"].get("network_prefix") or _network_prefix(
                state["entry"].get("exit_ip", "")
            )
            if prefix:
                loads[prefix] = loads.get(prefix, 0) + 1
        return loads

    @staticmethod
    def _selection_score(state: dict, now: float) -> float:
        entry = state["entry"]
        quality = max(0.05, min(1.5, float(entry.get("quality_score") or 0.5)))
        last_used = float(state.get("last_used_at") or 0.0)
        idle_bonus = 0.30 if last_used <= 0 else min(0.30, max(0.0, now - last_used) / 900.0 * 0.30)
        # 小比例探索抖动，防止低分节点永久得不到真实流量证明自己的机会。
        exploration = random.uniform(0.0, 0.08)
        local_penalty = min(0.35, float(state.get("consecutive_errors") or 0) * 0.12)
        return quality + idle_bonus + exploration - local_penalty

    def acquire(self, timeout: float = 30, *, exclude_exit_ips: set[str] | None = None):
        if not self._all:
            if self.allow_direct and not self.required:
                return None
            raise ProxyRequiredError("无可用代理", "POOL_EMPTY")
        excluded = {ip for ip in (exclude_exit_ips or set()) if ip}
        deadline = time.monotonic() + max(0.0, timeout)
        # 低于 min_usable 时不再立即崩溃：常驻验证守护进程会持续增补节点，
        # 这里有界等待（默认最长 PROXY_POOL_WAIT_FOR_REPLENISH_SEC）让它有机会补上，
        # 期间靠热重载线程 / 其它请求 release() 唤醒重新判断；仍不达标才真正报错。
        # 注意：这个等待预算独立于调用方传入的 timeout（那是给下面"找一个空闲
        # 出口"用的，调用方普遍固定传 30s）——否则 wait_for_replenish_sec 配的
        # 300s 会被 30s 的调用超时悄悄截断，形同虚设。
        replenish_deadline = time.monotonic() + max(0.0, self.wait_for_replenish_sec)
        with self._cv:
            while True:
                now = time.time()
                if self._target_pause_until > now:
                    # 多节点同时报错时先保护目标站，不通过疯狂换 IP 扩大封禁。
                    self._cv.wait(timeout=min(0.5, self._target_pause_until - now))
                    continue
                usable = self._usable_states_locked(now)
                if len(usable) < self.min_usable:
                    remaining = replenish_deadline - time.monotonic()
                    if remaining <= 0:
                        raise ProxyRequiredError(
                            f"运行时可用代理不足: {len(usable)} < {self.min_usable}"
                            f"（已等待补充 {self.wait_for_replenish_sec:.0f}s 仍未恢复）",
                            "POOL_BELOW_MINIMUM",
                        )
                    self._cv.wait(timeout=min(0.5, remaining))
                    continue
                prefix_load = self._prefix_load_locked()
                candidates = []
                prefix_blocked = []
                for state in self._states.values():
                    entry = state["entry"]
                    if state["in_use"] or state["disabled"] or state["cooldown_until"] > now:
                        continue
                    if state.get("pending_removal") or entry.get("exit_ip") in excluded:
                        continue
                    prefix = entry.get("network_prefix") or _network_prefix(entry.get("exit_ip", ""))
                    row = (self._selection_score(state, now), state, prefix)
                    if prefix and prefix_load.get(prefix, 0) >= max(1, PROXY_MAX_ACTIVE_PER_PREFIX):
                        prefix_blocked.append(row)
                    else:
                        candidates.append(row)
                # 多样性限制不能制造假死：若所有候选都来自同一网段，允许降级使用。
                choices = candidates or prefix_blocked
                if choices:
                    lease_values = [int(row[1].get("leases") or 0) for row in choices]
                    if max(lease_values) - min(lease_values) >= 5:
                        # 最低频节点最多落后 5 个租约；保证它们能获得少量真实流量
                        # 自证已恢复，而不是被历史低分永久冻结。
                        floor = min(lease_values)
                        choices = [row for row in choices if int(row[1].get("leases") or 0) == floor]
                    _, state, _ = max(choices, key=lambda row: row[0])
                    state["in_use"] = True
                    state["last_used_at"] = now
                    state["leases"] = int(state.get("leases") or 0) + 1
                    return state["entry"]
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    code = "NO_DISTINCT_PROXY" if excluded else "POOL_ACQUIRE_TIMEOUT"
                    raise ProxyRequiredError("没有可分配的独立出口代理", code)
                self._cv.wait(timeout=min(0.25, remaining))

    def release(self, entry, *, outcome: str = "SUCCESS"):
        if entry is None:
            return
        key = self._entry_key(entry)
        feedback_entry = None
        feedback_code = ""
        with self._cv:
            state = self._states.get(key)
            if not state:
                return
            now = time.time()
            code = str(outcome or "SUCCESS").upper()
            state["in_use"] = False
            state["last_used_at"] = now
            if code in (
                "SUCCESS", "ROTATE", "FILTERED", "PARSE_ERROR",
                "CLIENT_TLS_ERROR", "SESSION_CREATE_ERROR",
            ):
                if code == "SUCCESS":
                    state["successes"] += 1
                state["consecutive_errors"] = 0
            elif code == "CAPTCHA":
                state["captcha_strikes"] += 1
                state["consecutive_errors"] += 1
                state["last_error"] = code
                state["cooldown_until"] = now + PROXY_CAPTCHA_COOLDOWN
            elif code == "HTTP_429":
                state["rate_limit_strikes"] += 1
                state["consecutive_errors"] += 1
                state["last_error"] = code
                state["cooldown_until"] = now + PROXY_RATE_LIMIT_COOLDOWN
            elif code == "HTTP_403":
                # 403 可能是 Cookie/指纹/路径而非 IP 坏掉，只做短冷却，等重复出现
                # 或 daemon 复验再降级，避免误杀可用节点。
                state["rate_limit_strikes"] += 1
                state["consecutive_errors"] += 1
                state["last_error"] = code
                state["cooldown_until"] = now + min(PROXY_RATE_LIMIT_COOLDOWN, 120)
            elif code in (
                "CONNECT_TIMEOUT", "READ_TIMEOUT", "PROXY_CONNECT_ERROR",
                "TLS_ERROR", "CONNECTION_RESET", "REQUEST_ERROR", "EMPTY_RESPONSE",
                "OTHER_HTTP_STATUS",
            ):
                state["network_errors"] += 1
                state["consecutive_errors"] += 1
                state["last_error"] = code
                state["cooldown_until"] = now + PROXY_ERROR_COOLDOWN
            elif code == "HTTP_503":
                state["consecutive_errors"] += 1
                state["last_error"] = code
                state["cooldown_until"] = now + min(PROXY_ERROR_COOLDOWN, 60)
            elif code == "LOCALE_CURRENCY_MISMATCH":
                state["consecutive_errors"] += 1
                state["last_error"] = code
                state["cooldown_until"] = now + PROXY_CAPTCHA_COOLDOWN
            else:
                state["last_error"] = code
            if self.feedback_enabled and code not in {
                "ROTATE", "FILTERED", "PARSE_ERROR", "CLIENT_TLS_ERROR", "SESSION_CREATE_ERROR",
            }:
                feedback_entry = dict(state["entry"])
                feedback_code = code
            if state.get("pending_removal"):
                # 热重载已发现该节点从活池消失（守护进程淘汰），归还后立即移除
                self._remove_state_locked(key)
            self._cv.notify_all()
        if feedback_entry:
            report_proxy_event(feedback_entry, feedback_code, target=self.target_site)

    def record_success(self, entry, *, latency_ms: int = 0) -> None:
        key = self._entry_key(entry)
        feedback_entry = None
        with self._cv:
            state = self._states.get(key)
            if state:
                state["successes"] += 1
                state["consecutive_errors"] = 0
                if self.feedback_enabled:
                    feedback_entry = dict(state["entry"])
        if feedback_entry:
            report_proxy_event(
                feedback_entry, "SUCCESS", target=self.target_site,
                latency_ms=latency_ms,
            )

    def health_snapshot(self) -> dict:
        with self._cv:
            now = time.time()
            rows = []
            for state in self._states.values():
                rows.append({
                    "name": state["entry"].get("name", ""),
                    "exit_ip": state["entry"].get("exit_ip", ""),
                    "in_use": state["in_use"],
                    "disabled": state["disabled"],
                    "cooldown_sec": max(0, int(state["cooldown_until"] - now)),
                    "captcha_strikes": state["captcha_strikes"],
                    "rate_limit_strikes": state["rate_limit_strikes"],
                    "network_errors": state["network_errors"],
                    "successes": state["successes"],
                    "last_error": state["last_error"],
                })
            return {
                "total": len(rows),
                "usable": len(self._usable_states_locked(now)),
                "disabled": sum(1 for row in rows if row["disabled"]),
                "cooling": sum(1 for row in rows if row["cooldown_sec"] > 0 and not row["disabled"]),
                "target_pause_sec": max(0, int(self._target_pause_until - now)),
                "entries": rows,
            }

    def unique_exit_ips(self) -> set[str]:
        return {e.get("exit_ip") for e in self._all if e.get("exit_ip")}

    # ── 活池热重载：持续感知常驻验证守护进程的增补/淘汰 ─────────────
    def start_live_reload(self) -> None:
        if self._live_reload_thread and self._live_reload_thread.is_alive():
            return
        self._live_reload_stop.clear()
        self._live_reload_thread = threading.Thread(
            target=self._live_reload_loop, daemon=True, name="proxy-pool-live-reload",
        )
        self._live_reload_thread.start()

    def stop_live_reload(self) -> None:
        self._live_reload_stop.set()
        if self._live_reload_thread:
            self._live_reload_thread.join(timeout=5)

    def _live_reload_loop(self) -> None:
        while not self._live_reload_stop.wait(self._live_reload_interval):
            try:
                self.reload_from_file()
            except Exception as exc:
                log.warning("[proxy] 活池热重载异常: %s", exc)

    def reload_from_file(self) -> dict:
        """重新读取活池文件：把新出现的节点纳入运行时池（立即可用，无冷却），
        把已从活池消失的节点标记淘汰（若正被占用则等 release() 时再真正移除，
        不打断正在进行中的请求）。"""
        loaded = load_proxy_pool(self._pool_path, required=False, allow_direct=False, max_age=0)
        # load_proxy_pool 把"零节点"当作错误（POOL_EMPTY）而非合法快照；但对活池
        # 热重载而言，守护进程把活池清空（例如全部节点被剔除）是完全合法的瞬时
        # 状态，必须继续走下面的差量剔除逻辑，否则永远无法把节点摘干净。
        if not loaded.ok and loaded.error_code != "POOL_EMPTY":
            return {"ok": False, "error": loaded.error, "error_code": loaded.error_code}
        incoming: dict[str, dict] = {}
        for e in loaded.entries:
            key = e.get("proxy") or f"entry-{e.get('port')}"
            incoming[key] = e
        added = removed = 0
        with self._cv:
            old_pause = self._target_pause_until
            self._target_pause_until = float(
                (loaded.metadata.get("target_pause_until") or {}).get(self.target_site) or 0.0
            )
            for key, entry in incoming.items():
                if key in self._states:
                    # 保留当前租约/冷却状态，只原位更新daemon发布的质量与分层元数据。
                    state = self._states[key]
                    current = state["entry"]
                    previous_check = float(current.get("last_checked_at") or 0.0)
                    incoming_check = float(entry.get("last_checked_at") or 0.0)
                    for field in (
                        "node_key", "exit_ip", "quality_score", "network_prefix", "tier",
                        "success_rate", "latency_ewma_ms", "last_success_at", "last_checked_at",
                    ):
                        if field in entry:
                            current[field] = entry[field]
                    if incoming_check > previous_check:
                        # daemon 已完成一次更新的真实验证，可安全解除旧的本地冷却。
                        state["disabled"] = False
                        state["cooldown_until"] = 0.0
                        state["consecutive_errors"] = 0
                    continue
                self._add_entry_locked(dict(entry))
                added += 1
            for key in list(self._states):
                if key in incoming:
                    continue
                state = self._states[key]
                if state["in_use"]:
                    state["pending_removal"] = True
                else:
                    self._remove_state_locked(key)
                    removed += 1
            if added or removed or old_pause != self._target_pause_until:
                self._cv.notify_all()
        if added or removed:
            log.info("[proxy] 活池热重载: +%d -%d (total=%d)", added, removed, len(self._all))
        return {"ok": True, "added": added, "removed": removed}


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
