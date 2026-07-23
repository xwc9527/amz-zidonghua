"""
proxy_daemon.py — 常驻代理验证守护进程。

独立于 api_server / 抓取进程的生命周期：一直运行，持续验证候选节点、
增量发布活池（一个节点验证通过即刻可用，不等整批完成），并按 node_key
淘汰变坏节点、退避重试失败节点。

优化点：
- 复核抖动 / 轻量哨兵 / 池够用停扩 / 空闲放缓（降出站成本）
- 稳定端口映射 + 纯新增热重载 + 订阅指纹防抖（减重建抖动）
- 热重载失败时蓝绿切换兜底

用法：
    python proxy_daemon.py            # 常驻循环
    python proxy_daemon.py --once     # 只执行一次调度 tick
    python proxy_daemon.py --status   # 打印状态
"""
from __future__ import annotations

import argparse
import hashlib
import ipaddress
import json
import logging
import os
import random
import signal
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone

from config import (
    PROXY_BASE_PORT,
    PROXY_DAEMON_ACTIVITY_FILE,
    PROXY_DAEMON_BLUE_GREEN_BASE_PORT,
    PROXY_DAEMON_CONCURRENCY,
    PROXY_DAEMON_EXPLORATION_INTERVAL_SEC,
    PROXY_DAEMON_FEEDBACK_BATCH,
    PROXY_DAEMON_FEEDBACK_DEDUPE_SEC,
    PROXY_DAEMON_FULL_SCAN_INTERVAL_SEC,
    PROXY_DAEMON_IDLE_AFTER_SEC,
    PROXY_DAEMON_IDLE_CONCURRENCY,
    PROXY_DAEMON_IDLE_RECHECK_MULTIPLIER,
    PROXY_DAEMON_LIGHT_RECHECKS_BEFORE_FULL,
    PROXY_DAEMON_LOG_FILE,
    PROXY_DAEMON_PID_FILE,
    PROXY_DAEMON_PORT_MAP_FILE,
    PROXY_DAEMON_PUBLISH_DEBOUNCE_SEC,
    PROXY_DAEMON_RECHECK_INTERVAL_SEC,
    PROXY_DAEMON_RECHECK_JITTER_PCT,
    PROXY_DAEMON_RETRY_BACKOFF_BASE_SEC,
    PROXY_DAEMON_RETRY_BACKOFF_MAX_SEC,
    PROXY_DAEMON_STATE_FILE,
    PROXY_DAEMON_SUBSCRIPTION_CONFIRM_POLLS,
    PROXY_DAEMON_SUBSCRIPTION_POLL_SEC,
    PROXY_DAEMON_TICK_INTERVAL_SEC,
    PROXY_DAEMON_WARM_RECHECK_INTERVAL_SEC,
    PROXY_MIN_START_NODES,
    PROXY_MAX_ACTIVE_PER_PREFIX,
    PROXY_POOL_HOT_MAX_NODES,
    PROXY_POOL_LOW_WATERMARK,
    PROXY_POOL_TARGET_NODES,
    PROXY_POOL_FILE,
    PROXY_POOL_STATUS_FILE,
    PROXY_PORT_RANGE_END,
)
from proxy_events import ProxyEvent, drain_proxy_events
from proxy_health import get_reference_ips, verify_node
from proxy_node_source import ProfileFingerprint, load_candidate_nodes
from proxy_runtime import (
    check_listener,
    owned_mihomo_running,
    pid_alive,
    reload_mihomo_config,
    start_mihomo,
    start_mihomo_blue_green,
    stop_owned_mihomo,
)

log = logging.getLogger("proxy_daemon")

_DAEMON_LOCK_FILE = f"{PROXY_DAEMON_PID_FILE}.lock"
_DAEMON_STOP_FILE = f"{PROXY_DAEMON_PID_FILE}.stop"

STATE_NEW = "new"
STATE_VERIFIED = "verified"
STATE_FAILED = "failed"

_REFS_CACHE_TTL_SEC = 300


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write_json(path: str, data) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


class DaemonInstanceLock:
    """跨进程单实例锁，防止多个 API 同时拉起 daemon 后争抢 Mihomo。"""

    def __init__(self, path: str = _DAEMON_LOCK_FILE):
        self.path = path
        self._fh = None

    def acquire(self) -> bool:
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            self._fh = open(self.path, "a+b")
            self._fh.seek(0, os.SEEK_END)
            if self._fh.tell() == 0:
                self._fh.write(b" ")
                self._fh.flush()
            self._fh.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except (OSError, IOError):
            self.release()
            return False

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            self._fh.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except (OSError, IOError):
            pass
        try:
            self._fh.close()
        finally:
            self._fh = None


def node_key(node: dict) -> str:
    """稳定节点标识：与 Mihomo 运行时分配的端口号无关。"""
    raw = "|".join(str(node.get(k, "")) for k in ("name", "server", "port", "type"))
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:16]


def _jittered_interval(base: float) -> float:
    pct = max(0.0, min(0.5, float(PROXY_DAEMON_RECHECK_JITTER_PCT or 0)))
    if pct <= 0 or base <= 0:
        return float(base)
    return max(1.0, base * (1.0 + random.uniform(-pct, pct)))


def network_prefix(ip: str) -> str:
    try:
        addr = ipaddress.ip_address(str(ip or "").strip())
        return str(ipaddress.ip_network(f"{addr}/{24 if addr.version == 4 else 48}", strict=False))
    except ValueError:
        return ""


def touch_crawl_activity(*, active: bool = True, source: str = "") -> None:
    """抓取侧/API 写入活动心跳，供守护进程判断是否空闲放缓。"""
    try:
        _atomic_write_json(PROXY_DAEMON_ACTIVITY_FILE, {
            "active": bool(active),
            "source": source,
            "updated_at": _utc_now(),
            "ts": time.time(),
        })
    except Exception as e:
        log.debug("[daemon] 写入活动心跳失败: %s", e)


@dataclass
class NodeState:
    key: str
    node: dict
    port: int = 0
    state: str = STATE_NEW
    exit_ip: str = ""
    amazon_ok: bool = False
    last_checked_at: float = 0.0
    next_check_at: float = 0.0
    consecutive_failures: int = 0
    last_reason: str = ""
    last_error_code: str = ""
    verified_since: float = 0.0
    light_checks_since_full: int = 0
    feedback_successes: int = 0
    feedback_captcha: int = 0
    feedback_rate_limited: int = 0
    feedback_forbidden: int = 0
    feedback_network_errors: int = 0
    feedback_consecutive_errors: int = 0
    latency_ewma_ms: float = 0.0
    last_success_at: float = 0.0
    last_feedback_at: float = 0.0
    feedback_recheck: bool = False

    def quality_score(self) -> float:
        errors = (
            self.feedback_captcha + self.feedback_rate_limited
            + self.feedback_forbidden + self.feedback_network_errors
        )
        total = self.feedback_successes + errors
        success_rate = (self.feedback_successes + 2.0) / (total + 4.0)
        latency_penalty = min(0.30, max(0.0, self.latency_ewma_ms - 800.0) / 8000.0)
        # 长期错误由成功率吸收；额外风险惩罚只看连续错误，避免节点
        # 因为历史上出现过验证码就永久失去流量。
        risk_penalty = min(0.40, self.feedback_consecutive_errors * 0.10)
        freshness = 0.08 if self.last_success_at and time.time() - self.last_success_at < 1200 else 0.0
        return max(0.05, min(1.5, success_rate + freshness - latency_penalty - risk_penalty))

    def to_public_dict(self) -> dict:
        return {
            "name": self.node.get("name"),
            "port": self.port,
            "state": self.state,
            "exit_ip": self.exit_ip,
            "amazon_ok": self.amazon_ok,
            "consecutive_failures": self.consecutive_failures,
            "last_reason": self.last_reason,
            "last_error_code": self.last_error_code,
            "last_checked_at": self.last_checked_at,
            "next_check_at": self.next_check_at,
            "light_checks_since_full": self.light_checks_since_full,
            "quality_score": round(self.quality_score(), 4),
            "feedback_successes": self.feedback_successes,
            "feedback_errors": (
                self.feedback_captcha + self.feedback_rate_limited
                + self.feedback_forbidden + self.feedback_network_errors
            ),
            "latency_ewma_ms": round(self.latency_ewma_ms, 1),
            "last_success_at": self.last_success_at,
            "last_feedback_at": self.last_feedback_at,
        }


class ProxyDaemon:
    """调度器：连续从候选节点里挑“最紧急”的做验证，增量发布活池。"""

    def __init__(self):
        self._lock = threading.RLock()
        self._publish_lock = threading.Lock()
        self._states: dict[str, NodeState] = {}
        self._live_keys: set[str] = set()
        self._verified_ip_owner: dict[str, str] = {}
        self._inflight: set[str] = set()
        self._port_map: dict[str, int] = {}  # node_key -> port
        self._free_ports: list[int] = []
        self._fingerprint: ProfileFingerprint | None = None
        self._pending_fp_sha: str = ""
        self._pending_fp_hits: int = 0
        self._runtime_ok = False
        self._runtime_dirty = True
        self._runtime_retry_at = 0.0
        self._last_subscription_poll = 0.0
        self._refs_cache: set[str] = set()
        self._refs_cache_at = 0.0
        self._dirty = True
        self._last_publish_at = 0.0
        self._idle_mode = False
        self._pending_runtime_op = ""
        self._next_exploration_at = 0.0
        self._feedback_recheck_at: dict[tuple[str, str], float] = {}
        self._feedback_events_consumed = 0
        self._target_pause_until: dict[str, float] = {}
        self._stop_event = threading.Event()
        self._executor = ThreadPoolExecutor(
            max_workers=max(1, PROXY_DAEMON_CONCURRENCY), thread_name_prefix="proxyd"
        )
        self.run_id = f"DAEMON-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
        self._load_port_map()

    # ── 生命周期 ──────────────────────────────────────────────────
    def stop(self, *_args):
        self._stop_event.set()

    def bootstrap(self) -> None:
        self._maybe_reload_subscription(force=True)
        if self._runtime_dirty:
            self._sync_runtime()

    def tick(self) -> None:
        self._refresh_idle_mode()
        self._maybe_reload_subscription()
        self._consume_feedback()
        # Mihomo 意外退出时重新拉起（不依赖订阅变化）
        if self._runtime_ok and not owned_mihomo_running():
            log.warning("[daemon] 检测到 Mihomo 已退出，标记重新同步")
            with self._lock:
                self._runtime_ok = False
                self._runtime_dirty = True
                self._pending_runtime_op = "sync"
        if not self._runtime_ok and self._states and not self._runtime_dirty:
            with self._lock:
                self._runtime_dirty = True
                self._pending_runtime_op = "sync"
        if self._runtime_dirty:
            self._sync_runtime()
        if self._runtime_ok:
            self._dispatch_checks()
        self._maybe_publish()

    def run_forever(self) -> None:
        self._write_pid_file()
        log.info("[daemon] 启动 run_id=%s pid=%s", self.run_id, os.getpid())
        try:
            self.bootstrap()
            self._maybe_publish(force=True)
            while not self._stop_event.is_set():
                try:
                    self.tick()
                except Exception:
                    log.exception("[daemon] tick 异常，继续下一轮")
                deadline = time.monotonic() + PROXY_DAEMON_TICK_INTERVAL_SEC
                while not self._stop_event.is_set() and time.monotonic() < deadline:
                    if self._file_stop_requested():
                        log.info("[daemon] 收到优雅停止请求 run_id=%s", self.run_id)
                        self._stop_event.set()
                        break
                    self._stop_event.wait(min(0.25, max(0.0, deadline - time.monotonic())))
        finally:
            self._shutdown()

    def _file_stop_requested(self) -> bool:
        if not os.path.isfile(_DAEMON_STOP_FILE):
            return False
        try:
            with open(_DAEMON_STOP_FILE, encoding="utf-8") as f:
                rec = json.load(f) or {}
            return int(rec.get("pid") or 0) in (0, os.getpid())
        except Exception:
            return False

    def _shutdown(self) -> None:
        log.info("[daemon] 关闭中 run_id=%s", self.run_id)
        self._executor.shutdown(wait=True, cancel_futures=True)
        try:
            stop_owned_mihomo()
        except Exception:
            log.exception("[daemon] 停止 Mihomo 异常")
        self._save_port_map()
        self._remove_pid_file()
        try:
            if os.path.exists(_DAEMON_STOP_FILE):
                os.remove(_DAEMON_STOP_FILE)
        except OSError:
            pass

    # ── PID / 端口映射 ────────────────────────────────────────────
    def _write_pid_file(self) -> None:
        _atomic_write_json(PROXY_DAEMON_PID_FILE, {
            "pid": os.getpid(),
            "run_id": self.run_id,
            "started_at": _utc_now(),
        })

    @staticmethod
    def _remove_pid_file() -> None:
        try:
            if os.path.exists(PROXY_DAEMON_PID_FILE):
                os.remove(PROXY_DAEMON_PID_FILE)
        except OSError:
            pass

    def _load_port_map(self) -> None:
        if not os.path.isfile(PROXY_DAEMON_PORT_MAP_FILE):
            return
        try:
            with open(PROXY_DAEMON_PORT_MAP_FILE, encoding="utf-8") as f:
                data = json.load(f) or {}
            mapping = data.get("map") or {}
            self._port_map = {str(k): int(v) for k, v in mapping.items()}
            used = set(self._port_map.values())
            self._free_ports = [
                p for p in range(PROXY_BASE_PORT, PROXY_PORT_RANGE_END + 1)
                if p not in used
            ]
        except Exception as e:
            log.warning("[daemon] 端口映射加载失败: %s", e)
            self._port_map = {}
            self._free_ports = []

    def _save_port_map(self) -> None:
        try:
            _atomic_write_json(PROXY_DAEMON_PORT_MAP_FILE, {
                "updated_at": _utc_now(),
                "map": self._port_map,
            })
        except Exception as e:
            log.warning("[daemon] 端口映射保存失败: %s", e)

    def _ensure_port_locked(self, key: str) -> int:
        if key in self._port_map:
            port = self._port_map[key]
            if key in self._states:
                self._states[key].port = port
            return port
        if not self._free_ports:
            used = set(self._port_map.values())
            self._free_ports = [
                p for p in range(PROXY_BASE_PORT, PROXY_PORT_RANGE_END + 1)
                if p not in used
            ]
        if not self._free_ports:
            raise RuntimeError("无可用本地端口可分配")
        port = self._free_ports.pop(0)
        self._port_map[key] = port
        if key in self._states:
            self._states[key].port = port
        return port

    def _release_port_locked(self, key: str) -> None:
        port = self._port_map.pop(key, None)
        if port is not None and port not in self._free_ports:
            self._free_ports.append(port)
            self._free_ports.sort()

    # ── 空闲模式 ──────────────────────────────────────────────────
    def _refresh_idle_mode(self) -> None:
        active = self._is_crawl_active()
        idle = not active
        if idle != self._idle_mode:
            self._idle_mode = idle
            log.info("[daemon] 模式切换 → %s run_id=%s", "idle" if idle else "active", self.run_id)

    def _is_crawl_active(self) -> bool:
        # 1) 显式活动心跳
        try:
            if os.path.isfile(PROXY_DAEMON_ACTIVITY_FILE):
                with open(PROXY_DAEMON_ACTIVITY_FILE, encoding="utf-8") as f:
                    rec = json.load(f) or {}
                ts = float(rec.get("ts") or 0)
                if rec.get("active") and ts and (time.time() - ts) <= PROXY_DAEMON_IDLE_AFTER_SEC:
                    return True
        except Exception:
            pass
        # 2) 代理生命周期状态文件（running/preparing 等）
        try:
            if os.path.isfile(PROXY_POOL_STATUS_FILE):
                with open(PROXY_POOL_STATUS_FILE, encoding="utf-8") as f:
                    st = json.load(f) or {}
                status = str(st.get("status") or "")
                if status in (
                    "running", "preparing_proxy", "proxy_ready",
                    "starting_crawler", "stopping",
                ):
                    return True
        except Exception:
            pass
        return False

    def _effective_concurrency(self) -> int:
        if self._idle_mode:
            return max(1, PROXY_DAEMON_IDLE_CONCURRENCY)
        return max(1, PROXY_DAEMON_CONCURRENCY)

    def _effective_recheck_interval(self) -> float:
        base = float(PROXY_DAEMON_RECHECK_INTERVAL_SEC)
        if self._idle_mode:
            base *= max(1, PROXY_DAEMON_IDLE_RECHECK_MULTIPLIER)
        return base

    # ── 订阅加载 / 差量 + 指纹防抖 ────────────────────────────────
    def _maybe_reload_subscription(self, force: bool = False) -> None:
        now = time.monotonic()
        poll_interval = PROXY_DAEMON_SUBSCRIPTION_POLL_SEC if self._states else 10
        if not force and now - self._last_subscription_poll < poll_interval:
            return
        self._last_subscription_poll = now
        loaded = load_candidate_nodes()
        if not loaded.ok or not loaded.fingerprint:
            log.warning("[daemon] 候选节点加载失败: %s", loaded.error)
            return

        fp = loaded.fingerprint
        incoming = {node_key(n): n for n in loaded.nodes}

        with self._lock:
            current_sha = self._fingerprint.sha256 if self._fingerprint else ""
            if not force and fp.sha256 == current_sha and set(incoming) == set(self._states):
                self._pending_fp_sha = ""
                self._pending_fp_hits = 0
                # 仍刷新节点 dict（密码轮换等），但不触发运行时重建
                for key in set(incoming) & set(self._states):
                    self._states[key].node = incoming[key]
                return

            need_confirm = (
                not force
                and self._fingerprint is not None
                and PROXY_DAEMON_SUBSCRIPTION_CONFIRM_POLLS > 1
            )
            if need_confirm:
                if fp.sha256 == self._pending_fp_sha:
                    self._pending_fp_hits += 1
                else:
                    self._pending_fp_sha = fp.sha256
                    self._pending_fp_hits = 1
                if self._pending_fp_hits < PROXY_DAEMON_SUBSCRIPTION_CONFIRM_POLLS:
                    log.info(
                        "[daemon] 订阅指纹变化待确认 %d/%d sha=%s…",
                        self._pending_fp_hits, PROXY_DAEMON_SUBSCRIPTION_CONFIRM_POLLS,
                        fp.sha256[:12],
                    )
                    return
                self._pending_fp_sha = ""
                self._pending_fp_hits = 0

            self._fingerprint = fp
            existing_keys = set(self._states)
            new_keys = set(incoming) - existing_keys
            removed_keys = existing_keys - set(incoming)
            for key in new_keys:
                self._states[key] = NodeState(key=key, node=incoming[key])
                self._ensure_port_locked(key)
            for key in removed_keys:
                self._evict_key_locked(key)
                self._release_port_locked(key)
            for key in existing_keys & set(incoming):
                self._states[key].node = incoming[key]
                self._ensure_port_locked(key)
            if new_keys or removed_keys:
                self._runtime_dirty = True
                # 纯新增且实例已存活 → 热重载；否则完整同步
                self._pending_runtime_op = (
                    "hot_add" if new_keys and not removed_keys and self._runtime_ok
                    else "sync"
                )
            self._save_port_map()

        if new_keys or removed_keys:
            log.info(
                "[daemon] 订阅节点变化 +%d -%d (candidates=%d) run_id=%s",
                len(new_keys), len(removed_keys), len(self._states), self.run_id,
            )

    def _evict_key_locked(self, key: str) -> None:
        state = self._states.pop(key, None)
        if state is None:
            return
        if key in self._live_keys:
            self._live_keys.discard(key)
            self._mark_dirty_locked()
        if state.exit_ip and self._verified_ip_owner.get(state.exit_ip) == key:
            self._verified_ip_owner.pop(state.exit_ip, None)
        self._inflight.discard(key)

    # ── Mihomo 运行时同步（热重载优先，蓝绿兜底）──────────────────
    def _nodes_and_ports_locked(self) -> tuple[list[str], list[dict], list[int]]:
        keys = sorted(self._states.keys())
        nodes, ports = [], []
        for k in keys:
            self._ensure_port_locked(k)
            nodes.append(self._states[k].node)
            ports.append(self._states[k].port)
        return keys, nodes, ports

    def _sync_runtime(self) -> None:
        now = time.monotonic()
        if now < self._runtime_retry_at:
            return
        with self._lock:
            self._runtime_dirty = False
            op = getattr(self, "_pending_runtime_op", "sync") or "sync"
            self._pending_runtime_op = ""
            keys, nodes, ports = self._nodes_and_ports_locked()

        if not nodes:
            log.warning("[daemon] 当前无候选节点，暂停独立 Mihomo run_id=%s", self.run_id)
            stop_owned_mihomo()
            with self._lock:
                self._runtime_ok = False
                self._live_keys.clear()
                self._verified_ip_owner.clear()
            self._mark_dirty()
            return

        mihomo_alive = bool(owned_mihomo_running())

        # 纯新增且实例存活：热重载，不碰已有活池
        if op == "hot_add" and mihomo_alive:
            log.info("[daemon] 热重载增量配置 run_id=%s nodes=%d", self.run_id, len(nodes))
            result = reload_mihomo_config(nodes, ports)
            if result.ok:
                with self._lock:
                    self._runtime_ok = True
                log.info("[daemon] 热重载成功 run_id=%s listening=%d", self.run_id, len(result.ports))
                return
            log.warning("[daemon] 热重载失败，回退完整同步: %s", result.error)

        # 一般同步：优先热重载（端口稳定时保留活池）
        if mihomo_alive:
            result = reload_mihomo_config(nodes, ports)
            if result.ok:
                with self._lock:
                    self._runtime_ok = True
                    # 端口未变：保留已验证节点；仅对监听失败的做剔除。监听正常的
                    # 也不能直接信任旧状态——配置刚重载，隧道是否真的还通需要尽快
                    # 复核确认，因此强制其立刻进入下一轮检查（而非沿用旧的复核计划）。
                    dead = []
                    for k in list(self._live_keys):
                        st = self._states.get(k)
                        if not st or not check_listener(st.port).get("ok"):
                            dead.append(k)
                        else:
                            st.next_check_at = 0.0
                    for k in dead:
                        st = self._states.get(k)
                        if st:
                            st.state = STATE_NEW
                            st.next_check_at = 0.0
                            st.light_checks_since_full = 0
                        self._live_keys.discard(k)
                        if st and st.exit_ip and self._verified_ip_owner.get(st.exit_ip) == k:
                            self._verified_ip_owner.pop(st.exit_ip, None)
                if dead:
                    self._mark_dirty()
                log.info(
                    "[daemon] 配置热重载完成 run_id=%s nodes=%d live=%d",
                    self.run_id, len(nodes), len(self._live_keys),
                )
                return
            log.warning("[daemon] 热重载失败，尝试蓝绿切换: %s", result.error)
            bg = start_mihomo_blue_green(
                nodes, ports, alt_base_port=PROXY_DAEMON_BLUE_GREEN_BASE_PORT,
            )
            with self._lock:
                self._runtime_ok = bool(bg.ok)
                if not self._runtime_ok:
                    self._runtime_retry_at = time.monotonic() + 30
                else:
                    # 蓝绿后端口回到主端口：保留 VERIFIED 状态，只做监听体检；
                    # 监听正常的也强制立刻复核，避免长期沿用重启前的旧结论
                    for k in list(self._live_keys):
                        st = self._states.get(k)
                        if st and not check_listener(st.port).get("ok"):
                            st.state = STATE_NEW
                            st.next_check_at = 0.0
                            self._live_keys.discard(k)
                        elif st:
                            st.next_check_at = 0.0
            if bg.ok:
                log.info("[daemon] 蓝绿切换成功 run_id=%s nodes=%d", self.run_id, len(nodes))
                self._mark_dirty()
            else:
                log.warning(
                    "[daemon] 蓝绿失败 run_id=%s error=%s", self.run_id, bg.error,
                )
            return

        # 冷启动
        log.info("[daemon] 冷启动独立 Mihomo run_id=%s nodes=%d", self.run_id, len(nodes))
        result = start_mihomo(nodes, ports=ports)
        with self._lock:
            self._runtime_ok = bool(result.ok)
            if not self._runtime_ok:
                self._runtime_retry_at = time.monotonic() + 30
                # 冷启动失败时不保留可能失真的活池
                self._live_keys.clear()
                self._verified_ip_owner.clear()
            else:
                # 冷启动成功：已验证节点仍保留状态，但需重新监听确认后才能留在活池
                kept = set()
                for k, st in self._states.items():
                    if st.state == STATE_VERIFIED and check_listener(st.port).get("ok"):
                        kept.add(k)
                        st.next_check_at = 0.0
                    elif st.state == STATE_VERIFIED:
                        st.state = STATE_NEW
                        st.next_check_at = 0.0
                        st.light_checks_since_full = 0
                self._live_keys = kept
        self._mark_dirty()
        if not result.ok:
            log.warning(
                "[daemon] Mihomo 启动失败 run_id=%s error=%s error_code=%s",
                self.run_id, result.error, result.error_code,
            )

    # ── 验证调度 ──────────────────────────────────────────────────
    def _get_reference_ips_cached(self) -> set[str]:
        now = time.monotonic()
        with self._lock:
            if self._refs_cache_at and now - self._refs_cache_at < _REFS_CACHE_TTL_SEC:
                return set(self._refs_cache)
        refs = get_reference_ips()
        banned = {ip for ip in (refs.get("direct_ip"), refs.get("main_proxy_ip")) if ip}
        with self._lock:
            self._refs_cache = banned
            self._refs_cache_at = now
        return set(banned)

    def _select_tiers_locked(self) -> tuple[list[str], list[str]]:
        """按质量和出口网段选出热池，其余已验证节点作为温池。"""
        ranked = sorted(
            (
                self._states[key] for key in self._live_keys
                if key in self._states and self._states[key].state == STATE_VERIFIED
            ),
            key=lambda st: (st.quality_score(), st.last_success_at, st.verified_since),
            reverse=True,
        )
        hot: list[str] = []
        deferred: list[NodeState] = []
        prefix_counts: dict[str, int] = {}
        for st in ranked:
            if len(hot) >= PROXY_POOL_HOT_MAX_NODES:
                deferred.append(st)
                continue
            prefix = network_prefix(st.exit_ip)
            if prefix and prefix_counts.get(prefix, 0) >= PROXY_MAX_ACTIVE_PER_PREFIX:
                deferred.append(st)
                continue
            hot.append(st.key)
            if prefix:
                prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1

        # 候选多样性不足时不能因网段上限把热池压到启动门槛以下。
        if len(hot) < min(PROXY_POOL_HOT_MAX_NODES, len(ranked)):
            for st in deferred:
                if len(hot) >= PROXY_POOL_HOT_MAX_NODES:
                    break
                hot.append(st.key)
        hot_set = set(hot)
        warm = [st.key for st in ranked if st.key not in hot_set]
        return hot, warm

    def _consume_feedback(self) -> None:
        try:
            events = drain_proxy_events(limit=PROXY_DAEMON_FEEDBACK_BATCH)
        except Exception:
            log.exception("[daemon] 消费抓取侧代理反馈失败")
            return
        if not events:
            return

        # 同一目标、同一类错误若同时落在多个节点，先视为目标站异常，防止集体误杀。
        affected: dict[tuple[str, str], set[str]] = {}
        for event in events:
            if event.outcome != "SUCCESS":
                affected.setdefault((event.target, event.outcome), set()).add(event.node_key)
        # 只有明确的站点限流/服务不可用才暂停整个目标。CAPTCHA、TLS、连接
        # 错误都可能只属于单个出口；把它们升级成整站 90 秒暂停会形成反馈风暴。
        target_wide_outcomes = {"HTTP_429", "HTTP_503"}
        broad = {
            pair for pair, keys in affected.items()
            if pair[1] in target_wide_outcomes and len(keys) >= 3
        }
        now = time.time()

        with self._lock:
            hot_keys = set(self._select_tiers_locked()[0])
            self._feedback_events_consumed += len(events)
            for target, outcome in broad:
                self._target_pause_until[target] = max(
                    self._target_pause_until.get(target, 0.0),
                    now + (180.0 if outcome == "HTTP_429" else 90.0),
                )
            for event in events:
                st = self._states.get(event.node_key)
                if st is None:
                    continue
                st.last_feedback_at = max(st.last_feedback_at, event.created_at)
                if event.latency_ms > 0:
                    st.latency_ewma_ms = (
                        float(event.latency_ms) if st.latency_ewma_ms <= 0
                        else st.latency_ewma_ms * 0.8 + event.latency_ms * 0.2
                    )
                if event.outcome == "SUCCESS":
                    st.feedback_successes += 1
                    st.feedback_consecutive_errors = 0
                    st.last_success_at = max(st.last_success_at, event.created_at)
                    st.feedback_recheck = False
                    interval = (
                        PROXY_DAEMON_WARM_RECHECK_INTERVAL_SEC
                        if st.key not in hot_keys
                        else PROXY_DAEMON_RECHECK_INTERVAL_SEC
                    )
                    desired = max(st.next_check_at, now + _jittered_interval(interval))
                    # 真实抓取成功可取代频繁重复探测，但不能永久取代出口 IP/
                    # Amazon 全链路校验；每个节点最迟一小时仍做一次完整验证。
                    hard_deadline = (
                        st.last_checked_at + PROXY_DAEMON_FULL_SCAN_INTERVAL_SEC
                        if st.last_checked_at else desired
                    )
                    st.next_check_at = min(desired, hard_deadline)
                    continue

                st.feedback_consecutive_errors += 1
                if event.outcome == "CAPTCHA":
                    st.feedback_captcha += 1
                elif event.outcome == "HTTP_429":
                    st.feedback_rate_limited += 1
                elif event.outcome == "HTTP_403":
                    st.feedback_forbidden += 1
                else:
                    st.feedback_network_errors += 1

                immediate = event.outcome in {"CAPTCHA", "HTTP_429"}
                repeated = event.outcome == "HTTP_403" and st.feedback_forbidden >= 2
                repeated = repeated or (
                    event.outcome not in {"CAPTCHA", "HTTP_429", "HTTP_403"}
                    and st.feedback_consecutive_errors >= 2
                )
                dedupe_key = (event.node_key, event.outcome)
                if (
                    (event.target, event.outcome) not in broad
                    and (immediate or repeated)
                    and now >= self._feedback_recheck_at.get(dedupe_key, 0.0)
                ):
                    st.feedback_recheck = True
                    st.next_check_at = 0.0
                    self._feedback_recheck_at[dedupe_key] = now + PROXY_DAEMON_FEEDBACK_DEDUPE_SEC
            self._mark_dirty_locked()

    def _dispatch_checks(self) -> None:
        concurrency = self._effective_concurrency()
        with self._lock:
            slots = max(0, concurrency - len(self._inflight))
            if slots <= 0:
                return
            now = time.time()
            hot, _warm = self._select_tiers_locked()
            hot_set = set(hot)
            pool_at_target = len(hot) >= PROXY_POOL_TARGET_NODES
            due: list[tuple[int, float, NodeState]] = []
            exploration: list[NodeState] = []
            for k, st in self._states.items():
                if k in self._inflight:
                    continue
                if st.feedback_recheck:
                    due.append((0, st.next_check_at, st))
                elif st.state == STATE_VERIFIED and st.next_check_at <= now:
                    due.append((2 if k in hot_set else 3, st.next_check_at, st))
                elif st.state in {STATE_NEW, STATE_FAILED}:
                    if not pool_at_target and (st.state == STATE_NEW or st.next_check_at <= now):
                        due.append((1 if st.state == STATE_NEW else 4, st.next_check_at, st))
                    elif pool_at_target:
                        exploration.append(st)

            # 池稳定后仍低速探索，且任一候选最迟在全量周期内会被重验。
            if pool_at_target and exploration and time.monotonic() >= self._next_exploration_at:
                forced = [
                    st for st in exploration
                    if not st.last_checked_at
                    or now - st.last_checked_at >= PROXY_DAEMON_FULL_SCAN_INTERVAL_SEC
                ]
                candidates = forced or [st for st in exploration if st.next_check_at <= now]
                if candidates:
                    candidates.sort(key=lambda st: (st.last_checked_at, st.next_check_at, st.key))
                    due.append((5, candidates[0].next_check_at, candidates[0]))

            due.sort(key=lambda item: (item[0], item[1], item[2].key))
            chosen = [item[2] for item in due[:slots]]
            for st in chosen:
                self._inflight.add(st.key)
                if st.state in {STATE_NEW, STATE_FAILED} and pool_at_target:
                    self._next_exploration_at = (
                        time.monotonic() + PROXY_DAEMON_EXPLORATION_INTERVAL_SEC
                    )
                st.feedback_recheck = False
                # 决定本次是否轻量复核
                st._check_light = (
                    st.state == STATE_VERIFIED
                    and st.light_checks_since_full < max(1, PROXY_DAEMON_LIGHT_RECHECKS_BEFORE_FULL) - 1
                )
            if chosen:
                self._mark_dirty_locked()
        if not chosen:
            return
        banned_ips = self._get_reference_ips_cached()
        for st in chosen:
            self._executor.submit(
                self._check_one, st.key, banned_ips, bool(getattr(st, "_check_light", False)),
            )

    def _check_one(self, key: str, banned_ips: set[str], light: bool = False) -> None:
        try:
            with self._lock:
                st = self._states.get(key)
                if st is None:
                    return
                expected_ip = st.exit_ip
                entry = {
                    "name": st.node.get("name"),
                    "port": st.port,
                    "proxy": f"http://127.0.0.1:{st.port}",
                }
            listener = check_listener(entry["port"])
            if not listener["ok"]:
                self._on_check_result(
                    key, ok=False, reason="端口未监听", error_code="PORT_NOT_LISTENING",
                    exit_ip="", light=False,
                )
                return
            # 轻量哨兵：若出口变了，升级为完整校验
            result = verify_node(entry, banned_ips=banned_ips, light=light)
            if light and result.ok and expected_ip and result.exit_ip != expected_ip:
                result = verify_node(entry, banned_ips=banned_ips, light=False)
                light = False
            self._on_check_result(
                key, ok=result.ok, reason=result.reason, error_code=result.error_code,
                exit_ip=result.exit_ip, amazon_ok=result.amazon_ok, light=light,
            )
        except Exception as exc:
            log.exception("[daemon] 节点验证异常 key=%s", key)
            self._on_check_result(
                key, ok=False, reason=str(exc), error_code="CHECK_EXCEPTION",
                exit_ip="", light=False,
            )
        finally:
            with self._lock:
                self._inflight.discard(key)
                self._mark_dirty_locked()

    def _on_check_result(
        self, key: str, *, ok: bool, reason: str, error_code: str, exit_ip: str,
        amazon_ok: bool = False, light: bool = False,
    ) -> None:
        now = time.time()
        recheck = self._effective_recheck_interval()
        became_verified = False
        became_evicted = False
        with self._lock:
            st = self._states.get(key)
            if st is None:
                return
            st.last_checked_at = now
            st.last_reason = reason
            st.last_error_code = error_code

            if ok:
                dup_owner = self._verified_ip_owner.get(exit_ip)
                if dup_owner and dup_owner != key:
                    st.state = STATE_FAILED
                    st.exit_ip = exit_ip
                    st.amazon_ok = False
                    st.consecutive_failures = 0
                    st.last_reason = "出口IP重复(已由其它节点占用)"
                    st.last_error_code = "DUPLICATE_EXIT_IP"
                    st.next_check_at = now + _jittered_interval(recheck)
                    if key in self._live_keys:
                        self._live_keys.discard(key)
                        became_evicted = True
                else:
                    st.state = STATE_VERIFIED
                    st.exit_ip = exit_ip
                    if not light:
                        st.amazon_ok = amazon_ok
                        st.light_checks_since_full = 0
                    else:
                        st.light_checks_since_full += 1
                    st.consecutive_failures = 0
                    st.next_check_at = now + _jittered_interval(recheck)
                    if not st.verified_since:
                        st.verified_since = now
                    if exit_ip:
                        self._verified_ip_owner[exit_ip] = key
                    if key not in self._live_keys:
                        self._live_keys.add(key)
                        became_verified = True
                    hot, _warm = self._select_tiers_locked()
                    recheck = (
                        self._effective_recheck_interval()
                        if key in hot else PROXY_DAEMON_WARM_RECHECK_INTERVAL_SEC
                    )
                    st.next_check_at = now + _jittered_interval(recheck)
                    st.feedback_recheck = False
                    st.feedback_consecutive_errors = 0
            else:
                was_live = key in self._live_keys
                st.consecutive_failures += 1
                st.state = STATE_FAILED
                st.amazon_ok = False
                st.light_checks_since_full = 0
                backoff = min(
                    PROXY_DAEMON_RETRY_BACKOFF_MAX_SEC,
                    PROXY_DAEMON_RETRY_BACKOFF_BASE_SEC * (2 ** max(0, st.consecutive_failures - 1)),
                )
                st.next_check_at = now + _jittered_interval(backoff)
                if was_live:
                    self._live_keys.discard(key)
                    became_evicted = True
                if st.exit_ip and self._verified_ip_owner.get(st.exit_ip) == key:
                    self._verified_ip_owner.pop(st.exit_ip, None)
                st.exit_ip = ""

        if became_verified:
            log.info("[daemon] 节点通过验证并加入活池 run_id=%s key=%s", self.run_id, key)
        if became_evicted:
            log.warning(
                "[daemon] 节点被剔除出活池 run_id=%s key=%s reason=%s error_code=%s",
                self.run_id, key, reason, error_code,
            )
        # NEW/FAILED 节点即使没有改变活池成员，其 state/failed/pending
        # 也已变化，必须发布；否则达标后的低速探索会让面板永久停在
        # “验证中 1”。发布层已有 debounce，这里无需再判断状态类型。
        self._mark_dirty()

    # ── 发布（增量、去抖）────────────────────────────────────────
    def _mark_dirty(self) -> None:
        with self._publish_lock:
            self._dirty = True

    def _mark_dirty_locked(self) -> None:
        with self._publish_lock:
            self._dirty = True

    def _maybe_publish(self, force: bool = False) -> None:
        now = time.monotonic()
        with self._publish_lock:
            due = force or (
                self._dirty and now - self._last_publish_at >= PROXY_DAEMON_PUBLISH_DEBOUNCE_SEC
            )
            if not due:
                return
            self._dirty = False
            self._last_publish_at = now
        self._publish_pool()
        self._publish_status()

    def _publish_pool(self) -> None:
        with self._lock:
            hot, warm = self._select_tiers_locked()
            entries = []
            warm_entries = []
            for tier, keys, target in (("hot", hot, entries), ("warm", warm, warm_entries)):
                for key in keys:
                    st = self._states.get(key)
                    if not st:
                        continue
                    errors = (
                        st.feedback_captcha + st.feedback_rate_limited
                        + st.feedback_forbidden + st.feedback_network_errors
                    )
                    total = st.feedback_successes + errors
                    target.append({
                        "name": st.node.get("name"),
                        "port": st.port,
                        "proxy": f"http://127.0.0.1:{st.port}",
                        "node_key": st.key,
                        "exit_ip": st.exit_ip,
                        "network_prefix": network_prefix(st.exit_ip),
                        "amazon_ok": st.amazon_ok,
                        "captcha": False,
                        "tier": tier,
                        "quality_score": round(st.quality_score(), 4),
                        "success_rate": round((st.feedback_successes + 2) / (total + 4), 4),
                        "latency_ewma_ms": round(st.latency_ewma_ms, 1),
                        "last_success_at": st.last_success_at,
                        "last_checked_at": st.last_checked_at,
                    })
            fp = self._fingerprint
            verified_nodes = len(self._live_keys)
            target_pauses = {
                key: value for key, value in self._target_pause_until.items()
                if value > time.time()
            }
        payload = {
            "run_id": self.run_id,
            "published_at": _utc_now(),
            "profile_uid": fp.uid if fp else "",
            "profile_sha256": fp.sha256 if fp else "",
            "profile_updated_at": str(fp.updated_at) if fp else "",
            "stats": {
                "source": "proxy_daemon",
                "verified_nodes": verified_nodes,
                "hot_nodes": len(entries),
                "warm_nodes": len(warm_entries),
                "unique_ips": verified_nodes,
                "amazon_ok": verified_nodes,
            },
            "entries": entries,
            "warm_entries": warm_entries,
            "target_pause_until": target_pauses,
        }
        _atomic_write_json(PROXY_POOL_FILE, payload)

    def _publish_status(self) -> None:
        with self._lock:
            total = len(self._states)
            verified = len(self._live_keys)
            hot, warm = self._select_tiers_locked()
            checking = len(self._inflight)
            failed = sum(1 for st in self._states.values() if st.state == STATE_FAILED)
            pending_new = sum(1 for st in self._states.values() if st.state == STATE_NEW)
            fp = self._fingerprint
            runtime_ok = self._runtime_ok
            idle = self._idle_mode
        payload = {
            "daemon_pid": os.getpid(),
            "run_id": self.run_id,
            "updated_at": _utc_now(),
            "runtime_ok": runtime_ok,
            "idle_mode": idle,
            "start_gate": PROXY_MIN_START_NODES,
            "low_watermark": PROXY_POOL_LOW_WATERMARK,
            "target_pool_size": PROXY_POOL_TARGET_NODES,
            "hot_pool_max": PROXY_POOL_HOT_MAX_NODES,
            "start_ready": len(hot) >= PROXY_MIN_START_NODES,
            "pool_low": len(hot) < PROXY_POOL_LOW_WATERMARK,
            "candidates": total,
            "verified": verified,
            "hot": len(hot),
            "warm": len(warm),
            "checking": checking,
            "failed": failed,
            "pending": pending_new,
            "fingerprint": fp.to_dict() if fp else None,
            "last_subscription_poll_age_sec": max(0, int(time.monotonic() - self._last_subscription_poll)),
            "feedback_events_consumed": self._feedback_events_consumed,
            "target_pause_until": {
                key: value for key, value in self._target_pause_until.items()
                if value > time.time()
            },
        }
        _atomic_write_json(PROXY_DAEMON_STATE_FILE, payload)


# ── 供 proxy_pool_manager / api_server 复用的进程级控制 ────────────
def is_daemon_alive(pid_file: str | None = None) -> int | None:
    path = pid_file or PROXY_DAEMON_PID_FILE
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            rec = json.load(f)
        pid = int(rec.get("pid"))
    except Exception:
        return None
    return pid if pid_alive(pid) else None


def read_daemon_state() -> dict | None:
    if not os.path.isfile(PROXY_DAEMON_STATE_FILE):
        return None
    try:
        with open(PROXY_DAEMON_STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def request_daemon_stop(pid: int | None = None) -> None:
    """请求守护进程优雅退出；由其 finally 负责清理 Mihomo 与 PID 文件。"""
    target = int(pid or is_daemon_alive() or 0)
    _atomic_write_json(_DAEMON_STOP_FILE, {
        "pid": target,
        "requested_at": _utc_now(),
        "requested_by": os.getpid(),
    })


def _install_signal_handlers(daemon: ProxyDaemon) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, daemon.stop)
        except (ValueError, OSError):
            pass


def _configure_logging() -> None:
    os.makedirs(os.path.dirname(PROXY_DAEMON_LOG_FILE) or ".", exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(PROXY_DAEMON_LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="常驻代理验证守护进程")
    parser.add_argument("--once", action="store_true", help="只执行一次调度 tick 后退出")
    parser.add_argument("--status", action="store_true", help="打印当前状态文件内容后退出")
    args = parser.parse_args()

    if args.status:
        state = read_daemon_state()
        print(json.dumps(state or {"error": "无状态文件"}, ensure_ascii=False, indent=2))
        return 0

    _configure_logging()
    instance_lock = DaemonInstanceLock()
    if not instance_lock.acquire():
        log.warning("[daemon] 已有实例持有单实例锁，本进程退出 pid=%s", os.getpid())
        return 0

    # 上一轮强杀可能留下过期停止请求；取得单实例锁后可安全清除。
    try:
        if os.path.exists(_DAEMON_STOP_FILE):
            os.remove(_DAEMON_STOP_FILE)
    except OSError:
        pass

    daemon = ProxyDaemon()
    _install_signal_handlers(daemon)

    try:
        if args.once:
            daemon.bootstrap()
            daemon.tick()
            daemon._shutdown()
            return 0

        daemon.run_forever()
        return 0
    finally:
        instance_lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
