"""
proxy_pool_manager.py — 代理池编排：锁、状态机、验证、原子发布、最低阈值。

关键路径不依赖 Clash 9097。probe_results.json 仅为带指纹的缓存。
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from config import (
    PROXY_BASE_PORT,
    PROXY_LOCK_FILE,
    PROXY_MIN_AMAZON_OK,
    PROXY_MIN_UNIQUE_IPS,
    PROXY_MIN_VERIFIED_NODES,
    PROXY_POOL_CANDIDATE_FILE,
    PROXY_POOL_FILE,
    PROXY_POOL_LAST_FAILED_FILE,
    PROXY_POOL_LAST_GOOD_FILE,
    PROXY_POOL_STATUS_FILE,
    PROXY_PROBE_CACHE_FILE,
    PROXY_PROBE_CACHE_TTL,
)
from proxy_health import (
    get_reference_ips,
    pool_entries_from_health,
    verify_pool,
)
from proxy_node_source import ProfileFingerprint, load_candidate_nodes
from proxy_runtime import (
    check_listener,
    owned_mihomo_running,
    start_mihomo,
    stop_owned_mihomo,
)

log = logging.getLogger("proxy_pool_manager")

STATUS_IDLE = "idle"
STATUS_PREPARING = "preparing_proxy"
STATUS_PROXY_READY = "proxy_ready"
STATUS_STARTING_CRAWLER = "starting_crawler"
STATUS_RUNNING = "running"
STATUS_PROXY_FAILED = "proxy_failed"
STATUS_STOPPING = "stopping"

_state_lock = threading.RLock()
_refresh_lock = threading.Lock()
_status = {
    "status": STATUS_IDLE,
    "run_id": "",
    "updated_at": "",
    "detail": {},
}


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


def _read_json(path: str):
    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def set_status(status: str, run_id: str = "", **detail) -> dict:
    with _state_lock:
        _status["status"] = status
        if run_id:
            _status["run_id"] = run_id
        _status["updated_at"] = _utc_now()
        if detail:
            _status["detail"] = {**(_status.get("detail") or {}), **detail}
        snap = dict(_status)
        snap["detail"] = dict(_status.get("detail") or {})
    try:
        _atomic_write_json(PROXY_POOL_STATUS_FILE, snap)
    except Exception as e:
        log.warning("[status] 写入失败: %s", e)
    return snap


def get_status() -> dict:
    with _state_lock:
        return {
            "status": _status.get("status"),
            "run_id": _status.get("run_id"),
            "updated_at": _status.get("updated_at"),
            "detail": dict(_status.get("detail") or {}),
        }


class RefreshLock:
    """单实例刷新锁：防止第二次点击重复拉起 Mihomo。"""

    def __init__(self, path: str = PROXY_LOCK_FILE):
        self.path = path
        self._fh = None

    def acquire(self, timeout: float = 0.0) -> bool:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        deadline = time.time() + max(0.0, timeout)
        while True:
            try:
                self._fh = open(self.path, "a+", encoding="utf-8")
                if os.name == "nt":
                    import msvcrt
                    self._fh.seek(0)
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                self._fh.seek(0)
                self._fh.truncate()
                self._fh.write(json.dumps({
                    "pid": os.getpid(),
                    "acquired_at": _utc_now(),
                }))
                self._fh.flush()
                return True
            except OSError:
                if self._fh:
                    try:
                        self._fh.close()
                    except Exception:
                        pass
                    self._fh = None
                if time.time() >= deadline:
                    return False
                time.sleep(0.2)

    def release(self) -> None:
        if not self._fh:
            return
        try:
            if os.name == "nt":
                import msvcrt
                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except Exception as e:
            log.warning("[lock] 释放失败: %s", e)
        try:
            self._fh.close()
        except Exception:
            pass
        self._fh = None


@dataclass
class PrepareResult:
    ok: bool
    status: str
    run_id: str
    candidate_nodes: int = 0
    verified_nodes: int = 0
    unique_ips: int = 0
    amazon_ok: int = 0
    reason: str = ""
    error_code: str = ""
    fingerprint: dict | None = None
    stats: dict = field(default_factory=dict)
    fail_reasons: dict = field(default_factory=dict)
    pool_path: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _cache_valid(cache: dict, fp: ProfileFingerprint) -> bool:
    if not isinstance(cache, dict):
        return False
    if not cache.get("results"):
        return False
    if cache.get("profile_uid") != fp.uid:
        return False
    if cache.get("profile_sha256") != fp.sha256:
        return False
    if str(cache.get("profile_updated_at", "")) != str(fp.updated_at):
        return False
    if cache.get("aborted"):
        return False
    probed_at = cache.get("probed_at")
    if not probed_at:
        return False
    try:
        # 支持 ISO 或 epoch
        if isinstance(probed_at, (int, float)):
            age = time.time() - float(probed_at)
        else:
            ts = datetime.fromisoformat(str(probed_at).replace("Z", "+00:00")).timestamp()
            age = time.time() - ts
    except Exception:
        return False
    return age <= PROXY_PROBE_CACHE_TTL


def write_probe_cache(fp: ProfileFingerprint, results: list[dict], *, aborted: bool = False) -> None:
    payload = {
        "profile_uid": fp.uid,
        "profile_sha256": fp.sha256,
        "profile_updated_at": str(fp.updated_at),
        "probed_at": _utc_now(),
        "aborted": aborted,
        "results": results,
    }
    _atomic_write_json(PROXY_PROBE_CACHE_FILE, payload)


def read_probe_cache(fp: ProfileFingerprint) -> list[dict] | None:
    try:
        cache = _read_json(PROXY_PROBE_CACHE_FILE)
    except Exception as e:
        log.warning("[cache] 读取失败: %s", e)
        return None
    if not _cache_valid(cache or {}, fp):
        return None
    return list(cache.get("results") or [])


def _meets_thresholds(verified_nodes: int, unique_ips: int, amazon_ok: int) -> tuple[bool, str]:
    if verified_nodes < PROXY_MIN_VERIFIED_NODES:
        return False, f"verified_nodes={verified_nodes} < {PROXY_MIN_VERIFIED_NODES}"
    if unique_ips < PROXY_MIN_UNIQUE_IPS:
        return False, f"unique_ips={unique_ips} < {PROXY_MIN_UNIQUE_IPS}"
    if amazon_ok < PROXY_MIN_AMAZON_OK:
        return False, f"amazon_ok={amazon_ok} < {PROXY_MIN_AMAZON_OK}"
    return True, ""


def _publish_pool(entries: list[dict], fp: ProfileFingerprint, run_id: str, stats: dict) -> str:
    payload = {
        "run_id": run_id,
        "published_at": _utc_now(),
        "profile_uid": fp.uid,
        "profile_sha256": fp.sha256,
        "profile_updated_at": str(fp.updated_at),
        "stats": stats,
        "entries": entries,
    }
    # 兼容旧消费者：同时保留顶层 list 视图在 candidate，正式文件用带元数据对象
    # 抓取侧 proxy_session 已支持 dict.entries / list 两种格式
    _atomic_write_json(PROXY_POOL_CANDIDATE_FILE, payload)
    _atomic_write_json(PROXY_POOL_FILE, payload)
    _atomic_write_json(PROXY_POOL_LAST_GOOD_FILE, payload)
    return PROXY_POOL_FILE


_UUID_RE = re.compile(
    r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
)
_SENSITIVE_KV_RE = re.compile(
    r"(?i)\b(uuid|password|psk|secret|auth|token)\b\s*[:=]\s*\S+"
)


def _redact_log_text(text: str) -> str:
    """审计/失败记录里嵌入的 Mihomo 原始日志需先脱敏，防止配置报错回显凭据。"""
    if not text:
        return text
    text = _UUID_RE.sub("***", text)
    text = _SENSITIVE_KV_RE.sub(lambda m: f"{m.group(1)}=***", text)
    return text


def _record_failure(run_id: str, payload: dict) -> None:
    if "log_tail" in payload:
        payload = {**payload, "log_tail": _redact_log_text(payload.get("log_tail", ""))}
    data = {"run_id": run_id, "failed_at": _utc_now(), **payload}
    try:
        _atomic_write_json(PROXY_POOL_LAST_FAILED_FILE, data)
    except Exception as e:
        log.warning("[publish] 失败记录写入异常: %s", e)


def prepare_proxy_pool(
    *,
    max_nodes: int | None = None,
    stop_owned_after: bool = False,
    force: bool = True,
) -> PrepareResult:
    """
    完整构建并原子发布代理池。
    force=True：每次开始都重新验证（默认，符合验收要求）。
    """
    run_id = f"PROXY-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    lock = RefreshLock()
    if not lock.acquire(timeout=0.5):
        return PrepareResult(
            ok=False, status=STATUS_PROXY_FAILED, run_id=run_id,
            reason="已有代理池刷新在进行中", error_code="REFRESH_IN_PROGRESS",
        )

    set_status(STATUS_PREPARING, run_id=run_id, phase="acquire_lock")
    try:
        try:
            with _refresh_lock:
                return _prepare_locked(
                    run_id,
                    max_nodes=max_nodes,
                    stop_owned_after=stop_owned_after,
                    force=force,
                )
        except Exception as exc:
            # 任意未预期异常都必须失败关闭：不能残留独立 Mihomo，也不能让调用方
            # 把异常后的半成品状态当作可用代理池。
            log.exception("[prepare] 未处理异常 run_id=%s", run_id)
            cleanup = stop_owned_mihomo()
            reason = str(exc) or type(exc).__name__
            payload = {
                "reason": reason,
                "error_code": "PREPARE_EXCEPTION",
                "exception_type": type(exc).__name__,
                "cleanup": cleanup.to_dict(),
            }
            _record_failure(run_id, payload)
            set_status(STATUS_PROXY_FAILED, run_id=run_id, **payload)
            return PrepareResult(
                ok=False,
                status=STATUS_PROXY_FAILED,
                run_id=run_id,
                reason=reason,
                error_code="PREPARE_EXCEPTION",
            )
    finally:
        lock.release()


def _prepare_locked(
    run_id: str,
    *,
    max_nodes: int | None,
    stop_owned_after: bool,
    force: bool,
) -> PrepareResult:
    cold_started_at = time.monotonic()
    set_status(STATUS_PREPARING, run_id=run_id, phase="load_nodes")
    loaded = load_candidate_nodes(max_n=max_nodes)
    if not loaded.ok or not loaded.fingerprint:
        set_status(STATUS_PROXY_FAILED, run_id=run_id, reason=loaded.error)
        _record_failure(run_id, {
            "reason": loaded.error,
            "error_code": loaded.error_code,
            "stats": loaded.stats.to_dict(),
        })
        return PrepareResult(
            ok=False, status=STATUS_PROXY_FAILED, run_id=run_id,
            candidate_nodes=loaded.stats.candidates,
            reason=loaded.error, error_code=loaded.error_code,
            stats=loaded.stats.to_dict(),
        )

    fp = loaded.fingerprint
    nodes = loaded.nodes
    candidate_nodes = len(nodes)
    log.info(
        "[prepare] nodes loaded run_id=%s candidates=%d elapsed=%.1fs",
        run_id, candidate_nodes, time.monotonic() - cold_started_at,
    )

    # 非强制入口优先复用同一订阅指纹下、仍在完整验证有效期内的健康池。
    # 复用并非盲信文件：会恢复/确认 Mihomo，并重新验证池内全部健康节点；
    # 只有最终仍达到硬门槛（默认 11）才允许抓取启动。
    cache_hits = read_probe_cache(fp) if not force else None
    if cache_hits:
        reused = _try_reuse_pool(run_id, loaded, cache_hits)
        if reused is not None:
            return reused
        log.warning("[prepare] 热/温启动复核失败，转入完整冷启动 run_id=%s", run_id)

    set_status(STATUS_PREPARING, run_id=run_id, phase="start_mihomo",
               candidate_nodes=candidate_nodes)
    runtime_started_at = time.monotonic()
    runtime = start_mihomo(nodes)
    if not runtime.ok:
        set_status(STATUS_PROXY_FAILED, run_id=run_id, reason=runtime.error)
        _record_failure(run_id, {
            "reason": runtime.error,
            "error_code": runtime.error_code,
            "log_tail": runtime.log_tail,
            "fingerprint": fp.to_dict(),
            "stats": loaded.stats.to_dict(),
        })
        write_probe_cache(fp, [], aborted=True)
        return PrepareResult(
            ok=False, status=STATUS_PROXY_FAILED, run_id=run_id,
            candidate_nodes=candidate_nodes,
            reason=runtime.error, error_code=runtime.error_code,
            fingerprint=fp.to_dict(), stats=loaded.stats.to_dict(),
        )
    log.info(
        "[prepare] mihomo ready run_id=%s ports=%d elapsed=%.1fs",
        run_id, len(runtime.ports), time.monotonic() - runtime_started_at,
    )

    entries = []
    listener_started_at = time.monotonic()
    for i, n in enumerate(nodes):
        port = PROXY_BASE_PORT + i
        # start_mihomo 已经确认所属进程存活；这里逐端口只做 socket 检查。
        # 禁止为每个端口再次执行 tasklist（69 节点会产生约 50 秒纯开销）。
        chk = check_listener(port)
        if not chk["ok"]:
            continue
        entries.append({
            "name": n.get("name"),
            "port": port,
            "proxy": f"http://127.0.0.1:{port}",
        })
    log.info(
        "[prepare] listeners checked run_id=%s listening=%d elapsed=%.1fs",
        run_id, len(entries), time.monotonic() - listener_started_at,
    )

    if not entries:
        reason = "监听检查后无可用端口"
        set_status(STATUS_PROXY_FAILED, run_id=run_id, reason=reason)
        write_probe_cache(fp, [], aborted=True)
        _record_failure(run_id, {
            "reason": reason,
            "error_code": "NO_LISTENING_PORTS",
            "fingerprint": fp.to_dict(),
            "runtime": runtime.to_dict(),
        })
        stop_owned_mihomo()
        return PrepareResult(
            ok=False, status=STATUS_PROXY_FAILED, run_id=run_id,
            candidate_nodes=candidate_nodes, reason=reason,
            error_code="NO_LISTENING_PORTS",
            fingerprint=fp.to_dict(), stats=loaded.stats.to_dict(),
        )

    set_status(STATUS_PREPARING, run_id=run_id, phase="health_check",
               listening=len(entries))
    refs_started_at = time.monotonic()
    refs = get_reference_ips()
    banned = {ip for ip in (refs.get("direct_ip"), refs.get("main_proxy_ip")) if ip}
    log.info(
        "[prepare] reference exits checked run_id=%s direct_ok=%s main_ok=%s elapsed=%.1fs",
        run_id, refs.get("direct_ok"), refs.get("main_proxy_ok"),
        time.monotonic() - refs_started_at,
    )

    health_started_at = time.monotonic()
    progress_lock = threading.Lock()
    checked = 0

    def _health_progress(_result):
        nonlocal checked
        with progress_lock:
            checked += 1
            current = checked
        if current == len(entries) or current % 10 == 0:
            set_status(
                STATUS_PREPARING,
                run_id=run_id,
                phase="health_check",
                checked_nodes=current,
                candidate_nodes=len(entries),
            )

    set_status(
        STATUS_PREPARING,
        run_id=run_id,
        phase="health_check",
        checked_nodes=0,
        candidate_nodes=len(entries),
    )
    health = verify_pool(entries, banned_ips=banned, progress=_health_progress)
    verified = health["verified"]
    verified_nodes = health["verified_nodes"]
    unique_ips = health["unique_ips"]
    amazon_ok = health["amazon_ok"]
    fail_reasons = health["fail_reasons"]
    log.info(
        "[prepare] health checked run_id=%s verified=%d unique=%d amazon=%d elapsed=%.1fs",
        run_id, verified_nodes, unique_ips, amazon_ok,
        time.monotonic() - health_started_at,
    )

    # 写入缓存（结构化，含指纹）
    cache_results = [r.to_dict() for r in health["results"]]
    write_probe_cache(fp, cache_results, aborted=False)

    ok_thresh, thresh_reason = _meets_thresholds(verified_nodes, unique_ips, amazon_ok)
    stats = {
        **loaded.stats.to_dict(),
        "listening": len(entries),
        "exit_ip_passed_raw": health["raw_passed"],
        "verified_nodes": verified_nodes,
        "unique_ips": unique_ips,
        "amazon_ok": amazon_ok,
        "fail_reasons": fail_reasons,
        "reference_ips": {
            "direct_ip": refs.get("direct_ip"),
            "main_proxy_ip": refs.get("main_proxy_ip"),
        },
    }

    if not ok_thresh:
        # 失败：不得覆盖正式池为 []；抓取不会启动，独立 Mihomo 必须无条件释放端口
        set_status(STATUS_PROXY_FAILED, run_id=run_id, reason=thresh_reason, **stats)
        _record_failure(run_id, {
            "reason": thresh_reason,
            "error_code": "THRESHOLD_NOT_MET",
            "fingerprint": fp.to_dict(),
            "stats": stats,
            "candidate_entries": pool_entries_from_health(verified),
        })
        stop_owned_mihomo()
        return PrepareResult(
            ok=False, status=STATUS_PROXY_FAILED, run_id=run_id,
            candidate_nodes=candidate_nodes,
            verified_nodes=verified_nodes, unique_ips=unique_ips,
            amazon_ok=amazon_ok, reason=thresh_reason,
            error_code="THRESHOLD_NOT_MET",
            fingerprint=fp.to_dict(), stats=stats, fail_reasons=fail_reasons,
        )

    pool_entries = pool_entries_from_health(verified)
    # 先写 candidate，再原子发布正式池
    _atomic_write_json(PROXY_POOL_CANDIDATE_FILE, {
        "run_id": run_id,
        "entries": pool_entries,
        "stats": stats,
        "fingerprint": fp.to_dict(),
    })
    pool_path = _publish_pool(pool_entries, fp, run_id, stats)
    set_status(STATUS_PROXY_READY, run_id=run_id, **stats, pool_path=pool_path)
    log.info(
        "[prepare] cold start ready run_id=%s entries=%d total_elapsed=%.1fs",
        run_id, len(pool_entries), time.monotonic() - cold_started_at,
    )

    if stop_owned_after:
        stop_owned_mihomo()

    return PrepareResult(
        ok=True, status=STATUS_PROXY_READY, run_id=run_id,
        candidate_nodes=candidate_nodes,
        verified_nodes=verified_nodes, unique_ips=unique_ips,
        amazon_ok=amazon_ok, reason="ok",
        fingerprint=fp.to_dict(), stats=stats, fail_reasons=fail_reasons,
        pool_path=pool_path,
    )


def _pool_matches_fingerprint(data: dict, fp: ProfileFingerprint) -> bool:
    return bool(
        isinstance(data, dict)
        and data.get("profile_uid") == fp.uid
        and data.get("profile_sha256") == fp.sha256
        and str(data.get("profile_updated_at", "")) == str(fp.updated_at)
    )


def _try_reuse_pool(run_id: str, loaded, cache_hits: list[dict]) -> PrepareResult | None:
    """热/温启动：复核上次健康池；失败返回 None 触发完整冷启动。"""
    fp = loaded.fingerprint
    if fp is None:
        return None
    try:
        pool_data = _read_json(PROXY_POOL_FILE)
    except Exception as exc:
        log.warning("[reuse] 正式池读取失败 run_id=%s error=%s", run_id, exc)
        return None
    if not _pool_matches_fingerprint(pool_data or {}, fp):
        log.info("[reuse] 订阅指纹或正式池不匹配 run_id=%s", run_id)
        return None

    previous_entries = list((pool_data or {}).get("entries") or [])
    previous_entries = [e for e in previous_entries if isinstance(e, dict) and e.get("proxy")]
    if len(previous_entries) < PROXY_MIN_AMAZON_OK:
        log.info(
            "[reuse] 历史池不足硬门槛 run_id=%s entries=%d min=%d",
            run_id, len(previous_entries), PROXY_MIN_AMAZON_OK,
        )
        return None

    mode = "hot"
    pid = owned_mihomo_running()
    if not pid:
        mode = "warm"
        set_status(
            STATUS_PREPARING,
            run_id=run_id,
            phase="restart_mihomo",
            startup_path=mode,
            cached_entries=len(previous_entries),
        )
        runtime = start_mihomo(loaded.nodes)
        if not runtime.ok:
            log.warning("[reuse] Mihomo 恢复失败 run_id=%s reason=%s", run_id, runtime.error)
            return None

    set_status(
        STATUS_PREPARING,
        run_id=run_id,
        phase="quick_verify",
        startup_path=mode,
        cached_entries=len(previous_entries),
    )
    started = time.monotonic()
    # 对历史健康池全部并发复核，而不是只抽样；由此严格保证发布时仍有 >=11 个
    # 独立、Amazon 可用、非 IPRoyal（完整验证期内）的出口。
    refs = get_reference_ips()
    banned = {ip for ip in (refs.get("direct_ip"), refs.get("main_proxy_ip")) if ip}
    health = verify_pool(previous_entries, banned_ips=banned)
    verified = health["verified"]
    verified_nodes = health["verified_nodes"]
    unique_ips = health["unique_ips"]
    amazon_ok = health["amazon_ok"]
    ok_thresh, reason = _meets_thresholds(verified_nodes, unique_ips, amazon_ok)
    if not ok_thresh:
        log.warning(
            "[reuse] 复核未达门槛 run_id=%s mode=%s verified=%d unique=%d amazon=%d reason=%s elapsed=%.1fs",
            run_id, mode, verified_nodes, unique_ips, amazon_ok, reason,
            time.monotonic() - started,
        )
        return None

    stats = {
        **loaded.stats.to_dict(),
        "startup_path": mode,
        "cached_results": len(cache_hits),
        "cached_entries": len(previous_entries),
        "verified_nodes": verified_nodes,
        "unique_ips": unique_ips,
        "amazon_ok": amazon_ok,
        "fail_reasons": health["fail_reasons"],
        "reference_ips": {
            "direct_ip": refs.get("direct_ip"),
            "main_proxy_ip": refs.get("main_proxy_ip"),
        },
    }
    entries = pool_entries_from_health(verified)
    pool_path = _publish_pool(entries, fp, run_id, stats)
    set_status(STATUS_PROXY_READY, run_id=run_id, **stats, pool_path=pool_path)
    log.info(
        "[reuse] %s start ready run_id=%s entries=%d elapsed=%.1fs",
        mode, run_id, len(entries), time.monotonic() - started,
    )
    return PrepareResult(
        ok=True,
        status=STATUS_PROXY_READY,
        run_id=run_id,
        candidate_nodes=len(loaded.nodes),
        verified_nodes=verified_nodes,
        unique_ips=unique_ips,
        amazon_ok=amazon_ok,
        fingerprint=fp.to_dict(),
        stats=stats,
        fail_reasons=health["fail_reasons"],
        pool_path=pool_path,
    )


def ensure_proxy_ready(*, force: bool = False) -> PrepareResult:
    """
    API / 抓取启动入口。默认优先走热/温启动；仅在订阅变化、缓存过期、
    运行时缺失且无法恢复、或复核后少于硬门槛时进入完整冷启动。
    返回结构化结果；失败不启动抓取。
    """
    return prepare_proxy_pool(force=force, stop_owned_after=False)


def stop_proxy_pool() -> dict:
    set_status(STATUS_STOPPING)
    res = stop_owned_mihomo()
    set_status(STATUS_IDLE, stop=res.to_dict())
    return res.to_dict()


# ── 运行时动态池（供 probe_na_valid 等复用）────────────────────────

class PoolManager:
    """轻量运行时管理：从已发布池/ PID 端口做出口复核。"""

    def __init__(self, check_interval: int = 180, fail_threshold: int = 8,
                 verify_timeout: int = 10):
        self.check_interval = check_interval
        self.fail_threshold = fail_threshold
        self.verify_timeout = verify_timeout
        self._lock = threading.Lock()
        self._active = {}
        self._fails = {}
        self._stop_ports = set()
        self._monitor_thread = None
        self._monitor_stop = threading.Event()

    def bootstrap(self) -> list:
        from proxy_session import load_proxy_pool
        loaded = load_proxy_pool(required=False, allow_direct=False, max_age=0)
        dedup = {}
        if loaded.ok:
            for e in loaded.entries:
                ip = e.get("exit_ip") or f"port-{e.get('port')}"
                if ip not in dedup:
                    dedup[ip] = {
                        "port": e["port"],
                        "proxy": e["proxy"],
                        "exit_ip": e.get("exit_ip", ""),
                        "name": e.get("name"),
                    }
        with self._lock:
            self._active = dedup
            self._stop_ports.clear()
            self._fails.clear()
        log.info("[pool_mgr] bootstrap: %d 独立条目", len(dedup))
        return self.active_entries()

    def active_entries(self) -> list:
        with self._lock:
            return list(self._active.values())

    def report(self, port: int, ok: bool):
        with self._lock:
            self._fails[port] = 0 if ok else self._fails.get(port, 0) + 1

    def should_stop(self, port: int) -> bool:
        with self._lock:
            return port in self._stop_ports

    def start_monitor(self, on_add=None, on_remove=None):
        def _loop():
            while not self._monitor_stop.wait(self.check_interval):
                try:
                    self._refresh_once(on_add, on_remove)
                except Exception as ex:
                    log.warning("[pool_mgr] 复核异常: %s", ex)
        self._monitor_thread = threading.Thread(target=_loop, daemon=True)
        self._monitor_thread.start()

    def stop_monitor(self):
        self._monitor_stop.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)

    def _refresh_once(self, on_add, on_remove):
        from proxy_health import fetch_exit_ip
        with self._lock:
            current = list(self._active.values())
        alive = {}
        for e in current:
            res = fetch_exit_ip(e["proxy"])
            if res.ok:
                alive[res.ip] = {**e, "exit_ip": res.ip}
        with self._lock:
            old = set(self._active)
            new = set(alive)
            removed = old - new
            added = new - old
            for ip in removed:
                e = self._active.pop(ip, None)
                if e:
                    self._stop_ports.add(e["port"])
            for ip in added:
                self._active[ip] = alive[ip]
                self._stop_ports.discard(alive[ip]["port"])
        for ip in added:
            if on_add:
                on_add(alive[ip])
        for ip in removed:
            if on_remove:
                on_remove({"exit_ip": ip})
