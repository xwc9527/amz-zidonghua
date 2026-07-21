"""
proxy_worker.py — 抓取侧共享的强制代理 Worker。

榜单 / 最新到货复用同一套：独立出口重试、冷却、审计日志、最低可用门槛。
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

from config import (
    PROXY_LOW_POOL_DELAY_SCALE,
    PROXY_MIN_START_NODES,
    PROXY_POOL_TARGET_NODES,
    PROXY_REQUEST_DISTINCT_ATTEMPTS,
    PROXY_ROTATE_REQUESTS,
    PROXY_ROTATE_TTL,
)
from proxy_session import ForcedProxyPool, ProxyRequiredError

log = logging.getLogger("proxy_worker")

SessionFactory = Callable[[int, dict], object]
WarmupFn = Callable[[object], None]
PageCheckFn = Callable[[str], bool]


def pool_aware_delay(
    delay_min: float,
    delay_max: float,
    usable: int | None = None,
    *,
    target: int | None = None,
    scale: float | None = None,
) -> float:
    """可用节点少于目标规模时放大请求间隔，降低单出口被限流概率。"""
    tgt = PROXY_POOL_TARGET_NODES if target is None else max(1, int(target))
    sc = PROXY_LOW_POOL_DELAY_SCALE if scale is None else float(scale)
    u = tgt if usable is None else max(0, int(usable))
    if u >= tgt or sc <= 1.0:
        return random.uniform(delay_min, delay_max)
    # usable=0 → 满倍率；usable=target → 1.0
    factor = 1.0 + (1.0 - min(u, tgt) / tgt) * (sc - 1.0)
    return random.uniform(delay_min * factor, delay_max * factor)


def pool_aware_rotate_after(usable: int | None = None, *, target: int | None = None) -> int:
    """池子偏小时更积极轮换出口（最小仍为 1）。"""
    base = max(1, PROXY_ROTATE_REQUESTS)
    tgt = PROXY_POOL_TARGET_NODES if target is None else max(1, int(target))
    u = tgt if usable is None else max(0, int(usable))
    if u >= tgt:
        jitter = random.randint(-5, 5) if base > 5 else 0
        return max(1, base + jitter)
    # 不足目标：强制每请求轮换
    return 1


@dataclass
class FetchOutcome:
    ok: bool
    html: str | None = None
    error_code: str = ""
    final_reason: str = ""
    status_code: int | None = None
    attempts: int = 0
    exit_ips: list[str] = field(default_factory=list)
    reasons: list[str] = field(default_factory=list)
    elapsed_ms: int = 0


_CAPTCHA_MARKERS = (
    "captcha", "robot check", "type the characters", "/errors/validatecaptcha",
    "klicke auf die schaltfläche", "geben sie die zeichen", "entrez les caractères",
)


def is_captcha_page(text: str) -> bool:
    low = (text or "").lower()
    return any(marker in low for marker in _CAPTCHA_MARKERS)


def has_us_currency_mismatch(text: str) -> bool:
    """仅检查价格组件，避免代理地理位置把 US 价格本地化为 JPY/S$/CA$。"""
    if not text:
        return False
    price_values = re.findall(
        r'class=["\'][^"\']*(?:a-offscreen|a-price-symbol)[^"\']*["\'][^>]*>([^<]{1,24})<',
        text,
        flags=re.I,
    )
    foreign = re.compile(r"(?:S\$|CA\$|A\$|JPY|EUR|GBP|SGD|[¥￥€£])\s*[\d,.]", re.I)
    return any(foreign.search(re.sub(r"\s+", "", value)) for value in price_values)


def classify_request_exception(exc: Exception) -> str:
    text = f"{type(exc).__name__}: {exc}".lower()
    if "invalid library" in text or "openssl_internal:invalid library" in text:
        return "CLIENT_TLS_ERROR"
    if "timeout" in text:
        return "CONNECT_TIMEOUT" if "connect" in text else "READ_TIMEOUT"
    if "proxy" in text:
        return "PROXY_CONNECT_ERROR"
    if any(x in text for x in ("ssl", "tls", "certificate")):
        return "TLS_ERROR"
    if any(x in text for x in ("reset", "broken pipe", "closed")):
        return "CONNECTION_RESET"
    return "REQUEST_ERROR"


def http_error_code(status_code: int) -> str:
    return {
        403: "HTTP_403",
        429: "HTTP_429",
        503: "HTTP_503",
    }.get(status_code, "OTHER_HTTP_STATUS")


class AttemptAuditor:
    def __init__(self, path: str, run_id: str = ""):
        self.path = path
        self.run_id = run_id or os.getenv("AMZ_RUN_ID") or datetime.now().strftime("RUN-%Y%m%d-%H%M%S")
        self._lock = threading.Lock()
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)

    def write(self, **event) -> None:
        event.setdefault(
            "ts",
            datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        )
        event.setdefault("run_id", self.run_id)
        line = json.dumps(event, ensure_ascii=False, sort_keys=True)
        with self._lock:
            with open(self.path, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")


class WorkerProxyClient:
    """Worker 会话：失败立即换独立出口，正常请求按请求量/持有时长轮换。"""

    def __init__(
        self,
        pool: ForcedProxyPool,
        worker_id: int,
        *,
        make_session: SessionFactory,
        warmup: WarmupFn | None = None,
        auditor: AttemptAuditor | None = None,
        is_captcha: PageCheckFn | None = None,
        is_currency_mismatch: PageCheckFn | None = None,
        timeout: float = 18,
    ):
        self.pool = pool
        self.worker_id = worker_id
        self.make_session = make_session
        self.warmup = warmup
        self.auditor = auditor
        self.is_captcha = is_captcha or is_captcha_page
        self.is_currency_mismatch = is_currency_mismatch
        self.timeout = timeout
        self.entry = None
        self.session = None
        self.acquired_at = 0.0
        self.requests_used = 0
        try:
            usable = int(getattr(pool, "usable_count", PROXY_POOL_TARGET_NODES) or 0)
        except Exception:
            usable = PROXY_POOL_TARGET_NODES
        self.rotate_after = pool_aware_rotate_after(usable)

    def _audit(self, **event) -> None:
        if self.auditor:
            self.auditor.write(**event)

    def _close_session(self):
        if self.session is not None:
            try:
                self.session.close()
            except Exception:
                pass
        self.session = None

    def _release(self, outcome: str):
        self._close_session()
        if self.entry is not None:
            self.pool.release(self.entry, outcome=outcome)
        self.entry = None
        self.requests_used = 0
        self.acquired_at = 0.0

    def _ensure_session(self, excluded: set[str]):
        expired = self.entry is not None and (
            self.requests_used >= self.rotate_after
            or time.monotonic() - self.acquired_at >= PROXY_ROTATE_TTL
        )
        if expired:
            self._release("ROTATE")
        if self.entry is None:
            self.entry = self.pool.acquire(timeout=30, exclude_exit_ips=excluded)
            try:
                self.session = self.make_session(self.worker_id, self.entry)
            except Exception:
                self.pool.release(self.entry, outcome="SESSION_CREATE_ERROR")
                self.entry = None
                raise
            self.acquired_at = time.monotonic()
            try:
                self.rotate_after = pool_aware_rotate_after(self.pool.usable_count)
            except Exception:
                pass
            if self.warmup:
                self.warmup(self.session)

    def get(self, url: str, *, phase: str, item_id: str, referer: str = "") -> FetchOutcome:
        started = time.monotonic()
        used_ips: set[str] = set()
        reasons: list[str] = []
        last_status = None
        for attempt in range(1, PROXY_REQUEST_DISTINCT_ATTEMPTS + 1):
            last_status = None
            try:
                self._ensure_session(used_ips)
            except ProxyRequiredError as exc:
                reasons.append(exc.code)
                break
            except Exception as exc:
                reasons.append("SESSION_CREATE_ERROR")
                self._audit(
                    phase=phase, item_id=item_id, worker=self.worker_id, attempt=attempt,
                    result="FAILED", reason="SESSION_CREATE_ERROR", detail=str(exc)[:300],
                    status_code=None, exit_ip="", proxy_name="", port=None,
                    elapsed_ms=int((time.monotonic() - started) * 1000), rotated=True,
                )
                continue

            entry = self.entry or {}
            exit_ip = entry.get("exit_ip") or "unknown"
            if exit_ip != "unknown":
                used_ips.add(exit_ip)
            if referer:
                self.session.headers["Referer"] = referer
            self.pool.wait_if_target_paused()
            attempt_started = time.monotonic()
            code = ""
            detail = ""
            try:
                response = self.session.get(url, timeout=self.timeout)
                last_status = int(response.status_code)
                body = response.text or ""
                if last_status == 200 and self.is_captcha(body):
                    code = "CAPTCHA"
                elif (
                    last_status == 200
                    and self.is_currency_mismatch
                    and self.is_currency_mismatch(body)
                ):
                    code = "LOCALE_CURRENCY_MISMATCH"
                elif last_status != 200:
                    code = http_error_code(last_status)
                elif not body.strip():
                    code = "EMPTY_RESPONSE"
                else:
                    self.requests_used += 1
                    self.pool.record_success(
                        self.entry,
                        latency_ms=int((time.monotonic() - attempt_started) * 1000),
                    )
                    self._audit(
                        phase=phase, item_id=item_id, worker=self.worker_id, attempt=attempt,
                        result="SUCCESS", reason="", status_code=last_status,
                        exit_ip=exit_ip, proxy_name=entry.get("name", ""), port=entry.get("port"),
                        elapsed_ms=int((time.monotonic() - attempt_started) * 1000), rotated=False,
                    )
                    return FetchOutcome(
                        ok=True, html=body, status_code=last_status, attempts=attempt,
                        exit_ips=list(used_ips), reasons=reasons,
                        elapsed_ms=int((time.monotonic() - started) * 1000),
                    )
            except Exception as exc:
                code = classify_request_exception(exc)
                detail = str(exc)[:300]

            reasons.append(code)
            self._audit(
                phase=phase, item_id=item_id, worker=self.worker_id, attempt=attempt,
                result="FAILED", reason=code, detail=detail, status_code=last_status,
                exit_ip=exit_ip, proxy_name=entry.get("name", ""), port=entry.get("port"),
                elapsed_ms=int((time.monotonic() - attempt_started) * 1000), rotated=True,
            )
            log.warning(
                "[request] phase=%s item=%s worker=%d attempt=%d/%d reason=%s status=%s "
                "exit_ip=%s action=rotate",
                phase, item_id, self.worker_id, attempt, PROXY_REQUEST_DISTINCT_ATTEMPTS,
                code, last_status, exit_ip,
            )
            self._release(code)

        final_reason = reasons[-1] if reasons else "REQUEST_ERROR"
        fatal_code = "POOL_BELOW_MINIMUM" if "POOL_BELOW_MINIMUM" in reasons else "RETRY_EXHAUSTED"
        self._audit(
            phase=phase, item_id=item_id, worker=self.worker_id, attempt=len(reasons),
            result="EXHAUSTED", reason=final_reason, reasons=reasons,
            status_code=last_status, exit_ips=sorted(used_ips),
            elapsed_ms=int((time.monotonic() - started) * 1000), rotated=False,
        )
        return FetchOutcome(
            ok=False, error_code=fatal_code, final_reason=final_reason,
            status_code=last_status, attempts=len(reasons), exit_ips=list(used_ips),
            reasons=reasons, elapsed_ms=int((time.monotonic() - started) * 1000),
        )

    def close(self):
        self._release("ROTATE")


def raise_if_pool_below_minimum(outcome: FetchOutcome, min_usable: int | None = None) -> None:
    # ForcedProxyPool.acquire() 现在会有界等待常驻守护进程补充节点，只有等待
    # 超时仍不达标才会真正冒出 POOL_BELOW_MINIMUM；这里是最后一道防线。
    if outcome.error_code == "POOL_BELOW_MINIMUM":
        threshold = PROXY_MIN_START_NODES if min_usable is None else min_usable
        raise ProxyRequiredError(
            f"运行时可用代理低于最低要求 {threshold}，已安全暂停并保留断点",
            "POOL_BELOW_MINIMUM",
        )
