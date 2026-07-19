"""
proxy_health.py — 出口 IP 验证、Amazon 可达性、去重与结果分类。
"""
from __future__ import annotations

import ipaddress
import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from typing import Callable

from config import (
    CLASH_MIXED_PORT,
    PROXY_AMAZON_CHECK_URL,
    PROXY_CONNECT_TIMEOUT,
    PROXY_HEALTH_CONCURRENCY,
    PROXY_IP_ENDPOINTS,
    PROXY_IPROYAL_MARKERS,
    PROXY_READ_TIMEOUT,
)

log = logging.getLogger("proxy_health")

_CAPTCHA_MARKERS = (
    "captcha", "robot check", "type the characters", "enter the characters",
    "api-services-support@amazon.com", "/errors/validatecaptcha",
)


@dataclass
class ExitIpResult:
    ok: bool
    ip: str = ""
    endpoint: str = ""
    error: str = ""
    error_code: str = ""
    elapsed_ms: int = 0

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AmazonResult:
    ok: bool
    status_code: int | None = None
    elapsed_ms: int = 0
    captcha: bool = False
    error: str = ""
    error_code: str = ""
    url: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class NodeHealthResult:
    name: str
    port: int
    proxy: str
    ok: bool
    exit_ip: str = ""
    amazon_ok: bool = False
    captcha: bool = False
    reason: str = ""
    error_code: str = ""
    exit_check: dict = field(default_factory=dict)
    amazon_check: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


def _requests():
    try:
        from curl_cffi import requests as cr
        return cr, True
    except ImportError:
        import requests as rq
        return rq, False


def is_public_ip(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip.strip())
    except ValueError:
        return False
    return not (
        addr.is_private or addr.is_loopback or addr.is_link_local
        or addr.is_reserved or addr.is_multicast or addr.is_unspecified
    )


def _extract_ip(text: str) -> str:
    text = (text or "").strip()
    m = re.search(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", text)
    if m:
        return m.group(0)
    # 粗略 IPv6
    if ":" in text and len(text) <= 45 and " " not in text and "<" not in text:
        return text
    return ""


def fetch_exit_ip(proxy_url: str | None = None, timeout: tuple[int, int] | None = None) -> ExitIpResult:
    """依次尝试多个 HTTPS IP 端点，任一成功即可。"""
    req, use_cffi = _requests()
    to = timeout or (PROXY_CONNECT_TIMEOUT, PROXY_READ_TIMEOUT)
    proxies = None
    if proxy_url:
        proxies = {"http": proxy_url, "https": proxy_url}
    last_err = ""
    for index, ep in enumerate(PROXY_IP_ENDPOINTS):
        t0 = time.time()
        try:
            kwargs = {"proxies": proxies, "timeout": to, "verify": False}
            if use_cffi:
                kwargs["impersonate"] = "chrome124"
            r = req.get(ep, **kwargs)
            ip = _extract_ip(getattr(r, "text", "") or "")
            elapsed = int((time.time() - t0) * 1000)
            if not ip:
                last_err = f"{ep}: empty/non-ip body"
                continue
            if not is_public_ip(ip):
                return ExitIpResult(
                    ok=False, ip=ip, endpoint=ep, elapsed_ms=elapsed,
                    error="非公网IP", error_code="PRIVATE_IP",
                )
            return ExitIpResult(ok=True, ip=ip, endpoint=ep, elapsed_ms=elapsed)
        except Exception as e:
            last_err = f"{ep}: {e}"
            # 坏节点连续两个端点都超时后，第三次通常只会重复等待同一条失效链路。
            # 非超时错误仍保留第三端点兜底，兼顾端点自身异常。
            if index >= 1 and "tim" in str(e).lower():
                break
            continue
    return ExitIpResult(ok=False, error=last_err or "全部端点失败", error_code="IP_CHECK_FAILED")


def check_amazon(proxy_url: str, url: str | None = None,
                 timeout: tuple[int, int] | None = None) -> AmazonResult:
    req, use_cffi = _requests()
    to = timeout or (PROXY_CONNECT_TIMEOUT, PROXY_READ_TIMEOUT)
    target = url or PROXY_AMAZON_CHECK_URL
    proxies = {"http": proxy_url, "https": proxy_url}
    t0 = time.time()
    try:
        kwargs = {"proxies": proxies, "timeout": to, "verify": False}
        if use_cffi:
            kwargs["impersonate"] = "chrome124"
        r = req.get(target, **kwargs)
        elapsed = int((time.time() - t0) * 1000)
        body = (getattr(r, "text", "") or "")[:8000].lower()
        captcha = any(m in body for m in _CAPTCHA_MARKERS)
        # 业务可用必须同时满足 2xx 且不是验证码/Robot Check 页面。
        # 仅“能收到 HTTP 响应”不能代表能够抓取 Amazon。
        status_ok = 200 <= int(r.status_code) < 300
        ok = status_ok and not captcha
        return AmazonResult(
            ok=ok, status_code=int(r.status_code), elapsed_ms=elapsed,
            captcha=captcha, url=target,
            error="captcha_page" if captcha else ("" if status_ok else f"HTTP {r.status_code}"),
            error_code="CAPTCHA" if captcha else ("" if status_ok else "AMAZON_HTTP_STATUS"),
        )
    except Exception as e:
        elapsed = int((time.time() - t0) * 1000)
        return AmazonResult(
            ok=False, elapsed_ms=elapsed, error=str(e),
            error_code="AMAZON_UNREACHABLE", url=target,
        )


def lookup_isp_hint(ip: str, timeout: tuple[int, int] | None = None) -> dict:
    """补充性 ASN/ISP 查询；失败不阻断。"""
    if not ip:
        return {"ok": False, "error": "empty ip"}
    req, use_cffi = _requests()
    to = timeout or (PROXY_CONNECT_TIMEOUT, PROXY_READ_TIMEOUT)
    url = f"https://ipinfo.io/{ip}/json"
    try:
        kwargs = {"timeout": to, "verify": False}
        if use_cffi:
            kwargs["impersonate"] = "chrome124"
        r = req.get(url, **kwargs)
        data = r.json() if hasattr(r, "json") else {}
        org = str(data.get("org") or "")
        hit = any(m in org.lower() for m in PROXY_IPROYAL_MARKERS)
        return {
            "ok": True, "ip": ip, "org": org, "asn_hint": org,
            "iproyal_hit": hit, "source": "ipinfo.io",
        }
    except Exception as e:
        return {"ok": False, "ip": ip, "error": str(e)}


def get_reference_ips() -> dict:
    """获取本机直连出口与主代理（7897）出口，供排除。"""
    # 两条互不依赖，并发执行，避免出口服务异常时串行叠加超时。
    with ThreadPoolExecutor(max_workers=2) as ex:
        direct_f = ex.submit(fetch_exit_ip, None, (3, 5))
        main_f = ex.submit(
            fetch_exit_ip,
            f"http://127.0.0.1:{CLASH_MIXED_PORT}",
            (3, 5),
        )
        direct = direct_f.result()
        main_proxy = main_f.result()
    return {
        "direct_ip": direct.ip if direct.ok else "",
        "direct_ok": direct.ok,
        "main_proxy_ip": main_proxy.ip if main_proxy.ok else "",
        "main_proxy_ok": main_proxy.ok,
        "direct": direct.to_dict(),
        "main_proxy": main_proxy.to_dict(),
    }


def verify_node(
    entry: dict,
    banned_ips: set[str] | None = None,
    amazon_url: str | None = None,
) -> NodeHealthResult:
    name = entry.get("name", "")
    port = int(entry["port"])
    proxy = entry.get("proxy") or f"http://127.0.0.1:{port}"
    banned = banned_ips or set()

    exit_res = fetch_exit_ip(proxy)
    if not exit_res.ok:
        return NodeHealthResult(
            name=name, port=port, proxy=proxy, ok=False,
            reason=exit_res.error or "出口IP失败",
            error_code=exit_res.error_code or "IP_CHECK_FAILED",
            exit_check=exit_res.to_dict(),
        )
    ip = exit_res.ip
    if ip in banned:
        return NodeHealthResult(
            name=name, port=port, proxy=proxy, ok=False, exit_ip=ip,
            reason="出口IP与本机/主代理相同或禁止",
            error_code="BANNED_EXIT_IP",
            exit_check=exit_res.to_dict(),
        )

    isp = lookup_isp_hint(ip)
    if isp.get("iproyal_hit"):
        return NodeHealthResult(
            name=name, port=port, proxy=proxy, ok=False, exit_ip=ip,
            reason="出口归属命中 IPRoyal",
            error_code="IPROYAL_ASN",
            exit_check={**exit_res.to_dict(), "isp": isp},
        )

    amz = check_amazon(proxy, url=amazon_url)
    if not amz.ok:
        return NodeHealthResult(
            name=name, port=port, proxy=proxy, ok=False, exit_ip=ip,
            amazon_ok=False, captcha=amz.captcha,
            reason=amz.error or "Amazon不可达",
            error_code=amz.error_code or "AMAZON_UNREACHABLE",
            exit_check={**exit_res.to_dict(), "isp": isp},
            amazon_check=amz.to_dict(),
        )

    return NodeHealthResult(
        name=name, port=port, proxy=proxy, ok=True, exit_ip=ip,
        amazon_ok=True, captcha=amz.captcha,
        reason="ok" if not amz.captcha else "ok_with_captcha",
        error_code="",
        exit_check={**exit_res.to_dict(), "isp": isp},
        amazon_check=amz.to_dict(),
    )


def verify_pool(
    entries: list[dict],
    banned_ips: set[str] | None = None,
    concurrency: int | None = None,
    progress: Callable[[NodeHealthResult], None] | None = None,
) -> dict:
    """两阶段并发验证：先取出口并去重，再对独立出口做 ISP/Amazon 检查。"""
    workers = concurrency or PROXY_HEALTH_CONCURRENCY
    results: list[NodeHealthResult] = []
    lock = threading.Lock()
    banned = banned_ips or set()

    def _emit(res: NodeHealthResult):
        with lock:
            results.append(res)
        if progress:
            progress(res)

    def _exit_one(entry):
        name = entry.get("name", "")
        port = int(entry["port"])
        proxy = entry.get("proxy") or f"http://127.0.0.1:{port}"
        exit_res = fetch_exit_ip(proxy)
        if not exit_res.ok:
            return None, NodeHealthResult(
                name=name, port=port, proxy=proxy, ok=False,
                reason=exit_res.error or "出口IP失败",
                error_code=exit_res.error_code or "IP_CHECK_FAILED",
                exit_check=exit_res.to_dict(),
            )
        if exit_res.ip in banned:
            return None, NodeHealthResult(
                name=name, port=port, proxy=proxy, ok=False,
                exit_ip=exit_res.ip,
                reason="出口IP与本机/主代理相同或禁止",
                error_code="BANNED_EXIT_IP",
                exit_check=exit_res.to_dict(),
            )
        return {
            "name": name,
            "port": port,
            "proxy": proxy,
            "exit_res": exit_res,
        }, None

    exit_passed = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        futures = [ex.submit(_exit_one, e) for e in entries]
        for future in as_completed(futures):
            try:
                candidate, failed = future.result()
                if failed is not None:
                    _emit(failed)
                elif candidate is not None:
                    exit_passed.append(candidate)
            except Exception as exc:
                log.warning("[health] 出口检查 worker 异常: %s", exc)

    # 同出口只保留端口最小的一个代表；重复节点不再浪费 ISP 和 Amazon 请求。
    representatives = []
    seen_ips: set[str] = set()
    for candidate in sorted(exit_passed, key=lambda x: x["port"]):
        ip = candidate["exit_res"].ip
        if ip in seen_ips:
            _emit(NodeHealthResult(
                name=candidate["name"],
                port=candidate["port"],
                proxy=candidate["proxy"],
                ok=False,
                exit_ip=ip,
                reason="出口IP重复",
                error_code="DUPLICATE_EXIT_IP",
                exit_check=candidate["exit_res"].to_dict(),
            ))
            continue
        seen_ips.add(ip)
        representatives.append(candidate)

    def _deep_one(candidate):
        ip = candidate["exit_res"].ip
        isp = lookup_isp_hint(ip)
        if isp.get("iproyal_hit"):
            return NodeHealthResult(
                name=candidate["name"], port=candidate["port"],
                proxy=candidate["proxy"], ok=False, exit_ip=ip,
                reason="出口归属命中 IPRoyal", error_code="IPROYAL_ASN",
                exit_check={**candidate["exit_res"].to_dict(), "isp": isp},
            )
        amz = check_amazon(candidate["proxy"])
        if not amz.ok:
            return NodeHealthResult(
                name=candidate["name"], port=candidate["port"],
                proxy=candidate["proxy"], ok=False, exit_ip=ip,
                amazon_ok=False, captcha=amz.captcha,
                reason=amz.error or "Amazon不可达",
                error_code=amz.error_code or "AMAZON_UNREACHABLE",
                exit_check={**candidate["exit_res"].to_dict(), "isp": isp},
                amazon_check=amz.to_dict(),
            )
        return NodeHealthResult(
            name=candidate["name"], port=candidate["port"],
            proxy=candidate["proxy"], ok=True, exit_ip=ip,
            amazon_ok=True, captcha=False, reason="ok",
            exit_check={**candidate["exit_res"].to_dict(), "isp": isp},
            amazon_check=amz.to_dict(),
        )

    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(representatives) or 1))) as ex:
        futures = [ex.submit(_deep_one, c) for c in representatives]
        for future in as_completed(futures):
            try:
                _emit(future.result())
            except Exception as exc:
                log.warning("[health] 深度检查 worker 异常: %s", exc)

    deduped = [r for r in results if r.ok]
    verified_ips = {r.exit_ip for r in deduped if r.exit_ip}

    fail_reasons: dict[str, int] = {}
    for r in results:
        if not r.ok:
            key = r.error_code or r.reason or "UNKNOWN"
            fail_reasons[key] = fail_reasons.get(key, 0) + 1
    return {
        "results": results,
        "verified": deduped,
        "verified_nodes": len(deduped),
        "unique_ips": len(verified_ips),
        "amazon_ok": len(deduped),
        "fail_reasons": fail_reasons,
        "raw_passed": len(exit_passed),
    }


def pool_entries_from_health(verified: list[NodeHealthResult]) -> list[dict]:
    out = []
    for r in verified:
        out.append({
            "name": r.name,
            "port": r.port,
            "proxy": r.proxy,
            "exit_ip": r.exit_ip,
            "amazon_ok": r.amazon_ok,
            "captcha": r.captcha,
        })
    return out
