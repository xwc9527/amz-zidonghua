"""
start_lb_proxy.py — CLI 适配层（start / stop / check / refresh）
实际逻辑在 proxy_pool_manager / proxy_runtime / proxy_health。
"""
from __future__ import annotations

import argparse
import json
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from config import PROXY_MAX_NODES, PROXY_POOL_FILE
from proxy_health import get_reference_ips, verify_pool
from proxy_pool_manager import PrepareResult, ensure_proxy_ready, prepare_proxy_pool, stop_proxy_pool
from proxy_runtime import owned_mihomo_running
from proxy_session import load_proxy_pool


def cmd_start(max_n: int = PROXY_MAX_NODES) -> int:
    result = prepare_proxy_pool(max_nodes=max_n if max_n > 0 else None, force=True)
    _print_result(result)
    return 0 if result.ok else 1


def cmd_stop() -> int:
    res = stop_proxy_pool()
    print(json.dumps(res, ensure_ascii=False, indent=2))
    return 0 if res.get("ok") else 1


def cmd_check() -> int:
    loaded = load_proxy_pool(required=False, allow_direct=False, max_age=0)
    if not loaded.ok or not loaded.entries:
        print(f"[check] 无法加载池: {loaded.error} ({loaded.error_code})")
        return 1
    if not owned_mihomo_running():
        print("[check] 独立 Mihomo 未运行，仅校验池文件条目代理地址")
    refs = get_reference_ips()
    banned = {ip for ip in (refs.get("direct_ip"), refs.get("main_proxy_ip")) if ip}
    health = verify_pool(loaded.entries, banned_ips=banned)
    print(
        f"[check] verified={health['verified_nodes']} "
        f"unique_ips={health['unique_ips']} "
        f"fail={health['fail_reasons']}"
    )
    return 0 if health["verified_nodes"] > 0 else 1


def cmd_refresh(max_n: int = PROXY_MAX_NODES) -> int:
    result = ensure_proxy_ready(force=True) if max_n <= 0 else prepare_proxy_pool(
        max_nodes=max_n, force=True
    )
    _print_result(result)
    return 0 if result.ok else 1


def _print_result(result: PrepareResult) -> None:
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    if result.ok:
        print(f"[proxy] 已发布: {result.pool_path or PROXY_POOL_FILE}")
    else:
        print(f"[proxy] 失败: {result.error_code} {result.reason}")


# 供 api_server 兼容调用
def cmd_stop_compat():
    return stop_proxy_pool()


# 旧名称兼容
cmd_stop_legacy = cmd_stop


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mihomo 代理池管理")
    parser.add_argument("cmd", choices=["start", "stop", "check", "refresh"],
                        nargs="?", default="start")
    parser.add_argument("--max", type=int, default=PROXY_MAX_NODES,
                        help="最大代理节点数（0=不限）")
    args = parser.parse_args()

    if args.cmd == "start":
        raise SystemExit(cmd_start(args.max))
    if args.cmd == "stop":
        raise SystemExit(cmd_stop())
    if args.cmd == "check":
        raise SystemExit(cmd_check())
    if args.cmd == "refresh":
        raise SystemExit(cmd_refresh(args.max))
