"""
probe_proxies.py — CLI 入口：构建/刷新代理池（不再依赖 9097）。

输出 data/probe_results.json 为带订阅指纹的缓存，不是事实来源。
"""
from __future__ import annotations

import argparse
import json
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

from proxy_pool_manager import prepare_proxy_pool


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="验证机场节点并发布代理池")
    parser.add_argument("--max", type=int, default=0, help="最大节点数，0=不限")
    parser.add_argument(
        "--stop-after", action="store_true",
        help="验证并发布后停止独立 Mihomo（默认保持运行供抓取使用）",
    )
    args = parser.parse_args(argv)

    result = prepare_proxy_pool(
        max_nodes=args.max if args.max > 0 else None,
        stop_owned_after=args.stop_after,
        force=True,
    )
    print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
