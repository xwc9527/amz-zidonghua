"""Targeted real-page diagnostics for root movers, page 2, and rank fallback."""
from __future__ import annotations

import hashlib
import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
DB = ROOT / "data" / "categories.db"


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> int:
    os.environ.update({
        "PROXY_REQUIRED": "1",
        "ALLOW_DIRECT_FALLBACK": "0",
        "PYTHONIOENCODING": "utf-8",
    })
    import fetch_products as fp
    from bs4 import BeautifulSoup
    from config import get_marketplace

    mp = get_marketplace("US")
    fp._SITE = "US"
    fp._DOMAIN = mp["domain"]
    fp._LANG = mp["lang"]
    fp._CURRENCY = mp["currency"]
    fp._DECIMAL_SEP = mp["decimal_sep"]
    fp._RATING_PAT = mp["rating_pattern"]
    fp._RESULTS_PAT = mp["results_pattern"]

    before = digest(DB)
    pool = fp.ProxyPool()
    client = fp.WorkerProxyClient(pool, worker_id=92, warmup=False)
    checks = []
    urls = [
        ("movers-canonical-root", "https://www.amazon.com/gp/movers-and-shakers/home-garden/"),
        ("movers-crawler-root", "https://www.amazon.com/gp/movers-and-shakers/home-garden/home-garden/"),
        ("new-releases-page2", "https://www.amazon.com/gp/new-releases/home-garden/362533011/?pg=2"),
    ]
    try:
        page2_html = ""
        for label, url in urls:
            outcome = client.get(url, phase="LIST", item_id=label, referer="https://www.amazon.com/")
            html = outcome.html or ""
            item_count = fp._count_product_items(html) if outcome.ok else 0
            soup = BeautifulSoup(html, "html.parser") if html else None
            checks.append({
                "label": label,
                "url": url,
                "ok": outcome.ok,
                "status": outcome.status_code,
                "reason": outcome.final_reason or outcome.error_code,
                "attempts": outcome.attempts,
                "exit_ips": list(outcome.exit_ips or []),
                "product_items": item_count,
                "data_asin_nodes": len(soup.select("[data-asin]")) if soup else 0,
                "dp_links": len(soup.select("a[href*='/dp/']")) if soup else 0,
                "title": soup.title.get_text(" ", strip=True) if soup and soup.title else "",
                "bytes": len(html.encode("utf-8", errors="ignore")),
            })
            if label == "new-releases-page2" and outcome.ok:
                page2_html = html

        rank_check = {}
        if page2_html:
            fp._seen_asins.clear()
            products = fp.parse_products(
                page2_html,
                "362533011",
                "Blankets & Throws",
                "home-garden",
                3,
                "new-releases",
                fp.extract_list_total(page2_html),
                0,
                list_limit=0,
                position_start=31,
            )
            products = products[:5]
            rank_check = {
                "parsed": len(products),
                "asins": [p.get("asin") for p in products],
                "ranks": [p.get("rank") for p in products],
                "expected_fallback": list(range(31, 31 + len(products))),
            }
    finally:
        client.close()

    report = {
        "checks": checks,
        "rank_check": rank_check,
        "formal_hash_before": before,
        "formal_hash_after": digest(DB),
    }
    report["formal_db_unchanged"] = report["formal_hash_before"] == report["formal_hash_after"]
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
