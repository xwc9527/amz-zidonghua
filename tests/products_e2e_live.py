"""Bounded real-Amazon end-to-end checks for the ranking crawler.

This script never writes the production database. It copies the current
database into a temporary directory, redirects every project DB/checkpoint
environment variable there, and invokes the production crawler functions.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
FORMAL_DB = ROOT / "data" / "categories.db"
LISTS = [
    "new-releases",
    "bestsellers",
    "movers-and-shakers",
    "most-wished-for",
    "most-gifted",
]
DEFAULT_CANDIDATES = [
    ("362533011", "Blankets & Throws", 3, "home-garden"),
    ("11965981", "Accordion Accessories", 3, "musical-instruments"),
    ("21490696011", "Activity Cubes", 3, "toys-and-games"),
    ("284507", "Kitchen & Dining", 1, "home-garden"),
    ("16285931", "Electronics", 2, "electronics"),
    ("374875011", "Kitchen & Dining", 3, "sporting-goods"),
]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


class RecordingClient:
    def __init__(self, inner):
        self.inner = inner
        self.pool = inner.pool
        self.requests: list[dict] = []

    def get(self, url, **kwargs):
        outcome = self.inner.get(url, **kwargs)
        self.requests.append({
            "url": url,
            "phase": kwargs.get("phase"),
            "item_id": kwargs.get("item_id"),
            "ok": outcome.ok,
            "status": outcome.status_code,
            "attempts": outcome.attempts,
            "reason": outcome.final_reason or outcome.error_code,
            "exit_ips": list(outcome.exit_ips or []),
            "html_bytes": len((outcome.html or "").encode("utf-8", errors="ignore")),
        })
        return outcome

    def close(self):
        self.inner.close()


def rows(db: Path, sql: str, params=()):
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--details-per-list", type=int, default=3)
    parser.add_argument("--keep-temp", action="store_true")
    args = parser.parse_args()

    run_id = time.strftime("PRODUCT-E2E-%Y%m%d-%H%M%S")
    temp_root = Path(tempfile.mkdtemp(prefix=f"{run_id}-", dir=r"C:\tmp"))
    test_db = temp_root / "categories_e2e.db"
    checkpoint_dir = temp_root / "checkpoints"
    checkpoint_dir.mkdir()
    formal_hash_before = sha256(FORMAL_DB)
    shutil.copy2(FORMAL_DB, test_db)
    conn = sqlite3.connect(test_db)
    conn.execute("DELETE FROM product_sightings")
    conn.commit()
    conn.close()

    os.environ.update({
        "DB_BACKEND": "sqlite",
        "DB_FILE": str(test_db),
        "AMZ_DB_FILE": str(test_db),
        "AMZ_CHECKPOINT_DIR": str(checkpoint_dir),
        "PROXY_REQUIRED": "1",
        "ALLOW_DIRECT_FALLBACK": "0",
        "AMZ_RUN_ID": run_id,
        "PYTHONIOENCODING": "utf-8",
    })

    import fetch_products as fp
    from config import get_marketplace

    # Keep in-process module path aligned with the isolated env DB.
    fp.DB_PATH = str(test_db)

    mp = get_marketplace("US")
    fp._SITE = "US"
    fp._DOMAIN = mp["domain"]
    fp._LANG = mp["lang"]
    fp._CURRENCY = mp["currency"]
    fp._DECIMAL_SEP = mp["decimal_sep"]
    fp._RATING_PAT = mp["rating_pattern"]
    fp._RESULTS_PAT = mp["results_pattern"]

    report: dict = {
        "run_id": run_id,
        "test_db": str(test_db),
        "module_db_path": fp.DB_PATH,
        "formal_hash_before": formal_hash_before,
        "proxy": {},
        "discovery": {},
        "live_crawls": {},
        "page_two": {},
        "scope": {},
        "database": {},
        "failures": [],
    }

    pool = fp.ProxyPool()
    report["proxy"] = pool.health_snapshot()
    client = RecordingClient(fp.WorkerProxyClient(pool, worker_id=91, warmup=False))
    try:
        selected: dict[str, dict] = {}
        for list_type in LISTS:
            attempts = []
            for node_id, name, depth, slug in DEFAULT_CANDIDATES:
                url = f"{fp._DOMAIN}/gp/{list_type}/{slug}/{node_id}/"
                outcome = client.get(
                    url,
                    phase="LIST",
                    item_id=f"discover:{node_id}:{list_type}",
                    referer=f"{fp._DOMAIN}/",
                )
                count = fp._count_product_items(outcome.html or "") if outcome.ok else 0
                attempts.append({
                    "node_id": node_id,
                    "name": name,
                    "slug": slug,
                    "url": url,
                    "status": outcome.status_code,
                    "ok": outcome.ok,
                    "product_items": count,
                    "reason": outcome.final_reason or outcome.error_code,
                    "exit_ips": list(outcome.exit_ips or []),
                })
                if outcome.ok and outcome.status_code == 200 and count > 0:
                    selected[list_type] = {
                        "node_id": node_id,
                        "name": name,
                        "depth": depth,
                        "slug": slug,
                        "url": url,
                    }
                    break
            report["discovery"][list_type] = attempts
            if list_type not in selected:
                report["failures"].append(f"NO_LIVE_NODE:{list_type}")

        for list_type, chosen in selected.items():
            fp._seen_asins.clear()
            before = len(client.requests)
            status, error, found, attempts = fp.process_node(
                chosen,
                [list_type],
                0,
                0,
                client,
                0,
                0,
                0,
                0,
                0,
                1,
                0,
                {},
                max(1, min(args.details_per_list, 10)),
            )
            db_rows = rows(
                test_db,
                """SELECT asin,node_id,list_type,site,rank AS list_position,detail_scraped,
                          social_proof_count,bsr_main_rank,bsr_sub_rank
                   FROM product_sightings WHERE site='US' AND list_type=?
                   ORDER BY list_position""",
                (list_type,),
            )
            request_slice = client.requests[before:]
            detail_requests = [r for r in request_slice if r["phase"] == "DETAIL"]
            report["live_crawls"][list_type] = {
                "node": chosen,
                "status": status,
                "error": error,
                "found": found,
                "attempts": attempts,
                "db_rows": db_rows,
                "detail_requests": detail_requests,
                "list_requests": [r for r in request_slice if r["phase"] == "LIST"],
            }
            if status != "done" or found <= 0 or not db_rows:
                report["failures"].append(f"CRAWL_FAILED:{list_type}:{status}:{error}")
            if any(int(r.get("detail_scraped") or 0) != 1 for r in db_rows):
                report["failures"].append(f"DETAIL_NOT_SCRAPED:{list_type}")

        # Force the production process_node path to request page 2 while the
        # impossible price removes all products before detail requests.
        page_list = "new-releases"
        chosen = selected.get(page_list) or next(iter(selected.values()), None)
        if chosen:
            fp._seen_asins.clear()
            before = len(client.requests)
            status, error, found, attempts = fp.process_node(
                chosen,
                [page_list],
                0,
                0,
                client,
                999999,
                0,
                0,
                0,
                0,
                2,
                0,
                {},
                0,
            )
            request_slice = client.requests[before:]
            list_urls = [r["url"] for r in request_slice if r["phase"] == "LIST"]
            page2 = [url for url in list_urls if "?pg=2" in url]
            report["page_two"] = {
                "status": status,
                "error": error,
                "found_after_impossible_filter": found,
                "attempts": attempts,
                "list_urls": list_urls,
                "page2_requested": bool(page2),
                "detail_requests": len([r for r in request_slice if r["phase"] == "DETAIL"]),
            }
            if not page2:
                report["failures"].append("PAGE2_NOT_REQUESTED")

        # Exercise production category selectors against the isolated copy.
        exact = fp.get_descendant_nodes(["1063268"], LISTS, site="US", include_descendants=False)
        descendants = fp.get_descendant_nodes(["1063268"], LISTS, site="US", include_descendants=True)
        overlap = fp.get_descendant_nodes(
            ["1063268", "362533011"], LISTS, site="US", include_descendants=True
        )
        depth3 = fp.get_nodes_by_depth([3], site="US")
        report["scope"] = {
            "exact_ids": [n["node_id"] for n in exact],
            "descendant_count": len(descendants),
            "descendant_unique": len({n["node_id"] for n in descendants}),
            "overlap_count": len(overlap),
            "overlap_unique": len({n["node_id"] for n in overlap}),
            "depth3_count": len(depth3),
            "depth3_unique": len({n["node_id"] for n in depth3}),
        }
        if report["scope"]["descendant_count"] != report["scope"]["descendant_unique"]:
            report["failures"].append("DESCENDANT_DUPLICATES")
        if report["scope"]["overlap_count"] != report["scope"]["overlap_unique"]:
            report["failures"].append("OVERLAP_DUPLICATES")
        if report["scope"]["depth3_count"] != report["scope"]["depth3_unique"]:
            report["failures"].append("DEPTH_DUPLICATES")

        all_rows = rows(
            test_db,
            """SELECT asin,node_id,list_type,site,COUNT(*) AS copies
               FROM product_sightings
               GROUP BY asin,node_id,list_type,site""",
        )
        dup_rows = [r for r in all_rows if int(r["copies"]) > 1]
        report["database"] = {
            "rows": len(all_rows),
            "duplicate_keys": dup_rows,
            "distinct_asins": len({r["asin"] for r in all_rows}),
        }
        if dup_rows:
            report["failures"].append("DATABASE_DUPLICATES")
    finally:
        client.close()

    report["formal_hash_after"] = sha256(FORMAL_DB)
    report["formal_db_unchanged"] = report["formal_hash_after"] == formal_hash_before
    if not report["formal_db_unchanged"]:
        report["failures"].append("FORMAL_DB_CHANGED")
    report["passed"] = not report["failures"]

    report_path = temp_root / "products_e2e_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"REPORT={report_path}")

    if not args.keep_temp:
        shutil.rmtree(temp_root, ignore_errors=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
