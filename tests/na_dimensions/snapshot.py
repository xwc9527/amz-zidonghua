"""正式库只读快照（前后对比）。"""
from __future__ import annotations

import hashlib
import os
import sqlite3
import time
from pathlib import Path


def file_sha256(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def snapshot_sqlite(db_path: str) -> dict:
    out = {
        "path": db_path,
        "exists": os.path.exists(db_path),
        "mtime": None,
        "sha256": None,
        "tables": {},
        "site_counts": {},
        "asin_digest": None,
        "max_scraped_at": None,
    }
    if not out["exists"]:
        return out
    out["mtime"] = os.path.getmtime(db_path)
    out["sha256"] = file_sha256(db_path)
    conn = sqlite3.connect(db_path)
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )]
        for t in tables:
            try:
                out["tables"][t] = conn.execute(f"SELECT COUNT(*) FROM [{t}]").fetchone()[0]
            except sqlite3.Error:
                out["tables"][t] = -1
        if "new_arrivals" in tables:
            rows = conn.execute(
                "SELECT site, COUNT(*) FROM new_arrivals GROUP BY site"
            ).fetchall()
            out["site_counts"] = {s: c for s, c in rows}
            asins = [r[0] for r in conn.execute(
                "SELECT asin FROM new_arrivals ORDER BY asin"
            ).fetchall()]
            digest = hashlib.sha256(",".join(asins).encode()).hexdigest()
            out["asin_digest"] = digest
            out["asin_count"] = len(asins)
            row = conn.execute(
                "SELECT MAX(scraped_at) FROM new_arrivals"
            ).fetchone()
            out["max_scraped_at"] = row[0] if row else None
        if "product_sightings" in tables:
            out["product_sightings_count"] = out["tables"].get("product_sightings")
    finally:
        conn.close()
    return out


def compare_snapshots(before: dict, after: dict) -> dict:
    changed = before.get("sha256") != after.get("sha256")
    table_diffs = {}
    for t in set(before.get("tables", {})) | set(after.get("tables", {})):
        b = before.get("tables", {}).get(t)
        a = after.get("tables", {}).get(t)
        if b != a:
            table_diffs[t] = {"before": b, "after": a}
    return {
        "hash_changed": changed,
        "mtime_before": before.get("mtime"),
        "mtime_after": after.get("mtime"),
        "table_diffs": table_diffs,
        "asin_digest_before": before.get("asin_digest"),
        "asin_digest_after": after.get("asin_digest"),
        "site_counts_before": before.get("site_counts"),
        "site_counts_after": after.get("site_counts"),
    }
