"""正式收藏表：SQLite / PostgreSQL 同构快照存储。"""
from __future__ import annotations

import json
import os
import sqlite3
import threading
from typing import Any

from config import DB_FILE, assert_testing_paths_safe

_LOCK = threading.RLock()
_LIST_PAGE_MAX = 200

SNAPSHOT_FIELDS = (
    "name", "title", "price", "price_raw", "price_value", "original_price", "discount_pct",
    "rating", "review_count", "rank", "image_url", "product_url",
    "has_video", "is_amazon_choice", "is_bestseller",
    "list_type", "list_total", "category_name", "category_slug", "category_depth", "node_id",
    "bsr_main_rank", "bsr_main_category", "bsr_sub_rank", "bsr_sub_category", "bsr_sub",
    "variant_option_count", "other_sellers_count",
    "social_proof", "social_proof_count",
    "item_weight", "item_dimensions", "weight_lb", "dim_l_in", "dim_w_in", "dim_h_in",
    "date_first_available", "listing_date", "listing_age_days",
    "shipping_fee", "shipping_fee_value", "fba_fee", "placement_fee",
    "fulfillment_type", "country_of_origin", "detail_scraped", "chart",
)

_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS favorite_products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    site TEXT NOT NULL,
    asin TEXT NOT NULL,
    name TEXT,
    title TEXT,
    price REAL,
    price_raw TEXT,
    price_value REAL,
    original_price TEXT,
    discount_pct TEXT,
    rating REAL,
    review_count INTEGER,
    rank INTEGER,
    image_url TEXT,
    product_url TEXT,
    has_video INTEGER DEFAULT 0,
    is_amazon_choice INTEGER DEFAULT 0,
    is_bestseller INTEGER DEFAULT 0,
    list_type TEXT,
    list_total INTEGER,
    category_name TEXT,
    category_slug TEXT,
    category_depth INTEGER,
    node_id TEXT,
    bsr_main_rank INTEGER,
    bsr_main_category TEXT,
    bsr_sub_rank INTEGER,
    bsr_sub_category TEXT,
    bsr_sub TEXT,
    variant_option_count INTEGER,
    other_sellers_count INTEGER,
    social_proof TEXT,
    social_proof_count INTEGER,
    item_weight TEXT,
    item_dimensions TEXT,
    weight_lb REAL,
    dim_l_in REAL,
    dim_w_in REAL,
    dim_h_in REAL,
    date_first_available TEXT,
    listing_date TEXT,
    listing_age_days INTEGER,
    shipping_fee TEXT,
    shipping_fee_value REAL,
    fba_fee REAL,
    placement_fee REAL,
    fulfillment_type TEXT,
    country_of_origin TEXT,
    detail_scraped INTEGER DEFAULT 0,
    chart TEXT,
    source_cache_id INTEGER,
    source_run_id TEXT,
    source_list_type TEXT,
    source_node_id TEXT,
    snapshot_json TEXT,
    favorited_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(site, asin)
);
CREATE INDEX IF NOT EXISTS idx_fav_site ON favorite_products(site);
CREATE INDEX IF NOT EXISTS idx_fav_time ON favorite_products(favorited_at DESC, id DESC);
"""

PG_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS favorite_products (
    id SERIAL PRIMARY KEY,
    site TEXT NOT NULL,
    asin TEXT NOT NULL,
    name TEXT,
    title TEXT,
    price REAL,
    price_raw TEXT,
    price_value REAL,
    original_price TEXT,
    discount_pct TEXT,
    rating DOUBLE PRECISION,
    review_count INTEGER,
    rank INTEGER,
    image_url TEXT,
    product_url TEXT,
    has_video INTEGER DEFAULT 0,
    is_amazon_choice INTEGER DEFAULT 0,
    is_bestseller INTEGER DEFAULT 0,
    list_type TEXT,
    list_total INTEGER,
    category_name TEXT,
    category_slug TEXT,
    category_depth INTEGER,
    node_id TEXT,
    bsr_main_rank INTEGER,
    bsr_main_category TEXT,
    bsr_sub_rank INTEGER,
    bsr_sub_category TEXT,
    bsr_sub TEXT,
    variant_option_count INTEGER,
    other_sellers_count INTEGER,
    social_proof TEXT,
    social_proof_count INTEGER,
    item_weight TEXT,
    item_dimensions TEXT,
    weight_lb REAL,
    dim_l_in REAL,
    dim_w_in REAL,
    dim_h_in REAL,
    date_first_available TEXT,
    listing_date TEXT,
    listing_age_days INTEGER,
    shipping_fee TEXT,
    shipping_fee_value REAL,
    fba_fee REAL,
    placement_fee REAL,
    fulfillment_type TEXT,
    country_of_origin TEXT,
    detail_scraped INTEGER DEFAULT 0,
    chart TEXT,
    source_cache_id INTEGER,
    source_run_id TEXT,
    source_list_type TEXT,
    source_node_id TEXT,
    snapshot_json TEXT,
    favorited_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(site, asin)
);
CREATE INDEX IF NOT EXISTS idx_fav_site ON favorite_products(site);
CREATE INDEX IF NOT EXISTS idx_fav_time ON favorite_products(favorited_at DESC, id DESC);
"""


def _db_path(db_path: str | None = None) -> str:
    path = db_path or DB_FILE
    assert_testing_paths_safe(db_path=path)
    return path


def _snapshot_from_cache_row(row: dict) -> dict:
    out = {k: row.get(k) for k in SNAPSHOT_FIELDS}
    out["site"] = (row.get("site") or "").upper()
    out["asin"] = (row.get("asin") or "").upper()
    if not out.get("name") and out.get("title"):
        out["name"] = out["title"]
    if out.get("price") is None and out.get("price_value") is not None:
        out["price"] = out["price_value"]
    return out


def ensure_sqlite_schema(db_path: str | None = None) -> None:
    path = _db_path(db_path)
    with _LOCK:
        con = sqlite3.connect(path, timeout=30)
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.executescript(_SQLITE_SCHEMA)
            con.commit()
        finally:
            con.close()


def ensure_pg_schema(pg_exec_sync) -> None:
    """pg_exec_sync(sql) 同步执行多语句或逐条。"""
    for stmt in PG_SCHEMA_SQL.strip().split(";"):
        s = stmt.strip()
        if s:
            pg_exec_sync(s)


def favorite_key_set(backend: str, site: str | None = None) -> set[tuple[str, str]]:
    site = (site or "").strip().upper() or None
    if backend == "pg":
        return _favorite_keys_pg(site)
    return _favorite_keys_sqlite(site)


def _favorite_keys_sqlite(site: str | None) -> set[tuple[str, str]]:
    path = _db_path()
    ensure_sqlite_schema(path)
    with _LOCK:
        con = sqlite3.connect(path, timeout=15)
        try:
            if site:
                rows = con.execute(
                    "SELECT site, asin FROM favorite_products WHERE site=?", (site,)
                ).fetchall()
            else:
                rows = con.execute("SELECT site, asin FROM favorite_products").fetchall()
            return {(r[0], r[1]) for r in rows}
        finally:
            con.close()


def _favorite_keys_pg(site: str | None) -> set[tuple[str, str]]:
    import psycopg2
    from pg_config import get_pg_dsn
    conn = psycopg2.connect(get_pg_dsn())
    try:
        cur = conn.cursor()
        if site:
            cur.execute("SELECT site, asin FROM favorite_products WHERE site=%s", (site,))
        else:
            cur.execute("SELECT site, asin FROM favorite_products")
        return {(r[0], r[1]) for r in cur.fetchall()}
    finally:
        conn.close()


def upsert_favorite_from_cache(backend: str, cache_row: dict) -> dict:
    snap = _snapshot_from_cache_row(cache_row)
    meta = {
        "source_cache_id": cache_row.get("cache_id"),
        "source_run_id": cache_row.get("run_id"),
        "source_list_type": cache_row.get("list_type"),
        "source_node_id": cache_row.get("node_id"),
        "snapshot_json": json.dumps(dict(cache_row), ensure_ascii=False, default=str),
    }
    if backend == "pg":
        return _upsert_pg(snap, meta)
    return _upsert_sqlite(snap, meta)


def _upsert_sqlite(snap: dict, meta: dict) -> dict:
    path = _db_path()
    ensure_sqlite_schema(path)
    cols = list(SNAPSHOT_FIELDS) + [
        "site", "asin", "source_cache_id", "source_run_id",
        "source_list_type", "source_node_id", "snapshot_json",
    ]
    vals = []
    for c in cols:
        if c in ("site", "asin"):
            vals.append(snap[c])
        elif c in meta:
            vals.append(meta[c])
        else:
            vals.append(snap.get(c))
    placeholders = ",".join("?" * len(cols))
    updates = ", ".join(
        f"{c}=excluded.{c}" for c in cols
        if c not in ("site", "asin")
    )
    sql = f"""
        INSERT INTO favorite_products ({','.join(cols)}, favorited_at, updated_at)
        VALUES ({placeholders}, datetime('now'), datetime('now'))
        ON CONFLICT(site, asin) DO UPDATE SET
            {updates},
            updated_at=datetime('now')
    """
    with _LOCK:
        con = sqlite3.connect(path, timeout=30)
        con.row_factory = sqlite3.Row
        try:
            con.execute(sql, vals)
            con.commit()
            row = con.execute(
                "SELECT * FROM favorite_products WHERE site=? AND asin=?",
                (snap["site"], snap["asin"]),
            ).fetchone()
            return dict(row)
        finally:
            con.close()


def _upsert_pg(snap: dict, meta: dict) -> dict:
    import psycopg2
    import psycopg2.extras
    from pg_config import get_pg_dsn
    cols = list(SNAPSHOT_FIELDS) + [
        "site", "asin", "source_cache_id", "source_run_id",
        "source_list_type", "source_node_id", "snapshot_json",
    ]
    vals = []
    for c in cols:
        if c in ("site", "asin"):
            vals.append(snap[c])
        elif c in meta:
            vals.append(meta[c])
        else:
            vals.append(snap.get(c))
    ph = ",".join(["%s"] * len(cols))
    updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in ("site", "asin"))
    sql = f"""
        INSERT INTO favorite_products ({','.join(cols)}, favorited_at, updated_at)
        VALUES ({ph}, now(), now())
        ON CONFLICT(site, asin) DO UPDATE SET
            {updates},
            updated_at=now()
        RETURNING *
    """
    conn = psycopg2.connect(get_pg_dsn())
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, vals)
        row = cur.fetchone()
        conn.commit()
        return dict(row)
    finally:
        conn.close()


def delete_favorite(backend: str, site: str, asin: str) -> bool:
    site = site.strip().upper()
    asin = asin.strip().upper()
    if backend == "pg":
        import psycopg2
        from pg_config import get_pg_dsn
        conn = psycopg2.connect(get_pg_dsn())
        try:
            cur = conn.cursor()
            cur.execute("DELETE FROM favorite_products WHERE site=%s AND asin=%s", (site, asin))
            conn.commit()
            return cur.rowcount > 0
        finally:
            conn.close()
    path = _db_path()
    ensure_sqlite_schema(path)
    with _LOCK:
        con = sqlite3.connect(path, timeout=30)
        try:
            cur = con.execute(
                "DELETE FROM favorite_products WHERE site=? AND asin=?", (site, asin)
            )
            con.commit()
            return cur.rowcount > 0
        finally:
            con.close()


def count_favorites(backend: str, site: str | None = None) -> int:
    site = (site or "").strip().upper() or None
    if backend == "pg":
        import psycopg2
        from pg_config import get_pg_dsn
        conn = psycopg2.connect(get_pg_dsn())
        try:
            cur = conn.cursor()
            if site:
                cur.execute("SELECT COUNT(*) FROM favorite_products WHERE site=%s", (site,))
            else:
                cur.execute("SELECT COUNT(*) FROM favorite_products")
            return int(cur.fetchone()[0])
        finally:
            conn.close()
    path = _db_path()
    ensure_sqlite_schema(path)
    with _LOCK:
        con = sqlite3.connect(path, timeout=15)
        try:
            if site:
                return int(con.execute(
                    "SELECT COUNT(*) FROM favorite_products WHERE site=?", (site,)
                ).fetchone()[0])
            return int(con.execute("SELECT COUNT(*) FROM favorite_products").fetchone()[0])
        finally:
            con.close()


def list_favorites(
    backend: str,
    *,
    site: str | None = None,
    q: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    """列表 API：单页最多 200；稳定排序 favorited_at,id。"""
    site = (site or "").strip().upper() or None
    q = (q or "").strip()
    limit = max(1, min(int(limit), _LIST_PAGE_MAX))
    offset = max(0, int(offset))
    if backend == "pg":
        return _list_pg(site, q, limit, offset)
    return _list_sqlite(site, q, limit, offset)


def list_favorites_after(
    backend: str,
    *,
    site: str | None = None,
    q: str | None = None,
    limit: int = 200,
    after_at: Any = None,
    after_id: int | None = None,
) -> list[dict]:
    """键集分页：WHERE (favorited_at,id) < cursor，避免 OFFSET 漂移。"""
    site = (site or "").strip().upper() or None
    q = (q or "").strip()
    limit = max(1, min(int(limit), _LIST_PAGE_MAX))
    if backend == "pg":
        return _list_pg_after(site, q, limit, after_at, after_id)
    return _list_sqlite_after(site, q, limit, after_at, after_id)


def list_all_favorites(
    backend: str,
    *,
    site: str | None = None,
    q: str | None = None,
    page_size: int = 200,
) -> list[dict]:
    """导出用：键集分页循环读取，不被 API 单页上限截断。"""
    page_size = max(1, min(int(page_size), _LIST_PAGE_MAX))
    out: list[dict] = []
    after_at: Any = None
    after_id: int | None = None
    while True:
        chunk = list_favorites_after(
            backend, site=site, q=q, limit=page_size,
            after_at=after_at, after_id=after_id,
        )
        if not chunk:
            break
        out.extend(chunk)
        if len(chunk) < page_size:
            break
        last = chunk[-1]
        after_at = last.get("favorited_at")
        after_id = int(last["id"])
    return out


def _filter_sql_sqlite(site, q) -> tuple[str, list[Any]]:
    sql = "SELECT * FROM favorite_products WHERE 1=1"
    params: list[Any] = []
    if site:
        sql += " AND site=?"
        params.append(site)
    if q:
        sql += " AND (asin LIKE ? OR IFNULL(name,'') LIKE ? OR IFNULL(title,'') LIKE ?)"
        like = f"%{q}%"
        params.extend([like, like, like])
    return sql, params


def _list_sqlite(site, q, limit, offset) -> list[dict]:
    path = _db_path()
    ensure_sqlite_schema(path)
    sql, params = _filter_sql_sqlite(site, q)
    sql += " ORDER BY favorited_at DESC, id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    with _LOCK:
        con = sqlite3.connect(path, timeout=15)
        con.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in con.execute(sql, params).fetchall()]
        finally:
            con.close()


def _list_sqlite_after(site, q, limit, after_at, after_id) -> list[dict]:
    path = _db_path()
    ensure_sqlite_schema(path)
    sql, params = _filter_sql_sqlite(site, q)
    if after_at is not None and after_id is not None:
        sql += " AND (favorited_at < ? OR (favorited_at = ? AND id < ?))"
        params.extend([after_at, after_at, int(after_id)])
    sql += " ORDER BY favorited_at DESC, id DESC LIMIT ?"
    params.append(limit)
    with _LOCK:
        con = sqlite3.connect(path, timeout=15)
        con.row_factory = sqlite3.Row
        try:
            return [dict(r) for r in con.execute(sql, params).fetchall()]
        finally:
            con.close()


def _filter_sql_pg(site, q) -> tuple[str, list[Any]]:
    sql = "SELECT * FROM favorite_products WHERE 1=1"
    params: list[Any] = []
    if site:
        params.append(site)
        sql += " AND site=%s"
    if q:
        params.extend([f"%{q}%", f"%{q}%", f"%{q}%"])
        sql += " AND (asin ILIKE %s OR COALESCE(name,'') ILIKE %s OR COALESCE(title,'') ILIKE %s)"
    return sql, params


def _list_pg(site, q, limit, offset) -> list[dict]:
    import psycopg2
    import psycopg2.extras
    from pg_config import get_pg_dsn
    sql, params = _filter_sql_pg(site, q)
    params.extend([limit, offset])
    sql += " ORDER BY favorited_at DESC, id DESC LIMIT %s OFFSET %s"
    conn = psycopg2.connect(get_pg_dsn())
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def _list_pg_after(site, q, limit, after_at, after_id) -> list[dict]:
    import psycopg2
    import psycopg2.extras
    from pg_config import get_pg_dsn
    sql, params = _filter_sql_pg(site, q)
    if after_at is not None and after_id is not None:
        sql += " AND (favorited_at < %s OR (favorited_at = %s AND id < %s))"
        params.extend([after_at, after_at, int(after_id)])
    params.append(limit)
    sql += " ORDER BY favorited_at DESC, id DESC LIMIT %s"
    conn = psycopg2.connect(get_pg_dsn())
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()
