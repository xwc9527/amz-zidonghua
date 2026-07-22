"""按 run_id 隔离的跨进程商品运行缓存（独立 SQLite/WAL，非正式库）。"""
from __future__ import annotations

import os
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterable

from config import PRODUCT_RUN_CACHE_FILE, assert_testing_paths_safe

_ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")
_SITE_RE = re.compile(r"^[A-Z]{2}$")
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_LIST_TYPE_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_CHART_RE = re.compile(r"^(products|la)$")

_LOCK = threading.RLock()
_INITIALIZED = False


class StaleCacheError(RuntimeError):
    """active run 已换代或 cache_id 不存在。"""

    def __init__(self, message: str, *, active_run_id: str | None = None):
        super().__init__(message)
        self.active_run_id = active_run_id
        self.error_code = "STALE_CACHE_ITEM"


# 允许 UPDATE detail 的列白名单（动态 SQL 仅从此集合取列名）
DETAIL_UPDATE_COLS = frozenset({
    "name", "title", "price", "price_raw", "price_value", "original_price", "discount_pct",
    "rating", "review_count", "rank", "image_url", "product_url",
    "has_video", "is_amazon_choice", "is_bestseller",
    "list_total", "category_name", "category_slug", "category_depth",
    "bsr_main_rank", "bsr_main_category", "bsr_sub_rank", "bsr_sub_category", "bsr_sub",
    "variant_option_count", "other_sellers_count",
    "social_proof", "social_proof_count",
    "item_weight", "item_dimensions",
    "weight_lb", "dim_l_in", "dim_w_in", "dim_h_in",
    "date_first_available", "listing_date", "listing_age_days",
    "shipping_fee", "shipping_fee_value",
    "fba_fee", "placement_fee", "fulfillment_type", "country_of_origin",
    "detail_scraped", "detail_status", "run_id",
})

CACHED_DETAIL_KEYS = (
    "bsr_main_rank", "bsr_main_category", "bsr_sub_rank", "bsr_sub_category", "bsr_sub",
    "variant_option_count", "other_sellers_count",
    "social_proof", "social_proof_count",
    "item_weight", "item_dimensions",
    "weight_lb", "dim_l_in", "dim_w_in", "dim_h_in",
    "date_first_available", "listing_date", "listing_age_days",
    "shipping_fee", "shipping_fee_value",
    "fulfillment_type", "country_of_origin",
    "is_bestseller", "is_amazon_choice",
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS cache_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS product_cache (
    cache_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    chart TEXT NOT NULL,
    site TEXT NOT NULL,
    asin TEXT NOT NULL,
    node_id TEXT NOT NULL DEFAULT '',
    list_type TEXT NOT NULL DEFAULT '',
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
    list_total INTEGER,
    category_name TEXT,
    category_slug TEXT,
    category_depth INTEGER,
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
    detail_status TEXT DEFAULT 'pending',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(run_id, site, asin, node_id, list_type)
);

CREATE INDEX IF NOT EXISTS idx_pc_run ON product_cache(run_id);
CREATE INDEX IF NOT EXISTS idx_pc_active_lookup ON product_cache(run_id, site, chart);
CREATE INDEX IF NOT EXISTS idx_pc_asin ON product_cache(site, asin);
CREATE INDEX IF NOT EXISTS idx_pc_detail ON product_cache(run_id, site, asin, detail_scraped);
"""


def _validate_asin(asin: str) -> str:
    asin = (asin or "").strip().upper()
    if not _ASIN_RE.match(asin):
        raise ValueError(f"invalid asin: {asin!r}")
    return asin


def _validate_site(site: str) -> str:
    site = (site or "").strip().upper()
    if not _SITE_RE.match(site):
        raise ValueError(f"invalid site: {site!r}")
    return site


def _validate_run_id(run_id: str) -> str:
    run_id = (run_id or "").strip()
    if not _RUN_ID_RE.match(run_id):
        raise ValueError(f"invalid run_id: {run_id!r}")
    return run_id


def _validate_list_type(list_type: str) -> str:
    list_type = (list_type or "").strip()
    if list_type and not _LIST_TYPE_RE.match(list_type):
        raise ValueError(f"invalid list_type: {list_type!r}")
    return list_type


def _validate_chart(chart: str) -> str:
    chart = (chart or "products").strip().lower()
    if chart in ("latest-arrivals", "new-arrivals", "new_arrivals"):
        chart = "la"
    if chart not in ("products", "la"):
        # allow products list_type charts under products
        if not _CHART_RE.match(chart):
            raise ValueError(f"invalid chart: {chart!r}")
    return chart


def cache_path() -> str:
    return os.path.abspath(PRODUCT_RUN_CACHE_FILE)


def _open_raw_conn(*, immediate: bool = False) -> sqlite3.Connection:
    path = cache_path()
    assert_testing_paths_safe(cache_path=path)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    con = sqlite3.connect(path, timeout=30, check_same_thread=False)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    con.execute("PRAGMA synchronous=NORMAL")
    if immediate:
        # 跨进程写锁：收藏与换代互斥，避免 409 竞态漏写
        con.execute("BEGIN IMMEDIATE")
    return con


@contextmanager
def _conn(*, immediate: bool = False):
    con = _open_raw_conn(immediate=immediate)
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def ensure_schema() -> None:
    global _INITIALIZED
    with _LOCK:
        if _INITIALIZED:
            return
        with _conn() as con:
            con.executescript(_SCHEMA)
        _INITIALIZED = True


def _meta_get(con: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = con.execute("SELECT value FROM cache_meta WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def _meta_set(con: sqlite3.Connection, key: str, value: str) -> None:
    con.execute(
        """INSERT INTO cache_meta(key, value, updated_at) VALUES(?,?,datetime('now'))
           ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')""",
        (key, value),
    )


def get_active_run_id() -> str | None:
    ensure_schema()
    with _LOCK:
        with _conn() as con:
            return _meta_get(con, "active_run_id")


def get_active_chart() -> str | None:
    ensure_schema()
    with _LOCK:
        with _conn() as con:
            return _meta_get(con, "active_chart")


def create_generation(run_id: str, chart: str = "products") -> str:
    """创建新代次（写入 pending）；不切换 active，启动失败可保留旧缓存。"""
    run_id = _validate_run_id(run_id)
    chart = _validate_chart(chart)
    ensure_schema()
    with _LOCK:
        with _conn(immediate=True) as con:
            _meta_set(con, "pending_run_id", run_id)
            _meta_set(con, "pending_chart", chart)
            _meta_set(con, f"gen_created:{run_id}", str(time.time()))
    return run_id


def cancel_generation(run_id: str | None = None) -> None:
    """启动失败时清除 pending，保留当前 active。"""
    ensure_schema()
    with _LOCK:
        with _conn(immediate=True) as con:
            pending = _meta_get(con, "pending_run_id") or ""
            if run_id and pending and pending != run_id:
                return
            if run_id and pending == run_id:
                con.execute("DELETE FROM product_cache WHERE run_id=?", (run_id,))
            _meta_set(con, "pending_run_id", "")
            _meta_set(con, "pending_chart", "")


def activate_generation(run_id: str, chart: str | None = None, *, purge_others: bool = True) -> str:
    """子进程成功启动后原子切换 active，并清理其它代次。"""
    run_id = _validate_run_id(run_id)
    ensure_schema()
    with _LOCK:
        with _conn(immediate=True) as con:
            pending = _meta_get(con, "pending_run_id")
            if pending and pending != run_id:
                raise RuntimeError(
                    f"activate_generation run_id mismatch: pending={pending} got={run_id}"
                )
            if chart is None:
                chart = _meta_get(con, "pending_chart") or _meta_get(con, "active_chart") or "products"
            chart = _validate_chart(chart)
            old = _meta_get(con, "active_run_id")
            _meta_set(con, "active_run_id", run_id)
            _meta_set(con, "active_chart", chart)
            _meta_set(con, "pending_run_id", "")
            if purge_others:
                if old and old != run_id:
                    con.execute("DELETE FROM product_cache WHERE run_id=?", (old,))
                con.execute("DELETE FROM product_cache WHERE run_id<>?", (run_id,))
    return run_id


def open_existing_generation(run_id: str) -> str:
    """恢复/续跑：必须与当前 active 一致，禁止误清缓存。"""
    run_id = _validate_run_id(run_id)
    ensure_schema()
    with _LOCK:
        with _conn(immediate=True) as con:
            active = _meta_get(con, "active_run_id")
            if active and active != run_id:
                raise RuntimeError(
                    f"resume run_id mismatch: active={active} requested={run_id}"
                )
            if not active:
                _meta_set(con, "active_run_id", run_id)
            return _meta_get(con, "active_run_id") or run_id


@contextmanager
def locked_active_cache_item(cache_id: int, run_id: str):
    """持有缓存写锁期间校验 active+cache_id，供收藏写入正式表。

    调用方必须在 with 块内完成正式库 upsert；块结束才释放代次锁，
    从而与 activate_generation 互斥。
    """
    ensure_schema()
    run_id = _validate_run_id(run_id)
    cache_id = int(cache_id)
    with _LOCK:
        con = _open_raw_conn(immediate=True)
        try:
            active = _meta_get(con, "active_run_id")
            if not active or active != run_id:
                raise StaleCacheError(
                    "缓存已换代，请刷新后再收藏",
                    active_run_id=active,
                )
            row = con.execute(
                "SELECT * FROM product_cache WHERE cache_id=? AND run_id=?",
                (cache_id, run_id),
            ).fetchone()
            if not row:
                raise StaleCacheError(
                    "缓存条目不存在或已过期",
                    active_run_id=active,
                )
            yield dict(row)
            con.commit()
        except Exception:
            con.rollback()
            raise
        finally:
            con.close()


def _normalize_product(p: dict, *, run_id: str, chart: str, default_site: str) -> dict:
    site = _validate_site(p.get("site") or default_site)
    asin = _validate_asin(p.get("asin"))
    node_id = str(p.get("node_id") or "")
    list_type = _validate_list_type(p.get("list_type") or ("latest-arrivals" if chart == "la" else ""))
    name = p.get("name") or p.get("title")
    title = p.get("title") or p.get("name")
    price = p.get("price")
    price_value = p.get("price_value")
    if price is None and price_value is not None:
        price = price_value
    if price_value is None and price is not None:
        try:
            price_value = float(price)
        except (TypeError, ValueError):
            price_value = None
    return {
        "run_id": run_id,
        "chart": chart,
        "site": site,
        "asin": asin,
        "node_id": node_id,
        "list_type": list_type,
        "name": name,
        "title": title,
        "price": price,
        "price_raw": p.get("price_raw") or (str(p.get("price")) if isinstance(p.get("price"), str) else p.get("price_raw")),
        "price_value": price_value,
        "original_price": p.get("original_price"),
        "discount_pct": p.get("discount_pct"),
        "rating": p.get("rating"),
        "review_count": p.get("review_count"),
        "rank": p.get("rank"),
        "image_url": p.get("image_url"),
        "product_url": p.get("product_url"),
        "has_video": int(p.get("has_video") or 0),
        "is_amazon_choice": int(p.get("is_amazon_choice") or 0),
        "is_bestseller": int(p.get("is_bestseller") or 0),
        "list_total": p.get("list_total"),
        "category_name": p.get("category_name"),
        "category_slug": p.get("category_slug"),
        "category_depth": p.get("category_depth"),
        "bsr_main_rank": p.get("bsr_main_rank"),
        "bsr_main_category": p.get("bsr_main_category"),
        "bsr_sub_rank": p.get("bsr_sub_rank"),
        "bsr_sub_category": p.get("bsr_sub_category"),
        "bsr_sub": p.get("bsr_sub"),
        "variant_option_count": p.get("variant_option_count"),
        "other_sellers_count": p.get("other_sellers_count"),
        "social_proof": p.get("social_proof"),
        "social_proof_count": p.get("social_proof_count"),
        "item_weight": p.get("item_weight"),
        "item_dimensions": p.get("item_dimensions"),
        "weight_lb": p.get("weight_lb"),
        "dim_l_in": p.get("dim_l_in"),
        "dim_w_in": p.get("dim_w_in"),
        "dim_h_in": p.get("dim_h_in"),
        "date_first_available": p.get("date_first_available") or p.get("listing_date"),
        "listing_date": p.get("listing_date") or p.get("date_first_available"),
        "listing_age_days": p.get("listing_age_days"),
        "shipping_fee": p.get("shipping_fee"),
        "shipping_fee_value": p.get("shipping_fee_value"),
        "fba_fee": p.get("fba_fee"),
        "placement_fee": p.get("placement_fee"),
        "fulfillment_type": p.get("fulfillment_type"),
        "country_of_origin": p.get("country_of_origin"),
        "detail_scraped": int(p.get("detail_scraped") or 0),
        "detail_status": p.get("detail_status") or ("ok" if p.get("detail_scraped") == 1 else "pending"),
    }


def upsert_products(
    products: Iterable[dict],
    *,
    run_id: str,
    chart: str = "products",
    default_site: str = "US",
) -> int:
    """写入/刷新列表字段；已有详情行保留详情列。"""
    run_id = _validate_run_id(run_id)
    chart = _validate_chart(chart)
    ensure_schema()
    rows = [_normalize_product(p, run_id=run_id, chart=chart, default_site=default_site) for p in products]
    if not rows:
        return 0
    sql = """
        INSERT INTO product_cache (
            run_id, chart, site, asin, node_id, list_type,
            name, title, price, price_raw, price_value, original_price, discount_pct,
            rating, review_count, rank, image_url, product_url,
            has_video, is_amazon_choice, is_bestseller, list_total,
            category_name, category_slug, category_depth,
            bsr_main_rank, bsr_main_category, bsr_sub_rank, bsr_sub_category, bsr_sub,
            variant_option_count, other_sellers_count, social_proof, social_proof_count,
            item_weight, item_dimensions, weight_lb, dim_l_in, dim_w_in, dim_h_in,
            date_first_available, listing_date, listing_age_days,
            shipping_fee, shipping_fee_value, fba_fee, placement_fee,
            fulfillment_type, country_of_origin, detail_scraped, detail_status,
            created_at, updated_at
        ) VALUES (
            :run_id, :chart, :site, :asin, :node_id, :list_type,
            :name, :title, :price, :price_raw, :price_value, :original_price, :discount_pct,
            :rating, :review_count, :rank, :image_url, :product_url,
            :has_video, :is_amazon_choice, :is_bestseller, :list_total,
            :category_name, :category_slug, :category_depth,
            :bsr_main_rank, :bsr_main_category, :bsr_sub_rank, :bsr_sub_category, :bsr_sub,
            :variant_option_count, :other_sellers_count, :social_proof, :social_proof_count,
            :item_weight, :item_dimensions, :weight_lb, :dim_l_in, :dim_w_in, :dim_h_in,
            :date_first_available, :listing_date, :listing_age_days,
            :shipping_fee, :shipping_fee_value, :fba_fee, :placement_fee,
            :fulfillment_type, :country_of_origin, :detail_scraped, :detail_status,
            datetime('now'), datetime('now')
        )
        ON CONFLICT(run_id, site, asin, node_id, list_type) DO UPDATE SET
            chart=excluded.chart,
            name=COALESCE(excluded.name, product_cache.name),
            title=COALESCE(excluded.title, product_cache.title),
            price=COALESCE(excluded.price, product_cache.price),
            price_raw=COALESCE(excluded.price_raw, product_cache.price_raw),
            price_value=COALESCE(excluded.price_value, product_cache.price_value),
            rating=COALESCE(excluded.rating, product_cache.rating),
            review_count=COALESCE(excluded.review_count, product_cache.review_count),
            rank=COALESCE(excluded.rank, product_cache.rank),
            image_url=COALESCE(excluded.image_url, product_cache.image_url),
            product_url=COALESCE(excluded.product_url, product_cache.product_url),
            list_total=COALESCE(excluded.list_total, product_cache.list_total),
            category_name=COALESCE(excluded.category_name, product_cache.category_name),
            category_slug=COALESCE(excluded.category_slug, product_cache.category_slug),
            category_depth=COALESCE(excluded.category_depth, product_cache.category_depth),
            updated_at=datetime('now')
    """
    with _LOCK:
        with _conn() as con:
            con.executemany(sql, rows)
    return len(rows)


def load_cached_detail(asin: str, *, run_id: str, site: str) -> dict | None:
    """同轮详情复用：仅相同 run_id + site + asin。"""
    asin = _validate_asin(asin)
    run_id = _validate_run_id(run_id)
    site = _validate_site(site)
    ensure_schema()
    cols = ", ".join(CACHED_DETAIL_KEYS)
    with _LOCK:
        with _conn() as con:
            row = con.execute(
                f"""SELECT {cols} FROM product_cache
                    WHERE run_id=? AND site=? AND asin=? AND detail_scraped=1
                    LIMIT 1""",
                (run_id, site, asin),
            ).fetchone()
            if not row:
                return None
            return {k: row[k] for k in CACHED_DETAIL_KEYS}


def update_detail(
    asin: str,
    detail: dict,
    *,
    run_id: str,
    site: str,
    node_id: str | None = None,
    list_type: str | None = None,
) -> int:
    asin = _validate_asin(asin)
    run_id = _validate_run_id(run_id)
    site = _validate_site(site)
    payload = {k: v for k, v in detail.items() if k in DETAIL_UPDATE_COLS}
    if not payload:
        return 0
    payload["detail_scraped"] = int(payload.get("detail_scraped") or 1)
    payload["detail_status"] = payload.get("detail_status") or "ok"
    payload["run_id"] = run_id
    sets = ", ".join(f"{k}=?" for k in payload)
    vals = list(payload.values())
    ensure_schema()
    with _LOCK:
        with _conn() as con:
            if node_id is not None and list_type is not None:
                vals2 = vals + [asin, site, run_id, node_id, list_type]
                cur = con.execute(
                    f"""UPDATE product_cache SET {sets}, updated_at=datetime('now')
                        WHERE asin=? AND site=? AND run_id=? AND node_id=? AND list_type=?""",
                    vals2,
                )
            else:
                vals2 = vals + [asin, site, run_id]
                cur = con.execute(
                    f"""UPDATE product_cache SET {sets}, updated_at=datetime('now')
                        WHERE asin=? AND site=? AND run_id=?""",
                    vals2,
                )
            return cur.rowcount


def mark_detail_failed(
    asin: str,
    *,
    run_id: str,
    site: str,
    node_id: str | None = None,
    list_type: str | None = None,
) -> int:
    return update_detail(
        asin,
        {"detail_scraped": 2, "detail_status": "failed", "run_id": run_id},
        run_id=run_id,
        site=site,
        node_id=node_id,
        list_type=list_type,
    )


def delete_item(
    asin: str,
    *,
    run_id: str,
    site: str,
    node_id: str | None = None,
    list_type: str | None = None,
) -> int:
    asin = _validate_asin(asin)
    run_id = _validate_run_id(run_id)
    site = _validate_site(site)
    ensure_schema()
    with _LOCK:
        with _conn() as con:
            if node_id is not None and list_type is not None:
                cur = con.execute(
                    """DELETE FROM product_cache
                       WHERE asin=? AND site=? AND run_id=? AND node_id=? AND list_type=?""",
                    (asin, site, run_id, node_id, list_type),
                )
            else:
                cur = con.execute(
                    "DELETE FROM product_cache WHERE asin=? AND site=? AND run_id=?",
                    (asin, site, run_id),
                )
            return cur.rowcount


def get_by_cache_id(cache_id: int, *, run_id: str | None = None) -> dict | None:
    ensure_schema()
    with _LOCK:
        with _conn() as con:
            if run_id:
                run_id = _validate_run_id(run_id)
                row = con.execute(
                    "SELECT * FROM product_cache WHERE cache_id=? AND run_id=?",
                    (int(cache_id), run_id),
                ).fetchone()
            else:
                row = con.execute(
                    "SELECT * FROM product_cache WHERE cache_id=?",
                    (int(cache_id),),
                ).fetchone()
            return dict(row) if row else None


def _build_where(filters: dict, *, chart: str | None, active_run_id: str) -> tuple[str, list]:
    sql = " WHERE run_id=?"
    params: list[Any] = [active_run_id]
    if chart:
        sql += " AND chart=?"
        params.append(_validate_chart(chart))
    site = filters.get("site")
    if site:
        sql += " AND site=?"
        params.append(_validate_site(site))
    if filters.get("detail_only", True) and chart != "la":
        sql += " AND detail_scraped=1"
    # LA 默认也要求有有效 ASIN；详情字段在 LA 路径通常一次写全
    for key, op, col in [
        ("price_min", ">=", "price"), ("price_max", "<=", "price"),
        ("rating_min", ">=", "rating"), ("rating_max", "<=", "rating"),
        ("review_min", ">=", "review_count"), ("review_max", "<=", "review_count"),
        ("bsr_main_min", ">=", "bsr_main_rank"), ("bsr_main_max", "<=", "bsr_main_rank"),
        ("bsr_sub_min", ">=", "bsr_sub_rank"), ("bsr_sub_max", "<=", "bsr_sub_rank"),
        ("variant_min", ">=", "variant_option_count"), ("variant_max", "<=", "variant_option_count"),
        ("sellers_min", ">=", "other_sellers_count"), ("sellers_max", "<=", "other_sellers_count"),
        ("social_proof_min", ">=", "social_proof_count"),
        ("fba_fee_min", ">=", "fba_fee"), ("fba_fee_max", "<=", "fba_fee"),
        ("weight_min", ">=", "weight_lb"), ("weight_max", "<=", "weight_lb"),
    ]:
        val = filters.get(key)
        if val is not None and val != 0 and val is not False:
            sql += f" AND {col} IS NOT NULL AND {col} {op} ?"
            params.append(val)
    for key, col in (("dim_l", "dim_l_in"), ("dim_w", "dim_w_in"), ("dim_h", "dim_h_in")):
        val = filters.get(key)
        if val is not None and val != 0:
            sql += f" AND {col} IS NOT NULL AND {col} <= ?"
            params.append(val)
    ft = filters.get("fulfillment_type") or ""
    if ft:
        sql += " AND fulfillment_type=?"
        params.append(ft)
    country = (filters.get("country") or "").strip()
    if country:
        sql += " AND country_of_origin IS NOT NULL AND LOWER(country_of_origin) LIKE ?"
        params.append(f"%{country.lower()}%")
    if filters.get("amazons_choice"):
        sql += " AND is_amazon_choice=1"
    if filters.get("bestseller"):
        sql += " AND is_bestseller=1"
    date_range = filters.get("date_range") or ""
    date_col = "listing_date" if chart == "la" else "date_first_available"
    if date_range:
        sql += f" AND {date_col} IS NOT NULL"
        if date_range == "custom":
            if filters.get("date_from"):
                sql += f" AND {date_col} >= ?"
                params.append(filters["date_from"])
            if filters.get("date_to"):
                sql += f" AND {date_col} <= ?"
                params.append(filters["date_to"])
        else:
            try:
                from datetime import datetime, timedelta
                days = int(date_range)
                cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
                sql += f" AND {date_col} >= ?"
                params.append(cutoff)
            except ValueError:
                pass
    return sql, params


def query_products(filters: dict | None = None, *, chart: str = "products") -> list[dict]:
    filters = dict(filters or {})
    ensure_schema()
    with _LOCK:
        with _conn() as con:
            active = _meta_get(con, "active_run_id")
            if not active:
                return []
            where, params = _build_where(filters, chart=chart, active_run_id=active)
            limit = int(filters.get("limit") or 50)
            offset = int(filters.get("offset") or 0)
            rows = con.execute(
                f"""SELECT cache_id, run_id, chart, site, asin, node_id, list_type,
                           name, title, price, price_raw, price_value, original_price, discount_pct,
                           rating, review_count, rank, image_url, product_url,
                           has_video, is_amazon_choice, is_bestseller, list_total,
                           category_name, category_slug, category_depth,
                           bsr_main_rank, bsr_main_category, bsr_sub_rank, bsr_sub_category, bsr_sub,
                           variant_option_count, other_sellers_count, social_proof, social_proof_count,
                           item_weight, item_dimensions, weight_lb, dim_l_in, dim_w_in, dim_h_in,
                           date_first_available, listing_date, listing_age_days,
                           shipping_fee, shipping_fee_value, fba_fee, placement_fee,
                           fulfillment_type, country_of_origin, detail_scraped, detail_status,
                           created_at, updated_at, updated_at AS scraped_at
                    FROM product_cache{where}
                    ORDER BY updated_at DESC
                    LIMIT ? OFFSET ?""",
                [*params, limit, offset],
            ).fetchall()
            out = []
            for r in rows:
                d = dict(r)
                if not d.get("name") and d.get("title"):
                    d["name"] = d["title"]
                if d.get("price") is None and d.get("price_value") is not None:
                    d["price"] = d["price_value"]
                if not d.get("date_first_available") and d.get("listing_date"):
                    d["date_first_available"] = d["listing_date"]
                out.append(d)
            return out


def stats(site: str | None = None, run_id: str | None = None) -> dict:
    ensure_schema()
    with _LOCK:
        with _conn() as con:
            active = run_id or _meta_get(con, "active_run_id")
            if not active:
                return {
                    "total_asins": 0, "new_arrivals": 0, "by_list": [], "multi_list": 0,
                    "detail_ok": 0, "detail_failed": 0, "run_id": "", "active_chart": None,
                }
            params: list[Any] = [active]
            site_sql = ""
            if site:
                site_sql = " AND site=?"
                params.append(_validate_site(site))
            total = con.execute(
                f"SELECT COUNT(DISTINCT asin) FROM product_cache WHERE run_id=? AND chart='products'{site_sql}",
                params,
            ).fetchone()[0]
            na_total = con.execute(
                f"SELECT COUNT(DISTINCT asin) FROM product_cache WHERE run_id=? AND chart='la'{site_sql}",
                params,
            ).fetchone()[0]
            by_list = [
                dict(r) for r in con.execute(
                    f"""SELECT list_type, COUNT(*) AS cnt FROM product_cache
                        WHERE run_id=? AND chart='products'{site_sql}
                        GROUP BY list_type""",
                    params,
                ).fetchall()
            ]
            multi = con.execute(
                f"""SELECT COUNT(*) FROM (
                        SELECT asin FROM product_cache
                        WHERE run_id=? AND chart='products'{site_sql}
                        GROUP BY asin HAVING COUNT(DISTINCT list_type)>1
                    ) t""",
                params,
            ).fetchone()[0]
            detail_ok = con.execute(
                f"""SELECT COUNT(DISTINCT asin) FROM product_cache
                    WHERE run_id=? AND detail_scraped=1{site_sql}""",
                params,
            ).fetchone()[0]
            detail_failed = con.execute(
                f"""SELECT COUNT(DISTINCT asin) FROM product_cache
                    WHERE run_id=? AND detail_scraped=2{site_sql}""",
                params,
            ).fetchone()[0]
            return {
                "total_asins": total,
                "new_arrivals": na_total,
                "by_list": by_list,
                "multi_list": multi,
                "detail_ok": detail_ok,
                "detail_failed": detail_failed,
                "run_id": active,
                "active_chart": _meta_get(con, "active_chart"),
            }


def favorite_keys_for_pairs(pairs: Iterable[tuple[str, str]]) -> set[tuple[str, str]]:
    """占位：由 API 层用正式收藏表批量查询后注入；此处不读正式库。"""
    return set()
