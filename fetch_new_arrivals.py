"""
fetch_new_arrivals.py — 最新到货商品抓取（多 worker 并发）

走 Amazon 搜索接口 /s?rh=n:{node_id}&s=date-desc-rank 抓取按上架时间排序的商品列表，
再进详情页补全 BSR / 重量尺寸 / FBA 等字段，按用户筛选条件过滤后入库。

两阶段：
  Phase 1: 搜索列表页 → 提取 ASIN + 价格/评分/评论数（列表级筛选）
  Phase 2: 详情页 → 复用 detail_parser 提取详情字段 → 详情级筛选 → 入库

用法:
  python fetch_new_arrivals.py --site DE --roots 16435051
  python fetch_new_arrivals.py --site US --roots appliances --price-min 10 --price-max 50
  python fetch_new_arrivals.py --site DE --max-pages 5 --phase1-only
"""

import json, os, re, sys, time, random, sqlite3, threading, argparse, logging, traceback
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── 日志 ──────────────────────────────────────────────────────────
_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "fetch_new_arrivals.log")
_log = logging.getLogger("fetch_new_arrivals")
_log.setLevel(logging.DEBUG)
_fh = logging.FileHandler(_LOG_PATH, encoding="utf-8")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_sh = logging.StreamHandler(sys.stdout)
_sh.setLevel(logging.INFO)
_sh.setFormatter(logging.Formatter("%(message)s"))
_log.addHandler(_fh)
_log.addHandler(_sh)

from curl_cffi import requests as requests
from curl_cffi.requests import RequestsError
from bs4 import BeautifulSoup

from config import (
    HEADERS, DATA_DIR, DB_FILE, get_marketplace,
)
from proxy_session import (
    ForcedProxyPool,
    ProxyRequiredError,
    assert_session_has_proxy,
    make_forced_session,
)
from detail_parser import (
    parse_detail_fields, check_detail_filters, attach_normalized_dims,
    extract_image_url,
)
from fba_fees_us import estimate_fba_fees

DB_BACKEND = os.getenv("DB_BACKEND", "pg")

# PG support（与 fetch_products.py 对齐）
_pg_conn = None


def _get_pg():
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        import psycopg2
        from pg_config import get_pg_dsn
        _pg_conn = psycopg2.connect(get_pg_dsn())
        _pg_conn.autocommit = True
    return _pg_conn


def _pg_fetchall(sql, params=()):
    conn = _get_pg()
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _pg_execute(sql, params=()):
    conn = _get_pg()
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.rowcount


# ── 运行时站点配置 ──
_mp = get_marketplace("US")
_SITE = "US"
_DOMAIN = _mp["domain"]
_LANG = _mp["lang"]
_DECIMAL_SEP = _mp.get("decimal_sep", ".")
_RATING_PAT = _mp.get("rating_pattern", r"([\d.]+)\s+out")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

MAX_PAGES_PER_NODE = 10

# 运行时筛选（由 CLI / API 注入）
_LIST_FILTERS: dict = {}
_DETAIL_FILTERS: dict = {}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 代理池
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ProxyPool(ForcedProxyPool):
    """兼容旧名称；强制代理，禁止静默直连。"""

    def __init__(self):
        super().__init__(required=True)
        _log.info(f"[pool] 强制加载 {self.size} 个代理端口")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_db_lock = threading.Lock()

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS new_arrivals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    asin            TEXT NOT NULL,
    title           TEXT,
    price           TEXT,
    price_value     REAL,
    rating          REAL,
    review_count    INTEGER DEFAULT 0,
    listing_date    TEXT,
    listing_age_days INTEGER,
    bsr_main_category TEXT,
    bsr_main_rank   INTEGER,
    bsr_sub         TEXT,
    bsr_sub_rank    INTEGER,
    bsr_sub_category TEXT,
    image_url       TEXT,
    product_url     TEXT,
    node_id         TEXT,
    category_name   TEXT,
    category_depth  INTEGER,
    site            TEXT DEFAULT 'US',
    item_weight     TEXT,
    item_dimensions TEXT,
    weight_lb       REAL,
    dim_l_in        REAL,
    dim_w_in        REAL,
    dim_h_in        REAL,
    variant_option_count INTEGER,
    other_sellers_count INTEGER,
    fba_fee         REAL,
    placement_fee   REAL,
    fulfillment_type TEXT,
    country_of_origin TEXT,
    is_amazon_choice INTEGER DEFAULT 0,
    is_bestseller   INTEGER DEFAULT 0,
    scraped_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(asin, node_id, site)
)
"""

_NA_EXTRA_COLS = [
    ("bsr_sub_rank", "INTEGER"),
    ("bsr_sub_category", "TEXT"),
    ("item_weight", "TEXT"),
    ("item_dimensions", "TEXT"),
    ("weight_lb", "REAL"),
    ("dim_l_in", "REAL"),
    ("dim_w_in", "REAL"),
    ("dim_h_in", "REAL"),
    ("variant_option_count", "INTEGER"),
    ("other_sellers_count", "INTEGER"),
    ("fba_fee", "REAL"),
    ("placement_fee", "REAL"),
    ("fulfillment_type", "TEXT"),
    ("country_of_origin", "TEXT"),
    ("is_amazon_choice", "INTEGER DEFAULT 0"),
    ("is_bestseller", "INTEGER DEFAULT 0"),
]


CREATE_TABLE_PG_SQL = """
CREATE TABLE IF NOT EXISTS new_arrivals (
    id                   SERIAL PRIMARY KEY,
    asin                 TEXT NOT NULL,
    title                TEXT,
    price                TEXT,
    price_value          REAL,
    rating               REAL,
    review_count         INTEGER DEFAULT 0,
    listing_date         TEXT,
    listing_age_days     INTEGER,
    bsr_main_category    TEXT,
    bsr_main_rank        INTEGER,
    bsr_sub              TEXT,
    bsr_sub_rank         INTEGER,
    bsr_sub_category     TEXT,
    image_url            TEXT,
    product_url          TEXT,
    node_id              TEXT,
    category_name        TEXT,
    category_depth       INTEGER,
    site                 TEXT DEFAULT 'US',
    item_weight          TEXT,
    item_dimensions      TEXT,
    weight_lb            REAL,
    dim_l_in             REAL,
    dim_w_in             REAL,
    dim_h_in             REAL,
    variant_option_count INTEGER,
    other_sellers_count  INTEGER,
    fba_fee              REAL,
    placement_fee        REAL,
    fulfillment_type     TEXT,
    country_of_origin    TEXT,
    is_amazon_choice     INTEGER DEFAULT 0,
    is_bestseller        INTEGER DEFAULT 0,
    scraped_at           TIMESTAMPTZ DEFAULT now()
)
"""


_NA_RETENTION_DAYS = 30
_LAST_PURGE_CHECK = 0.0


def _purge_stale_new_arrivals(days: int = _NA_RETENTION_DAYS) -> int:
    """清理 new_arrivals 中抓取时间超过 days 天的旧记录，避免"最新到货"堆积陈旧数据。"""
    if DB_BACKEND == "pg":
        conn = _get_pg()
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM new_arrivals WHERE scraped_at < now() - (%s || ' days')::interval",
            (str(int(days)),),
        )
        deleted = cur.rowcount
        conn.commit()
        return deleted
    conn = sqlite3.connect(DB_FILE, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.execute(
        "DELETE FROM new_arrivals WHERE scraped_at < datetime('now', ?)",
        (f"-{int(days)} days",),
    )
    deleted = cur.rowcount
    conn.commit()
    conn.close()
    return deleted


def _maybe_purge_stale_new_arrivals():
    """节流：每个进程内最多每小时检查一次，避免高频轮询接口反复触发 DELETE。"""
    global _LAST_PURGE_CHECK
    now = time.time()
    if now - _LAST_PURGE_CHECK < 3600:
        return
    _LAST_PURGE_CHECK = now
    try:
        deleted = _purge_stale_new_arrivals()
        if deleted:
            _log.info(f"[清理] new_arrivals 超过 {_NA_RETENTION_DAYS} 天的旧数据已清理 {deleted} 条")
    except Exception as e:
        _log.warning(f"[清理] new_arrivals 旧数据清理失败: {e}")


def _init_db():
    if DB_BACKEND == "pg":
        conn = _get_pg()
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_PG_SQL)
        cur.execute("""
            DO $$ BEGIN
                ALTER TABLE new_arrivals
                    ADD CONSTRAINT uq_na_asin_node_site
                    UNIQUE NULLS NOT DISTINCT (asin, node_id, site);
            EXCEPTION WHEN duplicate_object THEN NULL;
            END $$;
        """)
        _maybe_purge_stale_new_arrivals()
        return
    conn = sqlite3.connect(DB_FILE, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(CREATE_TABLE_SQL)
    existing = {r[1] for r in conn.execute("PRAGMA table_info(new_arrivals)").fetchall()}
    for col, typ in _NA_EXTRA_COLS:
        if col not in existing:
            conn.execute(f"ALTER TABLE new_arrivals ADD COLUMN {col} {typ}")
    conn.commit()
    conn.close()
    _maybe_purge_stale_new_arrivals()


def _product_row_tuple(p: dict) -> tuple:
    return (
        p["asin"], p.get("title"), p.get("price"),
        p.get("price_value"), p.get("rating"),
        p.get("review_count", 0),
        p.get("listing_date"), p.get("listing_age_days"),
        p.get("bsr_main_category"), p.get("bsr_main_rank"),
        p.get("bsr_sub"),
        p.get("bsr_sub_rank"), p.get("bsr_sub_category"),
        p.get("image_url"), p.get("product_url"),
        p["node_id"], p.get("category_name"),
        p.get("category_depth"), p["site"],
        p.get("item_weight"), p.get("item_dimensions"),
        p.get("weight_lb"), p.get("dim_l_in"),
        p.get("dim_w_in"), p.get("dim_h_in"),
        p.get("variant_option_count"), p.get("other_sellers_count"),
        p.get("fba_fee"), p.get("placement_fee"),
        p.get("fulfillment_type"), p.get("country_of_origin"),
        1 if p.get("is_amazon_choice") else 0,
        1 if p.get("is_bestseller") else 0,
    )


def _save_products_sqlite(products: list) -> int:
    sql = """
        INSERT OR IGNORE INTO new_arrivals
        (asin, title, price, price_value, rating, review_count,
         listing_date, listing_age_days,
         bsr_main_category, bsr_main_rank, bsr_sub, bsr_sub_rank, bsr_sub_category,
         image_url, product_url,
         node_id, category_name, category_depth, site,
         item_weight, item_dimensions, weight_lb, dim_l_in, dim_w_in, dim_h_in,
         variant_option_count, other_sellers_count,
         fba_fee, placement_fee, fulfillment_type, country_of_origin,
         is_amazon_choice, is_bestseller)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    saved = 0
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=15)
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            for p in products:
                cur = conn.execute(sql, _product_row_tuple(p))
                if cur.rowcount:
                    saved += 1
            conn.commit()
        finally:
            conn.close()
    return saved


def _save_products_pg(products: list) -> int:
    sql = """
        INSERT INTO new_arrivals
        (asin, title, price, price_value, rating, review_count,
         listing_date, listing_age_days,
         bsr_main_category, bsr_main_rank, bsr_sub, bsr_sub_rank, bsr_sub_category,
         image_url, product_url,
         node_id, category_name, category_depth, site,
         item_weight, item_dimensions, weight_lb, dim_l_in, dim_w_in, dim_h_in,
         variant_option_count, other_sellers_count,
         fba_fee, placement_fee, fulfillment_type, country_of_origin,
         is_amazon_choice, is_bestseller)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT ON CONSTRAINT uq_na_asin_node_site DO NOTHING
    """
    saved = 0
    with _db_lock:
        conn = _get_pg()
        cur = conn.cursor()
        for p in products:
            try:
                cur.execute(sql, _product_row_tuple(p))
                if cur.rowcount:
                    saved += 1
            except Exception as e:
                _log.debug(f"PG insert skip {p.get('asin')}: {e}")
    return saved


def _save_products(products: list) -> int:
    if not products:
        return 0
    if DB_BACKEND == "pg":
        return _save_products_pg(products)
    return _save_products_sqlite(products)


def _load_nodes(site: str, depths: list[int] | None = None,
                root_ids: list[str] | None = None,
                include_descendants: bool = True) -> list[dict]:
    if DB_BACKEND == "pg":
        return _load_nodes_pg(site, depths, root_ids, include_descendants=include_descendants)
    conn = sqlite3.connect(DB_FILE, timeout=15)
    conn.row_factory = sqlite3.Row
    if root_ids:
        ph = ",".join("?" * len(root_ids))
        if include_descendants:
            rows = conn.execute(
                f"""WITH RECURSIVE sub AS (
                        SELECT node_id, name, depth FROM categories
                        WHERE node_id IN ({ph}) AND site = ?
                        UNION ALL
                        SELECT c.node_id, c.name, c.depth FROM categories c
                        JOIN sub s ON c.parent_node_id = s.node_id WHERE c.site = ?
                    ) SELECT node_id, name, depth FROM sub""",
                (*root_ids, site, site)
            ).fetchall()
        else:
            rows = conn.execute(
                f"""SELECT node_id, name, depth FROM categories
                    WHERE node_id IN ({ph}) AND site = ?
                    ORDER BY depth, name""",
                (*root_ids, site),
            ).fetchall()
    else:
        rows = conn.execute(
            "SELECT node_id, name, depth FROM categories WHERE site = ? AND depth > 0 ORDER BY depth, name",
            (site,)
        ).fetchall()
    conn.close()
    result = [{"node_id": r["node_id"], "name": r["name"], "depth": r["depth"]} for r in rows]
    if depths:
        result = [n for n in result if n["depth"] in depths]
    return _dedupe_nodes(result)


def _dedupe_nodes(nodes: list[dict]) -> list[dict]:
    """Preserve traversal order while preventing overlapping roots from double-fetching."""
    seen = set()
    unique = []
    for node in nodes:
        node_id = node["node_id"]
        if node_id in seen:
            continue
        seen.add(node_id)
        unique.append(node)
    return unique


def _load_nodes_pg(site: str, depths: list[int] | None = None,
                   root_ids: list[str] | None = None,
                   include_descendants: bool = True) -> list[dict]:
    if root_ids:
        ph = ",".join(["%s"] * len(root_ids))
        if include_descendants:
            rows = _pg_fetchall(
                f"""WITH RECURSIVE sub AS (
                        SELECT node_id, name, depth FROM categories
                        WHERE node_id IN ({ph}) AND site = %s
                        UNION ALL
                        SELECT c.node_id, c.name, c.depth FROM categories c
                        JOIN sub s ON c.parent_node_id = s.node_id WHERE c.site = %s
                    ) SELECT node_id, name, depth FROM sub""",
                (*root_ids, site, site),
            )
        else:
            rows = _pg_fetchall(
                f"""SELECT node_id, name, depth FROM categories
                    WHERE node_id IN ({ph}) AND site = %s
                    ORDER BY depth, name""",
                (*root_ids, site),
            )
    else:
        rows = _pg_fetchall(
            "SELECT node_id, name, depth FROM categories WHERE site = %s AND depth > 0 ORDER BY depth, name",
            (site,),
        )
    result = [{"node_id": r["node_id"], "name": r["name"], "depth": r["depth"]} for r in rows]
    if depths:
        result = [n for n in result if n["depth"] in depths]
    return _dedupe_nodes(result)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTTP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _make_session(worker_id: int, proxy_entry: dict | None) -> requests.Session:
    ua = USER_AGENTS[worker_id % len(USER_AGENTS)]
    hdrs = {
        **HEADERS,
        "User-Agent": ua,
        "Accept-Language": _LANG,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }
    session = make_forced_session(proxy_entry, headers=hdrs, required=True)
    assert_session_has_proxy(session, required=True)
    return session


def _safe_get(session: requests.Session, url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=18)
            if r.status_code == 200:
                if "captcha" in r.text.lower() or "Type the characters" in r.text:
                    _log.info("    [CAPTCHA] 等待 30s 后重试")
                    time.sleep(30 + random.uniform(0, 15))
                    continue
                return r.text
            if r.status_code == 429:
                wait = 60 + random.uniform(0, 30)
                _log.info(f"    [429] 限速 {wait:.0f}s")
                time.sleep(wait)
            elif r.status_code == 503:
                time.sleep(15 + random.uniform(0, 10))
            else:
                return None
        except RequestsError:
            wait = 5 * (2 ** attempt) + random.uniform(0, 3)
            time.sleep(wait)
    return None


def _warmup(session: requests.Session):
    assert_session_has_proxy(session, required=True)
    proxy = (getattr(session, "proxies", None) or {}).get("https") or ""
    try:
        session.get(f"{_DOMAIN}/", timeout=10)
        _log.info(f"[session] warmup ok proxy={proxy}")
        time.sleep(1 + random.uniform(0, 1))
    except Exception as e:
        _log.warning(f"[session] warmup 失败 proxy={proxy}: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Phase 1: 搜索列表页解析（价格/评分/评论在此筛选）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_search_url(node_id: str, page: int = 1) -> str:
    url = f"{_DOMAIN}/s?rh=n%3A{node_id}&s=date-desc-rank"
    if page > 1:
        url += f"&page={page}"
    return url


def _parse_price_value(raw: str) -> float | None:
    if not raw:
        return None
    m = re.search(r"[\d,.]+", raw)
    if not m:
        return None
    price_str = m.group()
    if _DECIMAL_SEP == ",":
        price_str = price_str.replace(".", "").replace(",", ".")
    else:
        price_str = price_str.replace(",", "")
    try:
        return float(price_str)
    except ValueError:
        return None


def _parse_card_fields(card) -> dict | None:
    """从搜索结果卡片提取 ASIN + 列表级字段。"""
    asin = (card.get("data-asin") or "").strip()
    if not asin:
        return None

    title = ""
    title_el = (
        card.select_one("h2 a span")
        or card.select_one("h2 span")
        or card.select_one(".a-text-normal")
    )
    if title_el:
        title = title_el.get_text(strip=True)

    price_raw = ""
    price_value = None
    price_el = card.select_one(".a-price .a-offscreen")
    if price_el:
        price_raw = price_el.get_text(strip=True)
        price_value = _parse_price_value(price_raw)

    rating = None
    rating_el = card.select_one(".a-icon-alt")
    if rating_el:
        rt = rating_el.get_text(strip=True)
        m_rt = re.search(_RATING_PAT, rt) or re.search(r"([\d,\.]+)", rt)
        if m_rt:
            try:
                rating = float(m_rt.group(1).replace(",", "."))
            except ValueError:
                pass

    review_count = None
    review_link = card.select_one('a[href*="customerReviews"], a[href*="#customerReviews"], a[href*="#reviews"]')
    if review_link:
        m = re.search(r"([\d,.]+)", review_link.get_text(strip=True))
        if m:
            try:
                review_count = int(m.group(1).replace(",", "").replace(".", ""))
            except ValueError:
                pass
    if review_count is None:
        review_el = card.select_one(".a-size-base.s-underline-text")
        if review_el:
            m = re.search(r"([\d,.]+)", review_el.get_text(strip=True))
            if m:
                try:
                    review_count = int(m.group(1).replace(",", "").replace(".", ""))
                except ValueError:
                    pass

    img = card.select_one("img.s-image, img")
    image_url = extract_image_url(img)

    return {
        "asin": asin,
        "title": title,
        "price": price_raw,
        "price_value": price_value,
        "rating": rating,
        "review_count": review_count,
        "image_url": image_url,
    }


def _pass_list_filters(item: dict, filters: dict) -> bool:
    """列表级筛选：价格 / 评分 / 评论数。缺失字段在设置了对应筛选时严格不通过。"""
    if not filters:
        return True

    price_min = filters.get("price_min") or 0
    price_max = filters.get("price_max") or 0
    if price_min or price_max:
        pv = item.get("price_value")
        if pv is None:
            return False
        if price_min and pv < price_min:
            return False
        if price_max and pv > price_max:
            return False

    rating_min = filters.get("rating_min") or 0
    rating_max = filters.get("rating_max") or 0
    if rating_min or rating_max:
        rt = item.get("rating")
        if rt is None:
            return False
        if rating_min and rt < rating_min:
            return False
        if rating_max and rt > rating_max:
            return False

    review_min = filters.get("review_min") or 0
    review_max = filters.get("review_max") or 0
    if review_min or review_max:
        rc = item.get("review_count")
        if rc is None:
            return False
        if review_min and rc < review_min:
            return False
        if review_max and rc > review_max:
            return False

    return True


def _parse_listing_page(html: str, filters: dict | None = None) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    filters = filters or {}

    if "captcha" in html.lower() or "Klicke auf die Schaltfläche" in html:
        return {"status": "captcha", "items": [], "has_next": False}

    cards = soup.select('[data-component-type="s-search-result"]')
    items = []
    for card in cards:
        item = _parse_card_fields(card)
        if not item:
            continue
        if not _pass_list_filters(item, filters):
            continue
        items.append(item)

    has_next = bool(soup.select_one(".s-pagination-next:not(.s-pagination-disabled)"))
    return {"status": "ok", "items": items, "has_next": has_next}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Phase 2: 详情页（复用 detail_parser）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_product_from_detail(html: str, asin: str, list_meta: dict) -> dict | None:
    """详情解析 + 详情级筛选。不通过返回 None（不入库）。"""
    detail = parse_detail_fields(html, _SITE)
    if not detail:
        return None

    # 列表价用于 Low-Price FBA 判定
    if list_meta.get("price_value") is not None and detail.get("price") is None:
        detail["price"] = list_meta["price_value"]
    attach_normalized_dims(detail)
    fees = estimate_fba_fees(
        _SITE,
        detail.get("item_weight"),
        detail.get("item_dimensions"),
        detail.get("price") if detail.get("price") is not None else list_meta.get("price_value"),
    )
    if fees.get("fba_fee") is not None:
        detail["fba_fee"] = fees["fba_fee"]
    if fees.get("placement_fee") is not None:
        detail["placement_fee"] = fees["placement_fee"]

    if _DETAIL_FILTERS and not check_detail_filters(detail, _DETAIL_FILTERS):
        return None

    listing_date = detail.get("date_first_available")
    listing_age_days = None
    if listing_date:
        try:
            listing_age_days = (datetime.now() - datetime.strptime(listing_date, "%Y-%m-%d")).days
        except ValueError:
            pass

    title_el_soup = BeautifulSoup(html, "html.parser")
    title_el = title_el_soup.select_one("#productTitle")
    title = title_el.get_text(strip=True) if title_el else (list_meta.get("title") or "")

    # 详情页价格优先，否则用列表页
    price = list_meta.get("price") or ""
    price_value = list_meta.get("price_value")
    price_el = title_el_soup.select_one("#corePrice_feature_div .a-offscreen, .a-price .a-offscreen")
    if price_el:
        price = price_el.get_text(strip=True) or price
        pv = _parse_price_value(price)
        if pv is not None:
            price_value = pv

    rating = detail.get("rating")
    if rating is None:
        rating = list_meta.get("rating")
        rating_el = title_el_soup.select_one("#acrPopover .a-icon-alt")
        if rating_el:
            m = re.search(r"([\d,\.]+)", rating_el.get_text())
            if m:
                try:
                    rating = float(m.group(1).replace(",", "."))
                except ValueError:
                    pass

    review_count = list_meta.get("review_count")
    review_el = title_el_soup.select_one("#acrCustomerReviewText")
    if review_el:
        m = re.search(r"([\d,.]+)", review_el.get_text())
        if m:
            try:
                review_count = int(m.group(1).replace(",", "").replace(".", ""))
            except ValueError:
                pass
    if review_count is None:
        review_count = 0

    image_url = list_meta.get("image_url") or ""
    img_el = title_el_soup.select_one("#landingImage, #imgBlkFront, #main-image")
    if img_el:
        image_url = (
            img_el.get("data-old-hires")
            or extract_image_url(img_el)
            or image_url
        )

    return {
        "asin": asin,
        "title": title,
        "price": price,
        "price_value": price_value,
        "rating": rating,
        "review_count": review_count,
        "listing_date": listing_date,
        "listing_age_days": listing_age_days,
        "bsr_main_category": detail.get("bsr_main_category"),
        "bsr_main_rank": detail.get("bsr_main_rank"),
        "bsr_sub": None,
        "bsr_sub_rank": detail.get("bsr_sub_rank"),
        "bsr_sub_category": detail.get("bsr_sub_category"),
        "image_url": image_url,
        "product_url": f"{_DOMAIN}/dp/{asin}",
        "item_weight": detail.get("item_weight"),
        "item_dimensions": detail.get("item_dimensions"),
        "weight_lb": detail.get("weight_lb"),
        "dim_l_in": detail.get("dim_l_in"),
        "dim_w_in": detail.get("dim_w_in"),
        "dim_h_in": detail.get("dim_h_in"),
        "variant_option_count": detail.get("variant_option_count"),
        "other_sellers_count": detail.get("other_sellers_count"),
        "fba_fee": detail.get("fba_fee"),
        "placement_fee": detail.get("placement_fee"),
        "fulfillment_type": detail.get("fulfillment_type"),
        "country_of_origin": detail.get("country_of_origin"),
        "is_amazon_choice": detail.get("is_amazon_choice", 0),
        "is_bestseller": detail.get("is_bestseller", 0),
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Worker
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

lock = threading.Lock()
stats = {
    "nodes_done": 0, "nodes_total": 0,
    "asins_found": 0, "asins_unique": 0, "list_filtered": 0,
    "details_ok": 0, "details_filtered": 0, "details_error": 0,
    "saved": 0,
    "captcha": 0, "p1_error": 0,
}
all_asins = {}  # asin -> list meta + node info


def _worker_phase1(worker_id: int, task_q: Queue, pool: ProxyPool, max_pages: int):
    proxy_entry = pool.acquire()
    session = _make_session(worker_id, proxy_entry)
    _warmup(session)

    while True:
        try:
            node = task_q.get(timeout=3)
        except Empty:
            break

        node_id = node["node_id"]
        node_items = []

        for page in range(1, max_pages + 1):
            url = _build_search_url(node_id, page)
            html = _safe_get(session, url)
            time.sleep(random.uniform(1.5, 3.0))

            if html is None:
                with lock:
                    stats["p1_error"] += 1
                break

            parsed = _parse_listing_page(html, _LIST_FILTERS)
            if parsed["status"] == "captcha":
                with lock:
                    stats["captcha"] += 1
                break

            node_items.extend(parsed["items"])
            if not parsed["has_next"]:
                break

        with lock:
            stats["nodes_done"] += 1
            stats["asins_found"] += len(node_items)
            for item in node_items:
                asin = item["asin"]
                if asin not in all_asins:
                    all_asins[asin] = {
                        **item,
                        "node_id": node_id,
                        "name": node["name"],
                        "depth": node["depth"],
                    }
            stats["asins_unique"] = len(all_asins)
            if stats["nodes_done"] % 50 == 0:
                _print_p1_progress()

    if proxy_entry:
        pool.release(proxy_entry)


def _worker_phase2(worker_id: int, task_q: Queue, pool: ProxyPool):
    proxy_entry = pool.acquire()
    session = _make_session(worker_id, proxy_entry)
    _warmup(session)
    session.headers["Referer"] = f"{_DOMAIN}/s?k=new"

    batch = []
    while True:
        try:
            item = task_q.get(timeout=3)
        except Empty:
            break

        asin = item["asin"]
        url = f"{_DOMAIN}/dp/{asin}"
        session.headers["Referer"] = _build_search_url(item["node_id"])
        html = _safe_get(session, url)
        time.sleep(random.uniform(3.0, 6.0))

        if html is None:
            with lock:
                stats["details_error"] += 1
            continue

        try:
            product = _build_product_from_detail(html, asin, item)
        except Exception as e:
            _log.error(f"  [detail] {asin} 解析异常: {e}\n{traceback.format_exc()}")
            with lock:
                stats["details_error"] += 1
            continue

        with lock:
            if product is None:
                stats["details_filtered"] += 1
            else:
                stats["details_ok"] += 1
                product["node_id"] = item["node_id"]
                product["category_name"] = item["name"]
                product["category_depth"] = item["depth"]
                product["site"] = _SITE
                batch.append(product)

                if len(batch) >= 20:
                    saved = _save_products(batch)
                    stats["saved"] += saved
                    batch.clear()

            done = stats["details_ok"] + stats["details_filtered"] + stats["details_error"]
            if done % 50 == 0:
                _print_p2_progress()

    if batch:
        with lock:
            saved = _save_products(batch)
            stats["saved"] += saved

    if proxy_entry:
        pool.release(proxy_entry)


def _print_p1_progress():
    n = stats["nodes_done"]
    t = stats["nodes_total"]
    _log.info(
        f"  [P1 {n}/{t}] ASIN总={stats['asins_found']} "
        f"去重={stats['asins_unique']} captcha={stats['captcha']} "
        f"err={stats['p1_error']}"
    )


def _print_p2_progress():
    done = stats["details_ok"] + stats["details_filtered"] + stats["details_error"]
    total = stats["asins_unique"]
    _log.info(
        f"  [P2 {done}/{total}] 命中={stats['details_ok']} "
        f"过滤={stats['details_filtered']} "
        f"入库={stats['saved']} err={stats['details_error']}"
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_filters_from_args(args) -> tuple[dict, dict]:
    """拆成列表级筛选（价格/评分/评论）与详情级筛选。"""
    list_filters = {
        "price_min": args.price_min, "price_max": args.price_max,
        "rating_min": args.rating_min, "rating_max": args.rating_max,
        "review_min": args.review_min, "review_max": args.review_max,
    }
    list_filters = {k: v for k, v in list_filters.items() if v}

    detail_filters = {
        "bsr_main_min": args.bsr_main_min, "bsr_main_max": args.bsr_main_max,
        "bsr_sub_min": args.bsr_sub_min, "bsr_sub_max": args.bsr_sub_max,
        "variant_min": args.variant_min, "variant_max": args.variant_max,
        "sellers_min": args.sellers_min, "sellers_max": args.sellers_max,
        "weight_min": args.weight_min, "weight_max": args.weight_max,
        "dim_l": args.dim_l, "dim_w": args.dim_w, "dim_h": args.dim_h,
        "fba_fee_min": args.fba_fee_min, "fba_fee_max": args.fba_fee_max,
        "fulfillment_type": args.fulfillment_type,
        "country": args.country,
        "amazons_choice": args.amazons_choice,
        "bestseller": args.bestseller,
        "date_range": args.date_range,
        "date_from": args.date_from, "date_to": args.date_to,
    }
    detail_filters = {k: v for k, v in detail_filters.items() if v}
    return list_filters, detail_filters


def main():
    global _mp, _SITE, _DOMAIN, _LANG, _DECIMAL_SEP, _RATING_PAT
    global _LIST_FILTERS, _DETAIL_FILTERS

    parser = argparse.ArgumentParser(description="Amazon 最新到货商品抓取")
    parser.add_argument("--site", default="DE", help="站点代码: US, DE, JP, UK, FR")
    parser.add_argument("--roots", nargs="+", help="根节点 node_id（默认全部类目）")
    parser.add_argument("--depth", nargs="+", type=int, help="只跑指定层级")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES_PER_NODE,
                        help=f"每类目最大翻页数（默认{MAX_PAGES_PER_NODE}）")
    parser.add_argument("--price-min", type=float, default=0)
    parser.add_argument("--price-max", type=float, default=0)
    parser.add_argument("--rating-min", type=float, default=0)
    parser.add_argument("--rating-max", type=float, default=0)
    parser.add_argument("--review-min", type=int, default=0)
    parser.add_argument("--review-max", type=int, default=0)
    parser.add_argument("--bsr-main-min", type=int, default=0)
    parser.add_argument("--bsr-main-max", type=int, default=0)
    parser.add_argument("--bsr-sub-min", type=int, default=0)
    parser.add_argument("--bsr-sub-max", type=int, default=0)
    parser.add_argument("--variant-min", type=int, default=0)
    parser.add_argument("--variant-max", type=int, default=0)
    parser.add_argument("--sellers-min", type=int, default=0)
    parser.add_argument("--sellers-max", type=int, default=0)
    parser.add_argument("--weight-min", type=float, default=0)
    parser.add_argument("--weight-max", type=float, default=0)
    parser.add_argument("--dim-l", type=float, default=0)
    parser.add_argument("--dim-w", type=float, default=0)
    parser.add_argument("--dim-h", type=float, default=0)
    parser.add_argument("--fba-fee-min", type=float, default=0)
    parser.add_argument("--fba-fee-max", type=float, default=0)
    parser.add_argument("--fulfillment-type", default="")
    parser.add_argument("--country", default="")
    parser.add_argument("--date-range", default="")
    parser.add_argument("--date-from", default="")
    parser.add_argument("--date-to", default="")
    parser.add_argument("--amazons-choice", action="store_true")
    parser.add_argument("--bestseller", action="store_true")
    parser.add_argument("--phase1-only", action="store_true", help="仅跑P1收集ASIN")
    parser.add_argument("--sample", type=int, default=0, help="只测试N个节点")
    parser.add_argument(
        "--exact-roots", action="store_true",
        help="仅抓 --roots 所选类目本身，不展开全部下级（默认会展开）",
    )
    args = parser.parse_args()

    _SITE = args.site.upper()
    _mp = get_marketplace(_SITE)
    _DOMAIN = _mp["domain"]
    _LANG = _mp["lang"]
    _DECIMAL_SEP = _mp.get("decimal_sep", ".")
    _RATING_PAT = _mp.get("rating_pattern", r"([\d.]+)\s+out")

    _LIST_FILTERS, _DETAIL_FILTERS = _build_filters_from_args(args)

    _init_db()
    try:
        pool = ProxyPool()
    except ProxyRequiredError as e:
        _log.error(f"[proxy] 强制代理失败: {e.code} {e}")
        raise SystemExit(2) from e
    num_workers = pool.size
    if num_workers < 1:
        _log.error("[proxy] 代理池为空，拒绝启动")
        raise SystemExit(2)

    nodes = _load_nodes(
        _SITE,
        depths=args.depth,
        root_ids=args.roots,
        include_descendants=not args.exact_roots,
    )
    _log.info(
        f"类目范围: {'仅抓所选' if args.exact_roots else '所选及全部下级'} "
        f"(roots={len(args.roots or [])} → nodes={len(nodes)})"
    )
    if args.sample > 0:
        random.shuffle(nodes)
        nodes = nodes[:args.sample]
    stats["nodes_total"] = len(nodes)

    _log.info(f"=== 最新到货抓取 [{_mp['name']}] ===")
    _log.info(f"DB_BACKEND: {DB_BACKEND}")
    _log.info(f"节点数: {len(nodes)}, Worker数: {num_workers}, 最大翻页: {args.max_pages}")
    _log.info(f"列表筛选: {_LIST_FILTERS or '(无)'}")
    _log.info(f"详情筛选: {_DETAIL_FILTERS or '(无)'}")
    _log.info("")

    # ── Phase 1 ──
    _log.info("━━━ Phase 1: 搜索列表页收集 ASIN（含价格/评分/评论筛选） ━━━")
    t0 = time.time()
    task_q = Queue()
    for n in nodes:
        task_q.put(n)

    with ThreadPoolExecutor(max_workers=num_workers) as exe:
        futs = [exe.submit(_worker_phase1, i, task_q, pool, args.max_pages)
                for i in range(num_workers)]
        for f in as_completed(futs):
            f.result()

    p1_time = time.time() - t0
    _log.info(
        f"\n[P1 完成] {p1_time:.0f}s — 节点={stats['nodes_done']}, "
        f"ASIN总={stats['asins_found']}, 去重={stats['asins_unique']}, "
        f"captcha={stats['captcha']}"
    )

    if args.phase1_only or not all_asins:
        _log.info("[结束] phase1-only 模式或无 ASIN")
        return

    # 测试/冒烟硬限制：AMZ_MAX_DETAILS>0 时截断详情队列（不改变筛选规则）
    try:
        _max_details = int(os.getenv("AMZ_MAX_DETAILS", "0") or "0")
    except ValueError:
        _max_details = 0
    if _max_details > 0 and len(all_asins) > _max_details:
        keep = dict(list(all_asins.items())[:_max_details])
        _log.info(
            f"[限制] AMZ_MAX_DETAILS={_max_details}，"
            f"详情 ASIN {len(all_asins)} → {len(keep)}"
        )
        all_asins.clear()
        all_asins.update(keep)

    # ── Phase 2 ──
    p2_workers = min(num_workers, max(num_workers // 2, 4))
    _log.info(f"\n━━━ Phase 2: {len(all_asins)} 个 ASIN 详情页解析 (worker={p2_workers}) ━━━")
    t1 = time.time()
    detail_q = Queue()
    for asin, info in all_asins.items():
        detail_q.put({"asin": asin, **info})

    with ThreadPoolExecutor(max_workers=p2_workers) as exe:
        futs = [exe.submit(_worker_phase2, i, detail_q, pool)
                for i in range(p2_workers)]
        for f in as_completed(futs):
            f.result()

    p2_time = time.time() - t1
    total_time = time.time() - t0
    _log.info(
        f"\n[P2 完成] {p2_time:.0f}s — 命中={stats['details_ok']}, "
        f"过滤={stats['details_filtered']}, 入库={stats['saved']}"
    )
    _log.info(f"\n=== 总计 {total_time:.0f}s ===")
    _log.info(f"  P1: {stats['nodes_done']}节点 → {stats['asins_unique']} ASIN")
    _log.info(
        f"  P2: {stats['details_ok']}命中 / {stats['details_filtered']}过滤 / "
        f"{stats['details_error']}失败"
    )
    _log.info(f"  入库: {stats['saved']} 条")

    _export_summary()


def _export_summary():
    if DB_BACKEND == "pg":
        total_rows = _pg_fetchall(
            "SELECT COUNT(*) AS cnt FROM new_arrivals WHERE site=%s", (_SITE,)
        )
        total = total_rows[0]["cnt"] if total_rows else 0
        top = _pg_fetchall(
            "SELECT asin, title, price, review_count, listing_age_days, bsr_main_rank, bsr_main_category "
            "FROM new_arrivals WHERE site=%s ORDER BY bsr_main_rank ASC NULLS LAST LIMIT 20",
            (_SITE,),
        )
    else:
        conn = sqlite3.connect(DB_FILE, timeout=15)
        conn.row_factory = sqlite3.Row
        total = conn.execute("SELECT COUNT(*) FROM new_arrivals WHERE site=?", (_SITE,)).fetchone()[0]
        top = [dict(r) for r in conn.execute(
            "SELECT asin, title, price, review_count, listing_age_days, bsr_main_rank, bsr_main_category "
            "FROM new_arrivals WHERE site=? ORDER BY bsr_main_rank ASC LIMIT 20",
            (_SITE,)
        ).fetchall()]
        conn.close()

    _log.info(f"\n=== DB 汇总 ({DB_BACKEND}): {total} 条记录 ===")
    if top:
        _log.info("\nTop 产品 (BSR 最优):")
        for r in top:
            age = f"{r['listing_age_days']}天" if r["listing_age_days"] is not None else "?"
            bsr = f"#{r['bsr_main_rank']}" if r["bsr_main_rank"] else "?"
            _log.info(
                f"  {r['asin']}  {bsr:>8}  {age:>4}  rev={r['review_count']:<4} "
                f"{r['price'] or '?':>10}  {(r['title'] or '')[:50]}"
            )


def export_excel(site: str = None, signal_only: bool = False) -> str:
    """导出 new_arrivals 到 Excel，返回文件路径。signal_only 已废弃，保留签名兼容。"""
    try:
        import openpyxl
    except ImportError:
        raise RuntimeError("需要 openpyxl: pip install openpyxl")

    sql = """
        SELECT asin, title, price, price_value, rating, review_count,
               listing_date, listing_age_days, bsr_main_category, bsr_main_rank,
               bsr_sub_rank, bsr_sub_category,
               image_url, product_url, node_id, category_name, category_depth,
               site, item_weight, item_dimensions, weight_lb,
               dim_l_in, dim_w_in, dim_h_in, fba_fee, placement_fee,
               fulfillment_type, country_of_origin, is_amazon_choice, is_bestseller,
               scraped_at
        FROM new_arrivals WHERE 1=1
    """
    params: list = []
    if site:
        if DB_BACKEND == "pg":
            sql += " AND site=%s"
        else:
            sql += " AND site=?"
        params.append(site.upper())
    sql += " ORDER BY bsr_main_rank ASC NULLS LAST, scraped_at DESC" if DB_BACKEND == "pg" else \
           " ORDER BY bsr_main_rank ASC, scraped_at DESC"

    if DB_BACKEND == "pg":
        rows = _pg_fetchall(sql, tuple(params))
        def excel_value(value):
            if isinstance(value, datetime) and value.tzinfo is not None:
                return value.replace(tzinfo=None)
            return value

        row_lists = [[excel_value(r.get(k)) for k in (
            "asin", "title", "price", "price_value", "rating", "review_count",
            "listing_date", "listing_age_days", "bsr_main_category", "bsr_main_rank",
            "bsr_sub_rank", "bsr_sub_category",
            "image_url", "product_url", "node_id", "category_name", "category_depth",
            "site", "item_weight", "item_dimensions", "weight_lb",
            "dim_l_in", "dim_w_in", "dim_h_in", "fba_fee", "placement_fee",
            "fulfillment_type", "country_of_origin", "is_amazon_choice", "is_bestseller",
            "scraped_at",
        )] for r in rows]
    else:
        conn = sqlite3.connect(DB_FILE, timeout=15)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
        conn.close()
        row_lists = [list(r) for r in rows]

    if not row_lists:
        raise RuntimeError("new_arrivals 无数据可导出")

    excel_path = os.path.join(DATA_DIR, "new_arrivals.xlsx")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "最新到货"
    headers = [
        "ASIN", "标题", "价格", "价格数值", "评分", "评论数",
        "上架日期", "上架天数", "BSR大类", "BSR大类排名",
        "BSR子类排名", "BSR子类",
        "图片URL", "商品URL", "节点ID", "类目名", "类目深度",
        "站点", "重量", "尺寸", "重量lb", "长in", "宽in", "高in",
        "FBA运费", "配置费", "配送方式", "产地", "Amazon精选", "畅销标记",
        "抓取时间",
    ]
    ws.append(headers)
    for r in row_lists:
        ws.append(r)
    wb.save(excel_path)
    _log.info(f"[fetch_new_arrivals] Excel 已导出: {excel_path} ({len(row_lists)} 行)")
    return excel_path


if __name__ == "__main__":
    main()
