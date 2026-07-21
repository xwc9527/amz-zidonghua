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

import json, os, re, sys, time, random, sqlite3, threading, argparse, logging, traceback, hashlib
from collections import Counter
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

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
_AUDIT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "fetch_new_arrivals_attempts.jsonl")
_RUN_ID = os.getenv("AMZ_RUN_ID") or datetime.now().strftime("NA-%Y%m%d-%H%M%S")

from curl_cffi import requests as requests
from bs4 import BeautifulSoup

from config import (
    HEADERS, DATA_DIR, DB_FILE, get_marketplace,
    PROXY_MAX_CRAWL_WORKERS,
    PROXY_MIN_START_NODES,
)
from crawl_autoscale import run_autoscaled_queue
from proxy_daemon import touch_crawl_activity
from proxy_session import (
    ForcedProxyPool,
    ProxyRequiredError,
    assert_session_has_proxy,
    make_forced_session,
)
from proxy_worker import (
    AttemptAuditor,
    FetchOutcome,
    WorkerProxyClient as SharedWorkerProxyClient,
    classify_request_exception as _classify_request_exception,
    has_us_currency_mismatch,
    is_captcha_page as _is_captcha_page,
    raise_if_pool_below_minimum as _raise_if_pool_below_minimum,
    pool_aware_delay,
)
from detail_parser import (
    parse_detail_fields, check_detail_filters, attach_normalized_dims,
    extract_image_url,
)
from fba_fees_us import estimate_fba_fees
from crawl_checkpoint import NewArrivalsCheckpoint, canonical_signature

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
    """兼容旧名称；强制代理，禁止静默直连。

    min_usable 用 PROXY_MIN_START_NODES（默认1）：常驻验证守护进程持续在
    后台增补节点，这里不再要求启动时就凑够一大批；enable_live_reload 让
    运行期间能持续感知守护进程新增/淘汰的节点。
    """

    def __init__(self):
        super().__init__(
            required=True, min_usable=PROXY_MIN_START_NODES, enable_live_reload=True,
        )
        _log.info(f"[pool] 强制加载 {self.size} 个代理端口（min_usable={self.min_usable}，活池热重载已开启）")


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
    social_proof    TEXT,
    social_proof_count INTEGER,
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
    ("social_proof", "TEXT"),
    ("social_proof_count", "INTEGER"),
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
    social_proof         TEXT,
    social_proof_count   INTEGER,
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
        cur.execute("ALTER TABLE new_arrivals ADD COLUMN IF NOT EXISTS social_proof TEXT")
        cur.execute("ALTER TABLE new_arrivals ADD COLUMN IF NOT EXISTS social_proof_count INTEGER")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_na_social_proof ON new_arrivals(social_proof_count)")
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
    conn.execute("CREATE INDEX IF NOT EXISTS idx_na_social_proof ON new_arrivals(social_proof_count)")
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
        p.get("social_proof"), p.get("social_proof_count"),
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
         social_proof, social_proof_count,
         fba_fee, placement_fee, fulfillment_type, country_of_origin,
         is_amazon_choice, is_bestseller)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
         social_proof, social_proof_count,
         fba_fee, placement_fee, fulfillment_type, country_of_origin,
         is_amazon_choice, is_bestseller)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
    """加载最新到货目标节点：仅 na_valid=1（NEW 标记）类目。"""
    if DB_BACKEND == "pg":
        return _load_nodes_pg(site, depths, root_ids, include_descendants=include_descendants)
    conn = sqlite3.connect(DB_FILE, timeout=15)
    conn.row_factory = sqlite3.Row
    if root_ids:
        ph = ",".join("?" * len(root_ids))
        if include_descendants:
            rows = conn.execute(
                f"""WITH RECURSIVE sub AS (
                        SELECT node_id, name, depth, na_valid FROM categories
                        WHERE node_id IN ({ph}) AND site = ?
                        UNION ALL
                        SELECT c.node_id, c.name, c.depth, c.na_valid FROM categories c
                        JOIN sub s ON c.parent_node_id = s.node_id WHERE c.site = ?
                    ) SELECT node_id, name, depth FROM sub
                    WHERE na_valid = 1
                    ORDER BY depth DESC, name""",
                (*root_ids, site, site)
            ).fetchall()
        else:
            rows = conn.execute(
                f"""SELECT node_id, name, depth FROM categories
                    WHERE node_id IN ({ph}) AND site = ? AND na_valid = 1
                    ORDER BY depth DESC, name""",
                (*root_ids, site),
            ).fetchall()
    else:
        # 未指定 roots = 全站全部 NEW 类目
        rows = conn.execute(
            "SELECT node_id, name, depth FROM categories WHERE site = ? AND na_valid = 1 "
            "ORDER BY depth DESC, name",
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
    """加载最新到货目标节点：仅 na_valid=1（NEW 标记）类目。"""
    if root_ids:
        ph = ",".join(["%s"] * len(root_ids))
        if include_descendants:
            rows = _pg_fetchall(
                f"""WITH RECURSIVE sub AS (
                        SELECT node_id, name, depth, na_valid FROM categories
                        WHERE node_id IN ({ph}) AND site = %s
                        UNION ALL
                        SELECT c.node_id, c.name, c.depth, c.na_valid FROM categories c
                        JOIN sub s ON c.parent_node_id = s.node_id WHERE c.site = %s
                    ) SELECT node_id, name, depth FROM sub
                    WHERE na_valid = 1
                    ORDER BY depth DESC, name""",
                (*root_ids, site, site),
            )
        else:
            rows = _pg_fetchall(
                f"""SELECT node_id, name, depth FROM categories
                    WHERE node_id IN ({ph}) AND site = %s AND na_valid = 1
                    ORDER BY depth DESC, name""",
                (*root_ids, site),
            )
    else:
        rows = _pg_fetchall(
            "SELECT node_id, name, depth FROM categories WHERE site = %s AND na_valid = 1 "
            "ORDER BY depth DESC, name",
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
    currency_code = _mp.get("currency_code")
    if currency_code:
        session.cookies.set("i18n-prefs", currency_code)
    assert_session_has_proxy(session, required=True)
    return session


def _has_marketplace_currency_mismatch(text: str) -> bool:
    """仅检查价格组件，避免代理地理位置把 US 价格本地化为 JPY/S$/CA$。"""
    if _SITE != "US":
        return False
    return has_us_currency_mismatch(text)


def _warmup(session: requests.Session):
    assert_session_has_proxy(session, required=True)
    proxy = (getattr(session, "proxies", None) or {}).get("https") or ""
    try:
        session.get(f"{_DOMAIN}/", timeout=10)
        _log.info(f"[session] warmup ok proxy={proxy}")
        time.sleep(1 + random.uniform(0, 1))
    except Exception as e:
        _log.warning(f"[session] warmup 失败 proxy={proxy}: {e}")


class WorkerProxyClient(SharedWorkerProxyClient):
    """最新到货 Worker：注入站点 Session / 币种错配检测。"""

    def __init__(self, pool: ProxyPool, worker_id: int, *, warmup: bool = True):
        super().__init__(
            pool,
            worker_id,
            make_session=_make_session,
            warmup=_warmup if warmup else None,
            # 每次新建，便于测试 patch _AUDIT_PATH
            auditor=AttemptAuditor(_AUDIT_PATH, _RUN_ID),
            is_captcha=_is_captcha_page,
            is_currency_mismatch=_has_marketplace_currency_mismatch,
        )


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
        "social_proof": detail.get("social_proof"),
        "social_proof_count": detail.get("social_proof_count"),
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
    "errors_by_reason": Counter(), "attempt_failures": Counter(),
    "pool_usable": 0, "pool_cooling": 0, "pool_disabled": 0,
}
all_asins = {}  # asin -> list meta + node info
checkpoint: NewArrivalsCheckpoint | None = None


def _update_pool_stats(pool: ProxyPool):
    snapshot = pool.health_snapshot()
    stats["pool_usable"] = snapshot["usable"]
    stats["pool_cooling"] = snapshot["cooling"]
    stats["pool_disabled"] = snapshot["disabled"]


def _record_fetch_failure(outcome: FetchOutcome):
    stats["errors_by_reason"][outcome.final_reason or outcome.error_code] += 1


def _record_attempt_reasons(outcome: FetchOutcome):
    for reason in outcome.reasons:
        stats["attempt_failures"][reason] += 1
        if reason == "CAPTCHA":
            stats["captcha"] += 1


def _worker_phase1(worker_id: int, task_q: Queue, pool: ProxyPool, max_pages: int):
    client = WorkerProxyClient(pool, worker_id, warmup=False)
    try:
        while True:
            try:
                node = task_q.get(timeout=3)
            except Empty:
                break

            node_id = node["node_id"]
            node_items = []
            node_error = ""
            node_attempts = 0

            for page in range(1, max_pages + 1):
                url = _build_search_url(node_id, page)
                outcome = client.get(url, phase="P1", item_id=f"{node_id}:p{page}")
                node_attempts += outcome.attempts
                with lock:
                    _record_attempt_reasons(outcome)
                time.sleep(pool_aware_delay(1.5, 3.0, pool.usable_count))

                if not outcome.ok:
                    _raise_if_pool_below_minimum(outcome)
                    node_error = outcome.final_reason or outcome.error_code
                    with lock:
                        stats["p1_error"] += 1
                        _record_fetch_failure(outcome)
                        _update_pool_stats(pool)
                    break

                parsed = _parse_listing_page(outcome.html or "", _LIST_FILTERS)
                if parsed["status"] == "captcha":
                    node_error = "CAPTCHA"
                    client._release("CAPTCHA")
                    with lock:
                        stats["p1_error"] += 1
                        stats["captcha"] += 1
                        stats["errors_by_reason"]["CAPTCHA"] += 1
                        stats["attempt_failures"]["CAPTCHA"] += 1
                        _update_pool_stats(pool)
                    break

                node_items.extend(parsed["items"])
                if not parsed["has_next"]:
                    break

            full_items = [{
                **item,
                "node_id": node_id,
                "name": node["name"],
                "depth": node["depth"],
            } for item in node_items]
            if checkpoint is not None:
                checkpoint.save_p1_node(
                    node_id, full_items, status="error" if node_error else "done",
                    error_code=node_error, attempts=node_attempts,
                )

            with lock:
                stats["nodes_done"] += 1
                stats["asins_found"] += len(full_items)
                for item in full_items:
                    if item["asin"] not in all_asins:
                        all_asins[item["asin"]] = item
                stats["asins_unique"] = len(all_asins)
                _update_pool_stats(pool)
                if stats["nodes_done"] % 50 == 0:
                    _print_p1_progress()
    finally:
        client.close()


def _worker_phase2(worker_id: int, task_q: Queue, pool: ProxyPool):
    client = WorkerProxyClient(pool, worker_id, warmup=False)
    try:
        while True:
            try:
                item = task_q.get(timeout=3)
            except Empty:
                break

            asin = item["asin"]
            url = f"{_DOMAIN}/dp/{asin}"
            outcome = client.get(
                url, phase="P2", item_id=asin,
                referer=_build_search_url(item["node_id"]),
            )
            with lock:
                _record_attempt_reasons(outcome)
            time.sleep(pool_aware_delay(3.0, 6.0, pool.usable_count))

            if not outcome.ok:
                _raise_if_pool_below_minimum(outcome)
                if checkpoint is not None:
                    checkpoint.save_p2_result(
                        asin, "error", error_code=outcome.error_code,
                        final_reason=outcome.final_reason, attempts=outcome.attempts,
                        exit_ips=outcome.exit_ips, reasons=outcome.reasons,
                    )
                with lock:
                    stats["details_error"] += 1
                    _record_fetch_failure(outcome)
                    _update_pool_stats(pool)
                continue

            try:
                product = _build_product_from_detail(outcome.html or "", asin, item)
            except Exception as exc:
                _log.error(f"[detail] asin={asin} reason=DETAIL_PARSE_ERROR error={exc}\n{traceback.format_exc()}")
                if checkpoint is not None:
                    checkpoint.save_p2_result(
                        asin, "error", error_code="DETAIL_PARSE_ERROR",
                        final_reason="DETAIL_PARSE_ERROR", attempts=outcome.attempts,
                        exit_ips=outcome.exit_ips, reasons=[*outcome.reasons, "DETAIL_PARSE_ERROR"],
                    )
                with lock:
                    stats["details_error"] += 1
                    stats["errors_by_reason"]["DETAIL_PARSE_ERROR"] += 1
                continue

            if product is None:
                if checkpoint is not None:
                    checkpoint.save_p2_result(
                        asin, "filtered", attempts=outcome.attempts,
                        exit_ips=outcome.exit_ips, reasons=outcome.reasons,
                    )
                with lock:
                    stats["details_filtered"] += 1
            else:
                product["node_id"] = item["node_id"]
                product["category_name"] = item["name"]
                product["category_depth"] = item["depth"]
                product["site"] = _SITE
                try:
                    saved = _save_products([product])
                except Exception as exc:
                    _log.error(f"[detail] asin={asin} reason=DB_SAVE_ERROR error={exc}")
                    if checkpoint is not None:
                        checkpoint.save_p2_result(
                            asin, "error", error_code="DB_SAVE_ERROR",
                            final_reason="DB_SAVE_ERROR", attempts=outcome.attempts,
                            exit_ips=outcome.exit_ips, reasons=[*outcome.reasons, "DB_SAVE_ERROR"],
                        )
                    with lock:
                        stats["details_error"] += 1
                        stats["errors_by_reason"]["DB_SAVE_ERROR"] += 1
                    continue
                if checkpoint is not None:
                    checkpoint.save_p2_result(
                        asin, "matched", attempts=outcome.attempts,
                        exit_ips=outcome.exit_ips, reasons=outcome.reasons,
                    )
                with lock:
                    stats["details_ok"] += 1
                    stats["saved"] += saved

            with lock:
                _update_pool_stats(pool)
                done = stats["details_ok"] + stats["details_filtered"] + stats["details_error"]
                if done % 50 == 0:
                    _print_p2_progress()
    finally:
        client.close()


def _print_p1_progress():
    n = stats["nodes_done"]
    t = stats["nodes_total"]
    _log.info(
        f"  [P1 {n}/{t}] ASIN总={stats['asins_found']} "
        f"去重={stats['asins_unique']} captcha={stats['captcha']} "
        f"err={stats['p1_error']} reasons={dict(stats['errors_by_reason'])} "
        f"pool={stats['pool_usable']}可用/{stats['pool_cooling']}冷却/{stats['pool_disabled']}禁用"
    )


def _print_p2_progress():
    done = stats["details_ok"] + stats["details_filtered"] + stats["details_error"]
    total = stats["asins_unique"]
    _log.info(
        f"  [P2 {done}/{total}] 命中={stats['details_ok']} "
        f"过滤={stats['details_filtered']} "
        f"入库={stats['saved']} err={stats['details_error']} "
        f"reasons={dict(stats['errors_by_reason'])} "
        f"pool={stats['pool_usable']}可用/{stats['pool_cooling']}冷却/{stats['pool_disabled']}禁用"
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
        "social_proof_min": args.social_proof_min,
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
    global _LIST_FILTERS, _DETAIL_FILTERS, checkpoint

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
    parser.add_argument("--social-proof-min", type=int, default=0)
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
    parser.add_argument("--no-resume", action="store_true", help="忽略同配置断点并重新开始")
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
    num_workers = max(1, min(pool.usable_count, PROXY_MAX_CRAWL_WORKERS))
    touch_crawl_activity(active=True, source="fetch_new_arrivals")
    if pool.usable_count < PROXY_MIN_START_NODES:
        _log.error(
            "[proxy] 启动拒绝：可用代理=%d，最低要求=%d",
            pool.usable_count, PROXY_MIN_START_NODES,
        )
        raise SystemExit(2)

    nodes = _load_nodes(
        _SITE,
        depths=args.depth,
        root_ids=args.roots,
        include_descendants=not args.exact_roots,
    )
    depth_hint = (
        f"L{nodes[0]['depth']}→L{nodes[-1]['depth']}" if nodes else "?"
    )
    _log.info(
        f"类目范围: {'仅抓所选' if args.exact_roots else '所选及全部下级'} "
        f"(roots={len(args.roots or [])} → nodes={len(nodes)}, "
        f"深度优先: {depth_hint})"
    )
    if args.sample > 0:
        sample_seed = canonical_signature({
            "site": _SITE,
            "roots": sorted(args.roots or []),
            "depth": sorted(args.depth or []),
            "exact_roots": bool(args.exact_roots),
            "sample": args.sample,
        })
        random.Random(sample_seed).shuffle(nodes)
        nodes = nodes[:args.sample]

    try:
        max_details = int(os.getenv("AMZ_MAX_DETAILS", "0") or "0")
    except ValueError:
        max_details = 0

    checkpoint_config = {
        "site": _SITE,
        "db_backend": DB_BACKEND,
        "db_target_sha256": hashlib.sha256(
            (
                os.path.abspath(DB_FILE)
                if DB_BACKEND != "pg"
                else __import__("pg_config").get_pg_dsn()
            ).encode("utf-8")
        ).hexdigest()[:20],
        "roots": sorted(args.roots or []),
        "depth": sorted(args.depth or []),
        "exact_roots": bool(args.exact_roots),
        "max_pages": args.max_pages,
        "max_details": max_details,
        "sample": args.sample,
        "node_count": len(nodes),
        "node_ids_sha256": hashlib.sha256(
            "\n".join(sorted(str(n["node_id"]) for n in nodes)).encode("utf-8")
        ).hexdigest(),
        "list_filters": _LIST_FILTERS,
        "detail_filters": _DETAIL_FILTERS,
    }
    signature = canonical_signature(checkpoint_config)
    checkpoint = NewArrivalsCheckpoint(
        signature, checkpoint_config, resume=not args.no_resume,
    )
    restored_asins = checkpoint.load_asins()
    all_asins.clear()
    all_asins.update(restored_asins)
    done_nodes = checkpoint.p1_done_ids()
    cp_summary = checkpoint.summary()
    stats.update({
        "nodes_done": len(done_nodes),
        "nodes_total": len(nodes),
        "asins_unique": len(all_asins),
        "details_ok": cp_summary["p2"].get("matched", 0),
        "details_filtered": cp_summary["p2"].get("filtered", 0),
        "details_error": cp_summary["p2"].get("error", 0),
        "saved": cp_summary["p2"].get("matched", 0),
    })
    _update_pool_stats(pool)

    _log.info(f"=== 最新到货抓取 [{_mp['name']}] ===")
    _log.info(f"DB_BACKEND: {DB_BACKEND}")
    _log.info(f"节点数: {len(nodes)}, Worker数: {num_workers}, 最大翻页: {args.max_pages}")
    _log.info(f"列表筛选: {_LIST_FILTERS or '(无)'}")
    _log.info(f"详情筛选: {_DETAIL_FILTERS or '(无)'}")
    _log.info(
        "运行编号: %s, 断点=%s, 恢复节点=%d, 恢复ASIN=%d",
        _RUN_ID, checkpoint.path, len(done_nodes), len(all_asins),
    )
    _log.info("")

    t0 = time.time()
    try:
        # ── Phase 1 ──
        checkpoint.set_phase("P1")
        remaining_nodes = [n for n in nodes if str(n["node_id"]) not in done_nodes]
        _log.info(
            "━━━ Phase 1: 搜索列表页收集 ASIN（待处理 %d / 总计 %d） ━━━",
            len(remaining_nodes), len(nodes),
        )
        p1_started = time.time()
        if remaining_nodes:
            task_q = Queue()
            for node in remaining_nodes:
                task_q.put(node)
            run_autoscaled_queue(
                _worker_phase1, task_q, pool,
                initial_workers=num_workers,
                max_workers=PROXY_MAX_CRAWL_WORKERS,
                worker_args=(args.max_pages,),
                log_prefix="P1",
            )

        all_asins.clear()
        all_asins.update(checkpoint.load_asins())
        stats["asins_unique"] = len(all_asins)
        final_p1_done = checkpoint.p1_done_ids()
        p1_time = time.time() - p1_started
        _log.info(
            "\n[P1 完成] %.0fs — 节点=%d/%d, 去重ASIN=%d, captcha=%d, 原因=%s",
            p1_time, stats["nodes_done"], stats["nodes_total"],
            stats["asins_unique"], stats["captcha"], dict(stats["errors_by_reason"]),
        )

        p1_pending = [
            str(node["node_id"]) for node in nodes
            if str(node["node_id"]) not in final_p1_done
        ]
        if p1_pending:
            checkpoint.set_phase("P1_RETRY_PENDING")
            _log.error(
                "[未完成] P1仍有%d个节点失败，断点已保留；下次只重试失败节点",
                len(p1_pending),
            )
            raise SystemExit(4)

        if args.phase1_only:
            checkpoint.set_phase("P1_COMPLETE")
            _log.info("[结束] phase1-only；P1断点已保留，可直接续跑P2")
            return
        if not all_asins:
            checkpoint.complete()
            _log.info("[结束] 未发现 ASIN")
            return

        if max_details > 0 and len(all_asins) > max_details:
            keep = dict(list(all_asins.items())[:max_details])
            _log.info(
                "[限制] AMZ_MAX_DETAILS=%d，详情 ASIN %d → %d",
                max_details, len(all_asins), len(keep),
            )
            all_asins.clear()
            all_asins.update(keep)

        # ── Phase 2 ──
        checkpoint.set_phase("P2")
        p2_done = checkpoint.p2_done_ids()
        remaining_asins = {asin: info for asin, info in all_asins.items() if asin not in p2_done}
        p2_workers = max(1, min(pool.usable_count, PROXY_MAX_CRAWL_WORKERS, max(num_workers, 4)))
        _log.info(
            "\n━━━ Phase 2: 待处理 %d / 总计 %d 个 ASIN (初始worker=%d, 可动态扩容) ━━━",
            len(remaining_asins), len(all_asins), p2_workers,
        )
        p2_started = time.time()
        if remaining_asins:
            detail_q = Queue()
            for asin, info in remaining_asins.items():
                detail_q.put({"asin": asin, **info})
            run_autoscaled_queue(
                _worker_phase2, detail_q, pool,
                initial_workers=p2_workers,
                max_workers=PROXY_MAX_CRAWL_WORKERS,
                log_prefix="P2",
            )

        p2_time = time.time() - p2_started
        final_cp = checkpoint.summary()
        p2_errors = final_cp["p2"].get("error", 0)
        if p2_errors:
            checkpoint.set_phase("P2_RETRY_PENDING")
            _log.error(
                "[未完成] P2仍有%d个ASIN失败，断点已保留；下次只重试失败ASIN",
                p2_errors,
            )
            raise SystemExit(4)
        checkpoint.complete()
        total_time = time.time() - t0
        _log.info(
            "\n[P2 完成] %.0fs — 命中=%d, 过滤=%d, 失败=%d, 入库=%d",
            p2_time, stats["details_ok"], stats["details_filtered"],
            stats["details_error"], stats["saved"],
        )
        _log.info(
            "[完成] run=%s 总耗时=%.0fs pool=%s 尝试失败=%s",
            _RUN_ID, total_time, pool.health_snapshot(), dict(stats["attempt_failures"]),
        )
        _export_summary()
    except ProxyRequiredError as exc:
        checkpoint.set_phase("PAUSED_PROXY")
        _log.critical(
            "[安全暂停] run=%s code=%s error=%s checkpoint=%s pool=%s",
            _RUN_ID, exc.code, exc, checkpoint.path, pool.health_snapshot(),
        )
        raise SystemExit(3) from exc
    finally:
        checkpoint.close()


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
               social_proof, social_proof_count,
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
            "social_proof", "social_proof_count",
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
        "FBA运费", "配置费", "社交证明原文", "月销量下限",
        "配送方式", "产地", "Amazon精选", "畅销标记",
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
