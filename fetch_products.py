"""
fetch_products.py — 商品抓取脚本（独立进程）
从 categories.db 读取有效节点，抓取 3 个 SSR 榜单的商品数据
用法: python fetch_products.py
"""
import sqlite3, threading, time, sys, os, re, json, argparse, logging, traceback, random
from curl_cffi import requests as requests
from curl_cffi.requests import RequestsError
from datetime import datetime
from queue import Queue, Empty
from bs4 import BeautifulSoup
from fba_fees_us import estimate_fba_fees
from detail_parser import (
    parse_detail_fields as _parse_detail_fields_shared,
    check_detail_filters as _check_detail_filters_shared,
    attach_normalized_dims,
    extract_image_url,
)

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── 日志 ──────────────────────────────────────────────────────────
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "fetch_products.log")
_log = logging.getLogger("fetch_products")
_log.setLevel(logging.DEBUG)
_fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_sh = logging.StreamHandler(sys.stdout)
_sh.setLevel(logging.INFO)
_sh.setFormatter(logging.Formatter("%(message)s"))
_log.addHandler(_fh)
_log.addHandler(_sh)

# ── 配置 ────────────────────────────────────────────────────────────
BASE    = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "data", "categories.db")
DB_BACKEND = os.getenv("DB_BACKEND", "pg")

from config import get_marketplace
_mp     = get_marketplace("US")
_SITE   = "US"
_DOMAIN = _mp["domain"]
_LANG   = _mp["lang"]
_CURRENCY     = _mp["currency"]
_DECIMAL_SEP  = _mp["decimal_sep"]
_RATING_PAT   = _mp["rating_pattern"]
_RESULTS_PAT  = _mp["results_pattern"]

# PG support
_pg_conn = None
def _get_pg():
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        import psycopg2
        from pg_config import get_pg_dsn
        _pg_conn = psycopg2.connect(get_pg_dsn())
        _pg_conn.autocommit = True
    return _pg_conn

# 默认值（可被看板 API 参数覆盖）
DEFAULT_REVIEW_MAX    = 10
DEFAULT_MIN_LIST_SIZE = 100
DEFAULT_PRICE_MIN     = 0.0
DEFAULT_PRICE_MAX     = 0.0
DEFAULT_DELAY         = 2.0   # 请求间隔（秒）
DEFAULT_LISTS         = ["new-releases", "bestsellers", "movers-and-shakers", "most-wished-for", "most-gifted"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

from proxy_session import (
    ProxyRequiredError,
    assert_session_has_proxy,
    load_proxy_pool,
    make_forced_session,
)

LIST_LABELS = {
    "new-releases":       "新品榜",
    "bestsellers":        "畅销榜",
    "movers-and-shakers": "飙升榜",
    "most-wished-for":    "心愿单",
    "most-gifted":        "礼品榜",
}

# 榜单名 → categories 表列名（用于写入验证结果）
LIST_COL_MAP = {
    "new-releases":       "nr_valid",
    "bestsellers":        "bs_valid",
    "movers-and-shakers": "ms_valid",
    "most-wished-for":    "mw_valid",
}

_db_lock = threading.Lock()
_stats = {"total_nodes": 0, "done_nodes": 0, "skipped": 0,
          "products_found": 0, "products_saved": 0, "products_dup": 0, "errors": 0}
_stats_lock = threading.Lock()
_seen_asins = set()
_seen_lock = threading.Lock()

# ── 代理池（强制代理，禁止静默直连）────────────────────────────────

_proxy_pool: list[dict] = []
_proxy_idx = 0

def _validate_proxy_pool():
    """强制加载已发布代理池；空池/过期直接失败。"""
    global _proxy_pool
    loaded = load_proxy_pool(required=True, allow_direct=False)
    if not loaded.ok:
        raise ProxyRequiredError(loaded.error, loaded.error_code)
    _proxy_pool = list(loaded.entries)
    ips = {e.get("exit_ip") for e in _proxy_pool if e.get("exit_ip")}
    _log.info(f"[proxy] 强制代理池就绪: {len(_proxy_pool)} 节点, {len(ips)} 独立IP")


def _next_proxy() -> dict:
    """轮询返回下一个代理；无代理视为程序错误。"""
    global _proxy_idx
    if not _proxy_pool:
        raise ProxyRequiredError("代理池为空", "POOL_EMPTY")
    p = _proxy_pool[_proxy_idx % len(_proxy_pool)]
    _proxy_idx += 1
    return p


def _make_session(warmup: bool = True) -> requests.Session:
    """创建强制带代理的 session。"""
    ua = random.choice(USER_AGENTS)
    hdrs = {
        **HEADERS,
        "User-Agent": ua,
        "Accept-Language": _LANG,
    }
    proxy = _next_proxy()
    session = make_forced_session(proxy, headers=hdrs, required=True)
    assert_session_has_proxy(session, required=True)
    if warmup:
        try:
            session.get(f"{_DOMAIN}/", timeout=15)
            _log.info(
                f"[session] warmup 完成, cookies={len(session.cookies)}, "
                f"proxy={proxy.get('exit_ip')}"
            )
            time.sleep(1 + random.uniform(0, 1))
        except Exception as e:
            _log.warning(f"[session] warmup 失败: {e}")
    return session

def _safe_get(session: requests.Session, url: str,
              referer: str = "", retries: int = 3) -> requests.Response | None:
    """带 CAPTCHA/429/503 检测和重试的 GET 请求。"""
    if referer:
        session.headers["Referer"] = referer
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=18)
            if r.status_code == 200:
                if "captcha" in r.text.lower() or "Type the characters" in r.text:
                    _log.warning(f"  [CAPTCHA] {url} — 等待 30s 后重试 ({attempt+1}/{retries})")
                    time.sleep(30 + random.uniform(0, 15))
                    session.headers["User-Agent"] = random.choice(USER_AGENTS)
                    proxy = _next_proxy()
                    session.proxies.update({"http": proxy["proxy"], "https": proxy["proxy"]})
                    assert_session_has_proxy(session, required=True)
                    continue
                return r
            if r.status_code == 429:
                wait = 60 + random.uniform(0, 30)
                _log.warning(f"  [429] {url} — 限速 {wait:.0f}s ({attempt+1}/{retries})")
                time.sleep(wait)
            elif r.status_code == 503:
                wait = 15 + random.uniform(0, 10)
                _log.warning(f"  [503] {url} — 等待 {wait:.0f}s ({attempt+1}/{retries})")
                time.sleep(wait)
            else:
                _log.debug(f"  [HTTP {r.status_code}] {url}")
                return None
        except RequestsError as e:
            wait = 5 * (2 ** attempt) + random.uniform(0, 3)
            _log.warning(f"  [网络异常] {url}: {e} — 重试等待 {wait:.0f}s")
            time.sleep(wait)
    _log.error(f"  [放弃] {url} — {retries} 次重试均失败")
    return None


# ── DB 工具 ─────────────────────────────────────────────────────────

_DETAIL_COLS = [
    ("bsr_main_rank", "INTEGER"),
    ("bsr_main_category", "TEXT"),
    ("bsr_sub_rank", "INTEGER"),
    ("bsr_sub_category", "TEXT"),
    ("variant_option_count", "INTEGER"),
    ("other_sellers_count", "INTEGER"),
    ("item_weight", "TEXT"),
    ("item_dimensions", "TEXT"),
    ("weight_lb", "REAL"),
    ("dim_l_in", "REAL"),
    ("dim_w_in", "REAL"),
    ("dim_h_in", "REAL"),
    ("date_first_available", "TEXT"),
    ("shipping_fee", "TEXT"),
    ("shipping_fee_value", "REAL"),
    ("fba_fee", "REAL"),
    ("placement_fee", "REAL"),
    ("fulfillment_type", "TEXT"),
    ("country_of_origin", "TEXT"),
    ("is_bestseller", "INTEGER DEFAULT 0"),
    ("detail_scraped", "INTEGER DEFAULT 0"),
]


def _attach_normalized_dims(d: dict) -> dict:
    return attach_normalized_dims(d)


def parse_detail_fields(html: str) -> dict:
    """兼容原调用：内部转发到共享模块并注入当前站点。"""
    return _parse_detail_fields_shared(html, _SITE)


def _check_detail_filters(detail: dict, filters: dict) -> bool:
    return _check_detail_filters_shared(detail, filters)


def _pg_fetchall(sql, params=()):
    conn = _get_pg()
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _pg_execute(sql, params=()):
    """执行 PG 写操作，返回 rowcount。"""
    conn = _get_pg()
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.rowcount


def db_conn():
    c = sqlite3.connect(DB_PATH, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    existing = {r[1] for r in c.execute("PRAGMA table_info(product_sightings)").fetchall()}
    for col, ctype in _DETAIL_COLS:
        if col not in existing:
            c.execute(f"ALTER TABLE product_sightings ADD COLUMN {col} {ctype}")
            existing.add(col)
    if "site" not in existing:
        c.execute("ALTER TABLE product_sightings ADD COLUMN site TEXT DEFAULT 'US'")
    c.commit()
    return c


def get_descendant_nodes(root_ids: list, lists: list, site: str = None,
                         include_descendants: bool = True) -> list:
    """根据选中的根节点 node_id 查抓取目标节点。

    include_descendants=True（默认）：所选节点 + 全部下级（现有行为）。
    include_descendants=False：仅所选节点本身（不展开下级）。

    node_id 仅在 (node_id, site) 组合下唯一，不同站点可能出现相同 node_id
    （例如 L1 根节点直接用 slug 字符串作 node_id）。所有查询必须显式带 site
    过滤，否则会把其它站点的同名节点及其全部后代一起选中并抓取
    （跨站点混抓）。
    """
    if not root_ids:
        return []
    if DB_BACKEND == "pg":
        return _get_descendant_nodes_pg(root_ids, include_descendants=include_descendants)
    site = (site or _SITE).upper()
    conn = db_conn()
    placeholders = ",".join("?" * len(root_ids))
    if not include_descendants:
        rows = conn.execute(
            f"""SELECT node_id, url, name, depth FROM categories
                WHERE node_id IS NOT NULL AND site = ?
                  AND node_id IN ({placeholders})
                ORDER BY depth DESC, name""",
            [site, *root_ids],
        ).fetchall()
        conn.close()
        result = [dict(r) for r in rows]
        _log.info(f"[fetch_products] [{site}] 仅抓所选 {len(root_ids)} 个节点 → {len(result)} 个目标")
        return result

    roots = conn.execute(
        f"SELECT node_id, url FROM categories WHERE node_id IN ({placeholders}) AND site = ?",
        [*root_ids, site]
    ).fetchall()
    if not roots:
        conn.close()
        return []
    like_clauses = []
    like_params = []
    for r in roots:
        prefix = r["url"].rstrip("/") + "/"
        like_clauses.append("url LIKE ?")
        like_params.append(f"{prefix}%")
    like_clauses.append(f"node_id IN ({placeholders})")
    sql = f"""
        SELECT node_id, url, name, depth FROM categories
        WHERE node_id IS NOT NULL AND site = ?
          AND ({" OR ".join(like_clauses)})
        ORDER BY depth DESC, name
    """
    rows = conn.execute(sql, [site, *like_params, *root_ids]).fetchall()
    conn.close()
    result = [dict(r) for r in rows]
    _log.info(f"[fetch_products] [{site}] 选中 {len(root_ids)} 个根节点 → {len(result)} 个后代节点（深度优先: L{result[0]['depth'] if result else '?'}→L{result[-1]['depth'] if result else '?'}）")
    return result


def _get_descendant_nodes_pg(root_ids, include_descendants: bool = True):
    ph = ",".join(["%s"] * len(root_ids))
    if not include_descendants:
        result = _pg_fetchall(
            f"""SELECT node_id, url, name, depth FROM categories
                WHERE node_id IS NOT NULL AND site = %s AND node_id IN ({ph})
                ORDER BY depth DESC, name""",
            [_SITE, *root_ids],
        )
        _log.info(f"[fetch_products] 仅抓所选 {len(root_ids)} 个节点 → {len(result)} 个目标")
        return result

    roots = _pg_fetchall(
        f"SELECT node_id, path FROM categories WHERE site = %s AND node_id IN ({ph})",
        [_SITE, *root_ids],
    )
    if not roots:
        return []
    clauses = []
    params = [_SITE]
    for r in roots:
        if r.get("path"):
            clauses.append("path <@ %s::ltree")
            params.append(str(r["path"]))
    if len(params) == 1:
        # 所有节点都无 path 时回退到 node_id IN（params 仅含 site）
        clauses.append(f"node_id IN ({ph})")
        params.extend(root_ids)
    sql = f"""
        SELECT node_id, url, name, depth FROM categories
        WHERE node_id IS NOT NULL AND site = %s AND ({" OR ".join(clauses)})
        ORDER BY depth DESC, name
    """
    result = _pg_fetchall(sql, params)
    _log.info(f"[fetch_products] 选中 {len(root_ids)} 个根节点 → {len(result)} 个后代节点（深度优先）")
    return result


_SLUG_RE = re.compile(r"^[a-zA-Z0-9_-]{1,80}$")


def _validate_slugs(slugs: list) -> list:
    """校验 slug 格式，拒绝注入字符。"""
    cleaned = []
    for s in slugs:
        if not isinstance(s, str) or not _SLUG_RE.match(s):
            raise ValueError(f"非法 slug: {s!r}（仅允许字母数字、_、-，最长80）")
        cleaned.append(s)
    return cleaned


def get_nodes_by_slugs(slugs: list, lists: list, site: str = None) -> list:
    """根据 L1 slug 查出所有后代节点。同一 slug 在不同站点的 URL 前缀不同，
    但仍需显式限定 site，避免历史数据里其它站点残留同名 slug 时混入。"""
    slugs = _validate_slugs(slugs)
    if DB_BACKEND == "pg":
        return _get_nodes_by_slugs_pg(slugs)
    site = (site or _SITE).upper()
    conn = db_conn()
    like_clauses = []
    params = []
    for slug in slugs:
        for pattern in (
            f"%/gp/new-releases/{slug}/%",
            f"%/gp/bestsellers/{slug}/%",
            f"%/gp/most-wished-for/{slug}/%",
        ):
            like_clauses.append("url LIKE ?")
            params.append(pattern)
    if not like_clauses:
        conn.close()
        return []
    sql = f"""
        SELECT DISTINCT node_id, url, name, depth FROM categories
        WHERE node_id IS NOT NULL AND site = ?
          AND ({" OR ".join(like_clauses)})
        ORDER BY depth DESC, name
    """
    rows = conn.execute(sql, [site, *params]).fetchall()
    conn.close()
    result = [dict(r) for r in rows]
    _log.info(f"[fetch_products] 选中 {len(slugs)} 个 L1 slug → {len(result)} 个后代节点（深度优先）")
    return result


def _get_nodes_by_slugs_pg(slugs):
    slugs = _validate_slugs(slugs)
    like_clauses = []
    params = [_SITE]
    for slug in slugs:
        for pattern in (
            f"%/gp/new-releases/{slug}/%",
            f"%/gp/bestsellers/{slug}/%",
            f"%/gp/most-wished-for/{slug}/%",
        ):
            like_clauses.append("url LIKE %s")
            params.append(pattern)
    if not like_clauses:
        return []
    sql = f"""
        SELECT DISTINCT node_id, url, name, depth FROM categories
        WHERE node_id IS NOT NULL AND site = %s AND ({" OR ".join(like_clauses)})
        ORDER BY depth DESC, name
    """
    result = _pg_fetchall(sql, params)
    _log.info(f"[fetch_products] 选中 {len(slugs)} 个 L1 slug → {len(result)} 个后代节点（深度优先）")
    return result


def extract_slug(url: str) -> str:
    """从类目 URL 提取 slug。"""
    parts = url.rstrip("/").split("/")
    try:
        gp_idx = parts.index("gp")
        return parts[gp_idx + 2]
    except (ValueError, IndexError):
        return ""


def save_link_validity(node_id: str, list_type: str, is_valid: int):
    """将单个榜单的有效性写入 categories。"""
    col = LIST_COL_MAP.get(list_type)
    if not col:
        return
    with _db_lock:
        if DB_BACKEND == "pg":
            conn = _get_pg()
            conn.cursor().execute(f"UPDATE categories SET {col}=%s WHERE node_id=%s", (is_valid, node_id))
        else:
            conn = db_conn()
            try:
                conn.execute(f"UPDATE categories SET {col}=? WHERE node_id=?", (is_valid, node_id))
                try:
                    conn.execute(
                        f"INSERT INTO link_cache (node_id, {col}, checked_at) "
                        f"VALUES (?, ?, datetime('now')) "
                        f"ON CONFLICT(node_id) DO UPDATE SET {col}=excluded.{col}, checked_at=datetime('now')",
                        (node_id, is_valid)
                    )
                except Exception:
                    pass
                conn.commit()
            finally:
                conn.close()


def save_products(products: list):
    """批量写入 product_sightings 表。"""
    if not products:
        return 0
    if DB_BACKEND == "pg":
        return _save_products_pg(products)
    sql = """
        INSERT OR IGNORE INTO product_sightings
        (asin, name, price, price_raw, original_price, discount_pct,
         rating, review_count, rank, image_url, product_url,
         has_video, is_amazon_choice,
         node_id, category_name, category_slug, category_depth,
         list_type, list_total, site)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    saved = 0
    with _db_lock:
        conn = db_conn()
        try:
            for p in products:
                try:
                    conn.execute(sql, (
                        p["asin"], p.get("name"), p.get("price"),
                        p.get("price_raw"), p.get("original_price"),
                        p.get("discount_pct"), p.get("rating"),
                        p.get("review_count"), p.get("rank"),
                        p.get("image_url"), p.get("product_url"),
                        p.get("has_video", 0), p.get("is_amazon_choice", 0),
                        p["node_id"], p.get("category_name"),
                        p.get("category_slug"), p.get("category_depth"),
                        p["list_type"], p.get("list_total"),
                        p.get("site", _SITE),
                    ))
                    saved += 1
                except sqlite3.IntegrityError:
                    pass
            conn.commit()
        finally:
            conn.close()
    return saved


def _save_products_pg(products):
    sql = """
        INSERT INTO product_sightings
        (asin, name, price, price_raw, original_price, discount_pct,
         rating, review_count, rank, image_url, product_url,
         has_video, is_amazon_choice,
         node_id, category_name, category_slug, category_depth,
         list_type, list_total, site)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT ON CONSTRAINT uq_ps_asin_node_list_site
        DO UPDATE SET
            name = EXCLUDED.name,
            price = EXCLUDED.price,
            price_raw = EXCLUDED.price_raw,
            rating = EXCLUDED.rating,
            review_count = EXCLUDED.review_count,
            rank = EXCLUDED.rank,
            image_url = EXCLUDED.image_url,
            product_url = EXCLUDED.product_url,
            list_total = EXCLUDED.list_total,
            scraped_at = now()
    """
    saved = 0
    with _db_lock:
        conn = _get_pg()
        cur = conn.cursor()
        for p in products:
            try:
                cur.execute(sql, (
                    p["asin"], p.get("name"), p.get("price"),
                    p.get("price_raw"), p.get("original_price"),
                    p.get("discount_pct"), p.get("rating"),
                    p.get("review_count"), p.get("rank"),
                    p.get("image_url"), p.get("product_url"),
                    p.get("has_video", 0), p.get("is_amazon_choice", 0),
                    p.get("node_id"), p.get("category_name"),
                    p.get("category_slug"), p.get("category_depth"),
                    p["list_type"], p.get("list_total"),
                    p.get("site", _SITE),
                ))
                saved += 1
            except Exception:
                _log.debug(f"save_products 写入失败 asin={p.get('asin')}: {traceback.format_exc()}")
    return saved


# ── HTML 解析 ───────────────────────────────────────────────────────

def extract_list_total(html: str) -> int:
    m = re.search(_RESULTS_PAT, html, re.IGNORECASE)
    if m:
        return int(m.group(1).replace(",", "").replace(".", "").replace(" ", ""))
    m2 = re.search(r'showing\s+\d+\s*-\s*\d+\s+of\s+([\d,]+)', html, re.IGNORECASE)
    if m2:
        return int(m2.group(1).replace(",", ""))
    soup = BeautifulSoup(html, "html.parser")
    cards = _select_product_items(soup)
    if cards:
        return len(cards)
    asins = {m.group(1) for m in re.finditer(r"/dp/([A-Z0-9]{10})", html)}
    return len(asins)


# 主选择器为线上已验证有效的两种榜单模板；下面几个是防御性兜底，
# 仅在主选择器 0 匹配时才会尝试，不影响现有已验证行为。
# 顺序按"误命中风险"从低到高排列：越靠后越宽泛（例如 [data-asin] 几乎会
# 命中页面上所有"赞助商品/其他人还买了"等不相关卡片），只在前面更精确的
# 选择器都未命中时才作为最后手段使用，避免把不相关内容当榜单商品抓入库。
_FALLBACK_ITEM_SELECTORS = [
    ".zg-item-immersion",
    "[id^='p13n-asin-index']",
    ".p13n-sc-uncoverable-faceout",
    "[data-asin]:has(a[href*='/dp/'])",
]


def _select_product_items(soup: BeautifulSoup) -> list:
    items = soup.select("[id^='gridItemRoot']")
    if items:
        return items
    items = soup.select(".zg-grid-general-faceout")
    if items:
        return items
    for sel in _FALLBACK_ITEM_SELECTORS:
        try:
            items = soup.select(sel)
        except NotImplementedError:
            # 部分 bs4/soupsieve 版本不支持 :has()，跳过该兜底选择器
            continue
        if items:
            _log.warning(f"[fetch_products] 主选择器未命中，使用兜底选择器: {sel} ({len(items)} 项)")
            return items
    return []


def _count_product_items(html: str) -> int:
    return len(_select_product_items(BeautifulSoup(html, "html.parser")))


def parse_products(html: str, node_id: str, category_name: str,
                   category_slug: str, category_depth: int,
                   list_type: str, list_total: int,
                   review_max: int,
                   price_min: float = 0.0,
                   price_max: float = 0.0,
                   review_min: int = 0,
                   rating_min: float = 0.0,
                   rating_max: float = 0.0,
                   list_limit: int = 0,
                   position_start: int = 1) -> list:
    """解析单页 HTML，提取符合条件的商品。"""
    soup = BeautifulSoup(html, "html.parser")
    items = _select_product_items(soup)

    products = []
    for idx, item in enumerate(items):
        p = {}
        list_position = position_start + idx

        # ASIN
        link = item.select_one("a[href*='/dp/']")
        if not link:
            continue
        href = link.get("href", "")
        m = re.search(r"/dp/([A-Z0-9]{10})", href)
        if not m:
            continue
        p["asin"] = m.group(1)
        p["product_url"] = (_DOMAIN + href) if href.startswith("/") else href

        # 商品名
        name_el = (item.select_one("div._cDEzb_p13n-sc-css-line-clamp-3_g3dy1")
                   or item.select_one(".p13n-sc-truncate")
                   or item.select_one("a > span > div"))
        p["name"] = name_el.get_text(strip=True) if name_el else ""

        # 图片：data-a-dynamic-image → srcset → data-src → src（跳过占位图）
        img = item.select_one("img")
        p["image_url"] = extract_image_url(img)

        # 价格
        price_el = item.select_one(".a-price .a-offscreen")
        if not price_el:
            price_el = item.select_one("._cDEzb_p13n-sc-price_3mJ9Z")
        if price_el:
            raw = price_el.get_text(strip=True)
            p["price_raw"] = raw
            m_price = re.search(r"[\d,.]+", raw)
            if m_price:
                price_str = m_price.group()
                if _DECIMAL_SEP == ",":
                    price_str = price_str.replace(".", "").replace(",", ".")
                else:
                    price_str = price_str.replace(",", "")
                try:
                    p["price"] = float(price_str)
                except ValueError:
                    pass

        # 原价
        orig_el = item.select_one(".a-text-price .a-offscreen")
        if orig_el:
            p["original_price"] = orig_el.get_text(strip=True)

        # 折扣
        disc_el = item.select_one(".a-badge-label-inner, [data-a-badge-color='sx-orange']")
        if disc_el:
            p["discount_pct"] = disc_el.get_text(strip=True)

        # 评分
        rating_el = item.select_one(".a-icon-alt")
        if rating_el:
            rt = rating_el.get_text(strip=True)
            m_rt = re.search(_RATING_PAT, rt)
            if m_rt:
                raw_rating = m_rt.group(1).replace(",", ".")
                try:
                    p["rating"] = float(raw_rating)
                except ValueError:
                    pass

        # 评论数
        review_el = item.select_one("a.a-size-small span, span.a-size-small")
        if review_el:
            rt_text = review_el.get_text(strip=True).replace(",", "")
            if rt_text.isdigit():
                p["review_count"] = int(rt_text)

        # 排名
        rank_el = item.select_one(".zg-badge-text")
        if rank_el:
            rk = rank_el.get_text(strip=True).lstrip("#")
            if rk.isdigit():
                p["rank"] = int(rk)
                list_position = p["rank"]

        if list_limit > 0 and list_position > list_limit:
            continue

        # 视频标记
        p["has_video"] = 1 if item.select_one(".vse-video-badge, .a-icon-vse") else 0

        # Amazon's Choice
        badge_text = " ".join(
            el.get_text(" ", strip=True)
            for el in item.select(".a-badge, .a-badge-label, .a-badge-label-inner, [data-a-badge-type]")
        )
        badge_type = " ".join(
            el.get("data-a-badge-type", "")
            for el in item.select("[data-a-badge-type]")
        )
        badge_blob = f"{badge_type} {badge_text}".lower()
        p["is_amazon_choice"] = 1 if (
            "amazons-choice" in badge_blob
            or "amazon's choice" in badge_blob
            or "amazon choice" in badge_blob
            or "amazon\u304a\u3059\u3059\u3081" in badge_blob
        ) else 0
        p["is_bestseller"] = 1 if re.search(
            r"best[\s-]*seller|bestseller|\u30d9\u30b9\u30c8\u30bb\u30e9\u30fc|\u58f2\u308c\u7b4b",
            badge_blob,
            re.I,
        ) else 0

        # ── 评论数筛选（闭区间；缺失值不通过）──
        if review_max > 0 or review_min > 0:
            if "review_count" not in p or p.get("review_count") is None:
                continue
            rc = p["review_count"]
            if review_max > 0 and rc > review_max:
                continue
            if review_min > 0 and rc < review_min:
                continue

        # ── 评分筛选 ──
        rt = p.get("rating")
        if (rating_min > 0 or rating_max > 0) and rt is None:
            continue
        if rt is not None:
            if rating_min > 0 and rt < rating_min:
                continue
            if rating_max > 0 and rt > rating_max:
                continue

        # ── 价格筛选 ──
        price = p.get("price")
        if (price_min > 0 or price_max > 0) and price is None:
            continue
        if price is not None:
            if price_min > 0 and price < price_min:
                continue
            if price_max > 0 and price > price_max:
                continue

        # ── ASIN去重 ──
        asin = p["asin"]
        with _seen_lock:
            if asin in _seen_asins:
                with _stats_lock:
                    _stats["products_dup"] += 1
                continue
            _seen_asins.add(asin)

        # 来源信息
        p["node_id"] = node_id
        p["category_name"] = category_name
        p["category_slug"] = category_slug
        p["category_depth"] = category_depth
        p["list_type"] = list_type
        p["list_total"] = list_total

        products.append(p)

    return products


_log.info("[fetch_products] 模块加载完成")




def enrich_with_details(products: list, session: requests.Session,
                        delay: float, filters: dict = None):
    """对列表页抓到的商品逐个请求详情页，补全字段并 UPDATE 到数据库。
    不符合筛选条件的商品从数据库删除。
    详情请求/解析失败时标记 detail_scraped=2，结果接口默认不展示。"""
    if not products:
        return
    if filters is None:
        filters = {}
    for p in products:
        asin = p["asin"]
        url = f"{_DOMAIN}/dp/{asin}"
        referer = p.get("product_url", f"{_DOMAIN}/s?k={asin}")
        try:
            r = _safe_get(session, url, referer=referer)
            if r is None or r.status_code != 200:
                _mark_detail_failed(asin)
                continue
            detail = parse_detail_fields(r.text)
            if not detail:
                _mark_detail_failed(asin)
                continue
            # 列表价用于 Low-Price FBA（<$10）判定
            if p.get("price") is not None and detail.get("price") is None:
                detail["price"] = p["price"]
            _attach_normalized_dims(detail)
            fees = estimate_fba_fees(
                _SITE,
                detail.get("item_weight"),
                detail.get("item_dimensions"),
                detail.get("price") if detail.get("price") is not None else p.get("price"),
            )
            if fees.get("fba_fee") is not None:
                detail["fba_fee"] = fees["fba_fee"]
            if fees.get("placement_fee") is not None:
                detail["placement_fee"] = fees["placement_fee"]
            detail["detail_scraped"] = 1

            # 详情页筛选 — 按 asin+site 删除，避免误伤其他站点
            if filters and not _check_detail_filters(detail, filters):
                _delete_sighting(asin)
                with _stats_lock:
                    _stats["products_saved"] -= 1
                continue

            # UPDATE DB — 按 asin+site
            _update_sighting_detail(asin, detail)
            p.update(detail)
        except (KeyError, TypeError, ValueError, AttributeError) as e:
            _mark_detail_failed(asin)
            _log.error(f"  [detail] {asin} 解析异常: {e}\n{traceback.format_exc()}")
        except Exception as e:
            _mark_detail_failed(asin)
            _log.error(f"  [detail] {asin} 未知异常: {e}\n{traceback.format_exc()}")
        time.sleep(delay + random.uniform(delay * 0.3, delay * 0.8))


def _delete_sighting(asin: str):
    """按 asin + 当前站点删除 product_sightings 记录。"""
    with _db_lock:
        if DB_BACKEND == "pg":
            _pg_execute(
                "DELETE FROM product_sightings WHERE asin=%s AND site=%s",
                (asin, _SITE),
            )
        else:
            conn = db_conn()
            try:
                conn.execute(
                    "DELETE FROM product_sightings WHERE asin=? AND site=?",
                    (asin, _SITE),
                )
                conn.commit()
            finally:
                conn.close()


def _mark_detail_failed(asin: str):
    """详情抓取失败：detail_scraped=2，结果接口默认不返回。"""
    with _db_lock:
        if DB_BACKEND == "pg":
            _pg_execute(
                "UPDATE product_sightings SET detail_scraped=2 WHERE asin=%s AND site=%s",
                (asin, _SITE),
            )
        else:
            conn = db_conn()
            try:
                conn.execute(
                    "UPDATE product_sightings SET detail_scraped=2 WHERE asin=? AND site=?",
                    (asin, _SITE),
                )
                conn.commit()
            finally:
                conn.close()


def _update_sighting_detail(asin: str, detail: dict):
    """按 asin + 当前站点更新详情字段。"""
    with _db_lock:
        if DB_BACKEND == "pg":
            sets = ", ".join(f"{k}=%s" for k in detail)
            vals = list(detail.values()) + [asin, _SITE]
            _pg_execute(
                f"UPDATE product_sightings SET {sets} WHERE asin=%s AND site=%s",
                vals,
            )
        else:
            sets = ", ".join(f"{k}=?" for k in detail)
            vals = list(detail.values()) + [asin, _SITE]
            conn = db_conn()
            try:
                conn.execute(
                    f"UPDATE product_sightings SET {sets} WHERE asin=? AND site=?",
                    vals,
                )
                conn.commit()
            finally:
                conn.close()


# ── Worker 主循环 ───────────────────────────────────────────────────

def process_node(node: dict, lists: list, review_max: int,
                 min_list_size: int, session: requests.Session,
                 price_min: float = 0.0, price_max: float = 0.0,
                 review_min: int = 0,
                 rating_min: float = 0.0, rating_max: float = 0.0,
                 max_pages: int = 2, delay: float = 2.0,
                 detail_filters: dict = None,
                 list_limit: int = 0):
    """处理单个节点的所有榜单。"""
    node_id = node["node_id"]
    slug    = extract_slug(node["url"])
    name    = node["name"]
    depth   = node["depth"]
    try:
        list_limit = int(list_limit)
    except (TypeError, ValueError):
        list_limit = 0
    list_limit = max(0, min(list_limit, 100))
    max_pages = max(1, min(int(max_pages), 2))

    for list_type in lists:
        url_base = f"{_DOMAIN}/gp/{list_type}/{slug}/{node_id}/"

        r = _safe_get(session, url_base, referer=f"{_DOMAIN}/")
        if r is None:
            with _stats_lock:
                _stats["errors"] += 1
            continue

        save_link_validity(node_id, list_type, 1 if r.status_code == 200 else 0)

        if r.status_code != 200:
            continue

        time.sleep(delay + random.uniform(0, delay * 0.5))

        html = r.text

        total = extract_list_total(html)
        if min_list_size > 0 and total < min_list_size:
            with _stats_lock:
                _stats["skipped"] += 1
            continue

        position_start = 1
        all_products = parse_products(
            html, node_id, name, slug, depth,
            list_type, total, review_max, price_min, price_max,
            review_min, rating_min, rating_max,
            list_limit, position_start
        )
        position_start += _count_product_items(html)

        for pg in range(2, max_pages + 1):
            if list_limit > 0 and position_start > list_limit:
                break
            rp = _safe_get(session, url_base + f"?pg={pg}", referer=url_base)
            if rp is None or rp.status_code != 200:
                break
            all_products += parse_products(
                rp.text, node_id, name, slug, depth,
                list_type, total, review_max, price_min, price_max,
                review_min, rating_min, rating_max,
                list_limit, position_start
            )
            position_start += _count_product_items(rp.text)
            time.sleep(delay + random.uniform(0, delay * 0.5))

        with _stats_lock:
            _stats["products_found"] += len(all_products)

        if all_products:
            saved = save_products(all_products)
            with _stats_lock:
                _stats["products_saved"] += saved
            enrich_with_details(all_products, session, delay, detail_filters)

    with _stats_lock:
        _stats["done_nodes"] += 1


def run_batch(root_ids: list, lists: list, review_max: int,
              min_list_size: int, delay: float = 2.0,
              price_min: float = 0.0, price_max: float = 0.0,
              review_min: int = 0,
              rating_min: float = 0.0, rating_max: float = 0.0,
              max_pages: int = 2,
              slugs: list = None,
              detail_filters: dict = None,
              list_limit: int = 0,
              include_descendants: bool = True):
    """主入口：单线程顺序抓取。从最深层类目开始，逐层向上。"""
    if slugs:
        # --slugs 语义本身就是 L1 下全部后代；exact 模式对 slug 入口不适用，仍展开
        nodes = get_nodes_by_slugs(slugs, lists, site=_SITE)
    else:
        nodes = get_descendant_nodes(
            root_ids, lists, site=_SITE, include_descendants=include_descendants
        )
    _stats["total_nodes"] = len(nodes)
    _stats["products_dup"] = 0
    with _seen_lock:
        _seen_asins.clear()
    try:
        list_limit = int(list_limit)
    except (TypeError, ValueError):
        list_limit = 0
    list_limit = max(0, min(list_limit, 100))
    max_pages = max(1, min(int(max_pages), 2))

    if not nodes:
        _log.warning("[fetch_products] 无目标节点，退出")
        return

    try:
        _validate_proxy_pool()
        session = _make_session()
    except ProxyRequiredError as e:
        _log.error(f"[fetch_products] 代理强制模式失败: {e.code} {e}")
        raise SystemExit(2) from e

    t0 = time.time()
    price_info = ""
    if price_min > 0 or price_max > 0:
        c = _CURRENCY
        price_info = f" 价格{c}{price_min:.0f}-{c}{price_max:.0f}" if price_max > 0 else f" 价格>{c}{price_min:.0f}"
    _log.info(f"[fetch_products] 开始抓取: {len(nodes)} 节点 × {len(lists)} 榜单, "
          f"评论<{review_max}, 最少{min_list_size}商品{price_info}, 延迟{delay}s",
          flush=True)

    for node in nodes:
        process_node(node, lists, review_max, min_list_size, session,
                     price_min, price_max, review_min,
                     rating_min, rating_max, max_pages, delay,
                     detail_filters,
                     list_limit)
        n = _stats["done_nodes"]
        total = _stats["total_nodes"]
        if n % 10 == 0 or n == total:
            elapsed = time.time() - t0
            rate = n / elapsed if elapsed > 0 else 0
            _log.info(f"  [{n}/{total}] {rate:.1f}节点/s "
                  f"找到:{_stats['products_found']} "
                  f"录入:{_stats['products_saved']} "
                  f"跳过:{_stats['skipped']}")

    elapsed = time.time() - t0
    _log.info(f"\n[fetch_products] 完成！"
              f"\n  耗时: {elapsed:.0f}s"
              f"\n  节点: {_stats['done_nodes']}/{_stats['total_nodes']}"
              f"\n  找到: {_stats['products_found']} 个符合条件商品"
              f"\n  录入: {_stats['products_saved']} 条（去重后）"
              f"\n  跳过: {_stats['skipped']} 个冷门榜单"
              f"\n  去重: {_stats['products_dup']} 个重复ASIN已跳过"
              f"\n  错误: {_stats['errors']}")

    export_excel()


def export_excel():
    """导出去重后的商品到 Excel。"""
    try:
        import openpyxl
    except ImportError:
        _log.error("[fetch_products] 需要 openpyxl: pip install openpyxl")
        return

    if DB_BACKEND == "pg":
        rows = _pg_fetchall("""
            SELECT asin, name, price, rating, review_count, image_url, product_url,
                   STRING_AGG(DISTINCT list_type, ',') AS appeared_lists,
                   COUNT(DISTINCT list_type) AS list_count,
                   STRING_AGG(DISTINCT category_name, ',') AS categories,
                   MIN(rank) AS best_rank,
                   MIN(scraped_at) AS first_seen
            FROM product_sightings
            GROUP BY asin, name, price, rating, review_count, image_url, product_url
            ORDER BY list_count DESC, review_count ASC
        """)
    else:
        conn = db_conn()
        rows = conn.execute("""
            SELECT asin, name, price, price_raw, original_price, discount_pct,
                   rating, review_count, image_url, product_url,
                   has_video, is_amazon_choice,
                   GROUP_CONCAT(DISTINCT list_type) AS appeared_lists,
                   COUNT(DISTINCT list_type) AS list_count,
                   GROUP_CONCAT(DISTINCT category_name) AS categories,
                   MIN(rank) AS best_rank,
                   MIN(scraped_at) AS first_seen
            FROM product_sightings
            GROUP BY asin
            ORDER BY list_count DESC, review_count ASC
        """).fetchall()
        conn.close()

    if not rows:
        _log.warning("[fetch_products] 无数据可导出")
        return

    excel_path = os.path.join(BASE, "data", "products.xlsx")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "筛选结果"

    headers = ["ASIN", "商品名", "价格", "价格原始", "原价", "折扣",
               "评分", "评论数", "图片URL", "商品URL",
               "有视频", "Amazon's Choice",
               "出现榜单", "榜单数", "所属类目", "最佳排名", "首次发现"]
    ws.append(headers)

    for r in rows:
        ws.append(list(r))

    wb.save(excel_path)
    _log.info(f"[fetch_products] Excel 已导出: {excel_path} ({len(rows)} 行)")


# ── CLI 入口 ────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Amazon 榜单商品抓取")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--roots", nargs="+",
                       help="根节点 node_id 列表")
    group.add_argument("--slugs", nargs="+",
                       help="L1 类目 slug 列表 (如 automotive baby-products)")
    parser.add_argument("--site", default="US", help="站点代码: US, DE, JP, UK, FR")
    parser.add_argument("--review-max", type=int, default=0)
    parser.add_argument("--review-min", type=int, default=0)
    parser.add_argument("--min-list",   type=int, default=0)
    parser.add_argument("--price-min",  type=float, default=DEFAULT_PRICE_MIN)
    parser.add_argument("--price-max",  type=float, default=DEFAULT_PRICE_MAX)
    parser.add_argument("--rating-min", type=float, default=0)
    parser.add_argument("--rating-max", type=float, default=0)
    parser.add_argument("--bsr-main-min", type=int, default=0)
    parser.add_argument("--bsr-main-max", type=int, default=0)
    parser.add_argument("--bsr-sub-min",  type=int, default=0)
    parser.add_argument("--bsr-sub-max",  type=int, default=0)
    parser.add_argument("--variant-min",  type=int, default=0)
    parser.add_argument("--variant-max",  type=int, default=0)
    parser.add_argument("--sellers-min",  type=int, default=0)
    parser.add_argument("--sellers-max",  type=int, default=0)
    parser.add_argument("--weight-min", type=float, default=0)
    parser.add_argument("--weight-max", type=float, default=0)
    parser.add_argument("--dim-l", type=float, default=0)
    parser.add_argument("--dim-w", type=float, default=0)
    parser.add_argument("--dim-h", type=float, default=0)
    parser.add_argument("--list-limit", type=int, default=0)
    parser.add_argument("--fba-fee-min", type=float, default=0)
    parser.add_argument("--fba-fee-max", type=float, default=0)
    parser.add_argument("--fulfillment-type", default="")
    parser.add_argument("--country", default="")
    parser.add_argument("--date-range", default="")
    parser.add_argument("--date-from",  default="")
    parser.add_argument("--date-to",    default="")
    parser.add_argument("--amazons-choice", action="store_true")
    parser.add_argument("--bestseller",     action="store_true")
    parser.add_argument("--max-pages", type=int, default=2)
    parser.add_argument("--delay",      type=float, default=DEFAULT_DELAY)
    parser.add_argument("--lists", nargs="+", default=DEFAULT_LISTS)
    parser.add_argument(
        "--exact-roots", action="store_true",
        help="仅抓 --roots 所选类目本身，不展开全部下级（默认会展开）",
    )
    args = parser.parse_args()
    args.list_limit = max(0, min(args.list_limit, 100))
    args.max_pages = max(1, min(args.max_pages, 2))

    mp = get_marketplace(args.site)
    _SITE   = args.site.upper()
    _DOMAIN = mp["domain"]
    _LANG   = mp["lang"]
    _CURRENCY    = mp["currency"]
    _DECIMAL_SEP = mp["decimal_sep"]
    _RATING_PAT  = mp["rating_pattern"]
    _RESULTS_PAT = mp["results_pattern"]
    _log.info(f"[站点] {mp['name']} ({_SITE}) → {_DOMAIN}")

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

    run_batch(
        root_ids=args.roots or [],
        lists=args.lists,
        review_max=args.review_max,
        min_list_size=args.min_list,
        delay=args.delay,
        price_min=args.price_min,
        price_max=args.price_max,
        review_min=args.review_min,
        rating_min=args.rating_min,
        rating_max=args.rating_max,
        max_pages=args.max_pages,
        slugs=args.slugs,
        detail_filters=detail_filters,
        list_limit=args.list_limit,
        include_descendants=not args.exact_roots,
    )
