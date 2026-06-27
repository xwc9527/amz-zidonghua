"""
fetch_products.py — 商品抓取脚本（独立进程）
从 categories.db 读取有效节点，抓取 3 个 SSR 榜单的商品数据
用法: python fetch_products.py
"""
import sqlite3, requests, threading, time, sys, os, re, json, argparse, logging, traceback, random
import urllib3; urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from datetime import datetime
from queue import Queue, Empty
from bs4 import BeautifulSoup

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
        from pg_config import PG_DSN
        _pg_conn = psycopg2.connect(PG_DSN)
        _pg_conn.autocommit = True
    return _pg_conn

# 默认值（可被看板 API 参数覆盖）
DEFAULT_REVIEW_MAX    = 10
DEFAULT_MIN_LIST_SIZE = 100
DEFAULT_PRICE_MIN     = 0.0
DEFAULT_PRICE_MAX     = 0.0
DEFAULT_DELAY         = 2.0   # 请求间隔（秒）
DEFAULT_LISTS         = ["new-releases", "bestsellers", "movers-and-shakers", "most-wished-for"]

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

from config import PROXY_ENABLED, PROXY_POOL_FILE, PROXY_VERIFY

LIST_LABELS = {
    "new-releases":       "新品榜",
    "bestsellers":        "畅销榜",
    "movers-and-shakers": "飙升榜",
    "most-wished-for":    "心愿单",
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

# ── 代理池 ─────────────────────────────────────────────────────────

_proxy_pool: list[dict] = []
_proxy_idx = 0

def _validate_proxy_pool():
    """启动时并发校验 proxy_pool.json 中所有端口，剔除不可用的。"""
    global _proxy_pool
    if not PROXY_ENABLED or not os.path.exists(PROXY_POOL_FILE):
        _log.info("[proxy] 代理未启用或池文件不存在，使用直连")
        return
    with open(PROXY_POOL_FILE, encoding="utf-8") as f:
        pool = json.load(f)
    if not pool:
        _log.info("[proxy] 代理池为空，使用直连")
        return

    _log.info(f"[proxy] 启动校验 {len(pool)} 个代理端口...")
    CHECK_URL = "http://ip-api.com/json?fields=query,country"
    alive = []
    lock = threading.Lock()

    local_ip = ""
    try:
        r = requests.get("http://ip-api.com/json?fields=query", timeout=5)
        local_ip = r.json().get("query", "")
    except Exception:
        pass

    def _check(entry):
        px = {"http": entry["proxy"], "https": entry["proxy"]}
        try:
            r = requests.get(CHECK_URL, proxies=px, verify=False, timeout=12)
            d = r.json()
            ip = d.get("query", "")
            if ip and ip != local_ip and not ip.startswith(("192.", "10.")):
                entry_copy = dict(entry)
                entry_copy["exit_ip"] = ip
                entry_copy["country"] = d.get("country", "?")
                with lock:
                    alive.append(entry_copy)
                _log.debug(f"  ✅ {entry.get('port', '?')} → {ip} ({d.get('country', '?')})")
            else:
                _log.debug(f"  ❌ {entry.get('port', '?')} IP异常({ip})")
        except Exception as e:
            _log.debug(f"  ❌ {entry.get('port', '?')} {e}")

    threads = [threading.Thread(target=_check, args=(p,), daemon=True) for p in pool]
    for t in threads:
        t.start()
        time.sleep(0.15)
    for t in threads:
        t.join()

    seen_ips = {}
    for entry in sorted(alive, key=lambda x: x.get("delay") or 9999):
        ip = entry.get("exit_ip", "")
        if ip and ip not in seen_ips:
            seen_ips[ip] = entry
    _proxy_pool = sorted(seen_ips.values(), key=lambda x: x.get("delay") or 9999)

    with open(PROXY_POOL_FILE, "w", encoding="utf-8") as f:
        json.dump(_proxy_pool, f, ensure_ascii=False, indent=2)
    _log.info(f"[proxy] 校验完成: {len(_proxy_pool)}/{len(pool)} 可用, {len(seen_ips)} 独立IP")


def _next_proxy() -> dict | None:
    """轮询返回下一个代理。"""
    global _proxy_idx
    if not _proxy_pool:
        return None
    p = _proxy_pool[_proxy_idx % len(_proxy_pool)]
    _proxy_idx += 1
    return p


def _make_session(warmup: bool = True) -> requests.Session:
    """创建带 UA 轮换、Sec-Fetch 头、代理的 session，并 warmup 拿 cookie。"""
    session = requests.Session()
    ua = random.choice(USER_AGENTS)
    session.headers.update({
        **HEADERS,
        "User-Agent": ua,
        "Accept-Language": _LANG,
    })
    proxy = _next_proxy()
    if proxy:
        session.proxies.update({"http": proxy["proxy"], "https": proxy["proxy"]})
    if warmup:
        try:
            session.get(f"{_DOMAIN}/", timeout=15, verify=PROXY_VERIFY)
            _log.info(f"[session] warmup 完成, cookies={len(session.cookies)}")
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
            r = session.get(url, timeout=18, verify=PROXY_VERIFY)
            if r.status_code == 200:
                if "captcha" in r.text.lower() or "Type the characters" in r.text:
                    _log.warning(f"  [CAPTCHA] {url} — 等待 30s 后重试 ({attempt+1}/{retries})")
                    time.sleep(30 + random.uniform(0, 15))
                    session.headers["User-Agent"] = random.choice(USER_AGENTS)
                    proxy = _next_proxy()
                    if proxy:
                        session.proxies.update({"http": proxy["proxy"], "https": proxy["proxy"]})
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
        except requests.RequestException as e:
            wait = 5 * (2 ** attempt) + random.uniform(0, 3)
            _log.warning(f"  [网络异常] {url}: {e} — 重试等待 {wait:.0f}s")
            time.sleep(wait)
    _log.error(f"  [放弃] {url} — {retries} 次重试均失败")
    return None


# ── DB 工具 ─────────────────────────────────────────────────────────

DE_MONTHS = {
    "Januar": 1, "Februar": 2, "März": 3, "April": 4,
    "Mai": 5, "Juni": 6, "Juli": 7, "August": 8,
    "September": 9, "Oktober": 10, "November": 11, "Dezember": 12,
}

_DETAIL_COLS = [
    ("bsr_main_rank", "INTEGER"),
    ("bsr_main_category", "TEXT"),
    ("bsr_sub_rank", "INTEGER"),
    ("bsr_sub_category", "TEXT"),
    ("variant_option_count", "INTEGER"),
    ("other_sellers_count", "INTEGER"),
    ("item_weight", "TEXT"),
    ("item_dimensions", "TEXT"),
    ("date_first_available", "TEXT"),
    ("shipping_fee", "TEXT"),
    ("shipping_fee_value", "REAL"),
    ("fulfillment_type", "TEXT"),
    ("country_of_origin", "TEXT"),
    ("detail_scraped", "INTEGER DEFAULT 0"),
]


def db_conn():
    c = sqlite3.connect(DB_PATH, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    existing = {r[1] for r in c.execute("PRAGMA table_info(product_sightings)").fetchall()}
    for col, ctype in _DETAIL_COLS:
        if col not in existing:
            c.execute(f"ALTER TABLE product_sightings ADD COLUMN {col} {ctype}")
    c.commit()
    return c


def _pg_fetchall(sql, params=()):
    conn = _get_pg()
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_descendant_nodes(root_ids: list, lists: list) -> list:
    """根据选中的根节点 node_id，查出所有后代叶子节点。"""
    if DB_BACKEND == "pg":
        return _get_descendant_nodes_pg(root_ids)
    conn = db_conn()
    placeholders = ",".join("?" * len(root_ids))
    roots = conn.execute(
        f"SELECT node_id, url FROM categories WHERE node_id IN ({placeholders})",
        root_ids
    ).fetchall()
    if not roots:
        conn.close()
        return []
    like_clauses = []
    for r in roots:
        prefix = r["url"].rstrip("/") + "/"
        like_clauses.append(f"url LIKE '{prefix}%'")
    like_clauses.append(f"node_id IN ({placeholders})")
    sql = f"""
        SELECT node_id, url, name, depth FROM categories
        WHERE node_id IS NOT NULL
          AND ({" OR ".join(like_clauses)})
        ORDER BY depth DESC, name
    """
    rows = conn.execute(sql, root_ids).fetchall()
    conn.close()
    result = [dict(r) for r in rows]
    _log.info(f"[fetch_products] 选中 {len(root_ids)} 个根节点 → {len(result)} 个后代节点（深度优先: L{result[0]['depth'] if result else '?'}→L{result[-1]['depth'] if result else '?'}）")
    return result


def _get_descendant_nodes_pg(root_ids):
    ph = ",".join(["%s"] * len(root_ids))
    roots = _pg_fetchall(
        f"SELECT node_id, path FROM categories WHERE node_id IN ({ph})", root_ids
    )
    if not roots:
        return []
    clauses = []
    params = list(root_ids)
    for r in roots:
        if r.get("path"):
            clauses.append("path <@ %s::ltree")
            params.append(str(r["path"]))
    if not clauses:
        clauses.append(f"node_id IN ({ph})")
    sql = f"""
        SELECT node_id, url, name, depth FROM categories
        WHERE node_id IS NOT NULL AND ({" OR ".join(clauses)})
        ORDER BY depth DESC, name
    """
    result = _pg_fetchall(sql, params)
    _log.info(f"[fetch_products] 选中 {len(root_ids)} 个根节点 → {len(result)} 个后代节点（深度优先）")
    return result


def get_nodes_by_slugs(slugs: list, lists: list) -> list:
    """根据 L1 slug 查出所有后代节点。"""
    if DB_BACKEND == "pg":
        return _get_nodes_by_slugs_pg(slugs)
    conn = db_conn()
    like_clauses = []
    for slug in slugs:
        like_clauses.append(f"url LIKE '%/gp/new-releases/{slug}/%'")
        like_clauses.append(f"url LIKE '%/gp/bestsellers/{slug}/%'")
        like_clauses.append(f"url LIKE '%/gp/most-wished-for/{slug}/%'")
    if not like_clauses:
        conn.close()
        return []
    sql = f"""
        SELECT DISTINCT node_id, url, name, depth FROM categories
        WHERE node_id IS NOT NULL
          AND ({" OR ".join(like_clauses)})
        ORDER BY depth DESC, name
    """
    rows = conn.execute(sql).fetchall()
    conn.close()
    result = [dict(r) for r in rows]
    _log.info(f"[fetch_products] 选中 {len(slugs)} 个 L1 slug → {len(result)} 个后代节点（深度优先）")
    return result


def _get_nodes_by_slugs_pg(slugs):
    like_clauses = []
    for slug in slugs:
        like_clauses.append(f"url LIKE '%/gp/new-releases/{slug}/%'")
        like_clauses.append(f"url LIKE '%/gp/bestsellers/{slug}/%'")
        like_clauses.append(f"url LIKE '%/gp/most-wished-for/{slug}/%'")
    if not like_clauses:
        return []
    sql = f"""
        SELECT DISTINCT node_id, url, name, depth FROM categories
        WHERE node_id IS NOT NULL AND site = %s AND ({" OR ".join(like_clauses)})
        ORDER BY depth DESC, name
    """
    result = _pg_fetchall(sql, (_SITE,))
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
         list_type, list_total)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
        (asin, name, price, review_count, rank, rating,
         image_url, product_url, list_type, category_name, site)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    saved = 0
    with _db_lock:
        conn = _get_pg()
        cur = conn.cursor()
        for p in products:
            try:
                cur.execute(sql, (
                    p["asin"], p.get("name"), p.get("price"),
                    p.get("review_count"), p.get("rank"), p.get("rating"),
                    p.get("image_url"), p.get("product_url"),
                    p["list_type"], p.get("category_name"), _SITE,
                ))
                saved += 1
            except sqlite3.IntegrityError:
                pass
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
    return 0


def parse_products(html: str, node_id: str, category_name: str,
                   category_slug: str, category_depth: int,
                   list_type: str, list_total: int,
                   review_max: int,
                   price_min: float = 0.0,
                   price_max: float = 0.0,
                   review_min: int = 0,
                   rating_min: float = 0.0,
                   rating_max: float = 0.0) -> list:
    """解析单页 HTML，提取符合条件的商品。"""
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("[id^='gridItemRoot']")
    if not items:
        items = soup.select(".zg-grid-general-faceout")

    products = []
    for item in items:
        p = {}

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

        # 图片
        img = item.select_one("img")
        if img:
            p["image_url"] = img.get("src", "")

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

        # 视频标记
        p["has_video"] = 1 if item.select_one(".vse-video-badge, .a-icon-vse") else 0

        # Amazon's Choice
        p["is_amazon_choice"] = 1 if item.select_one(".a-badge[data-a-badge-type='amazons-choice']") else 0

        # ── 评论数筛选 ──
        rc = p.get("review_count", 0)
        if review_max > 0 and rc >= review_max:
            continue
        if review_min > 0 and rc < review_min:
            continue

        # ── 评分筛选 ──
        rt = p.get("rating")
        if rt is not None:
            if rating_min > 0 and rt < rating_min:
                continue
            if rating_max > 0 and rt > rating_max:
                continue

        # ── 价格筛选 ──
        price = p.get("price")
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


# ── 详情页解析 ─────────────────────────────────────────────────────

def parse_detail_fields(html: str) -> dict:
    """从详情页HTML提取补全字段。"""
    soup = BeautifulSoup(html, "html.parser")
    d = {}

    # BSR
    bsr_section = (
        soup.select_one("#prodDetails")
        or soup.select_one("#detailBulletsWrapper_feature_div")
        or soup.select_one("#productDetails_db_sections")
    )
    if bsr_section:
        bsr_text = bsr_section.get_text(" ")
        bsr_matches = []
        for pat in [r"Nr\.\s*([\d\.]+)\s+in\s+(.+?)(?:\s*\(|\s{2,}|\s*#|\s*$)",
                    r"#([\d,]+)\s+in\s+(.+?)(?:\s*\(|\s{2,}|\s*#|\s*$)"]:
            for m in re.finditer(pat, bsr_text):
                rank_str = m.group(1).replace(".", "").replace(",", "")
                cat = m.group(2).strip().rstrip("( ,")
                if not cat or len(cat) < 2:
                    continue
                try:
                    bsr_matches.append((int(rank_str), cat))
                except ValueError:
                    pass
        if bsr_matches:
            d["bsr_main_rank"] = bsr_matches[0][0]
            d["bsr_main_category"] = bsr_matches[0][1]
        if len(bsr_matches) > 1:
            d["bsr_sub_rank"] = bsr_matches[1][0]
            d["bsr_sub_category"] = bsr_matches[1][1]

    # 商品属性表
    detail_rows = soup.select(
        "#detailBullets_feature_div li, "
        "#productDetails_techSpec_section_1 tr, "
        "#productDetails_detailBullets_sections1 tr, "
        "#prodDetails tr"
    )
    for row in detail_rows:
        text = row.get_text(" ", strip=True)

        # 上架日期
        if any(k in text for k in ["Date First Available", "Datum der Ersten",
                                     "Erstmals verfügbar", "発売日"]):
            dm = re.search(r"(\d{1,2})\.\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s*(\d{4})", text)
            if dm:
                try:
                    dt = datetime(int(dm.group(3)), DE_MONTHS[dm.group(2)], int(dm.group(1)))
                    d["date_first_available"] = dt.strftime("%Y-%m-%d")
                except (ValueError, KeyError):
                    pass
            if "date_first_available" not in d:
                em = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})", text)
                if em:
                    try:
                        dt = datetime.strptime(f"{em.group(1)} {em.group(2)} {em.group(3)}", "%B %d %Y")
                        d["date_first_available"] = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass
            if "date_first_available" not in d:
                jm = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", text)
                if jm:
                    try:
                        dt = datetime(int(jm.group(1)), int(jm.group(2)), int(jm.group(3)))
                        d["date_first_available"] = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        pass

        # 重量
        if any(k in text for k in ["Item Weight", "Artikelgewicht", "商品の重量"]):
            wm = re.search(r"([\d,.]+)\s*(pounds?|ounces?|kg|g|Kilogramm|Gramm|lbs?|oz)\b", text, re.I)
            if wm:
                d["item_weight"] = wm.group(0).strip()

        # 尺寸
        if any(k in text for k in ["Item Dimensions", "Produktabmessungen",
                                     "Artikelabmessungen", "Package Dimensions"]):
            dim_m = re.search(r"[\d,.]+\s*x\s*[\d,.]+(?:\s*x\s*[\d,.]+)?(?:\s*(?:inches|cm|mm|Zoll|zoll))?", text, re.I)
            if dim_m:
                d["item_dimensions"] = dim_m.group(0).strip()

        # 产地
        if any(k in text for k in ["Country of Origin", "Herkunftsland", "原産国"]):
            parts = re.split(r"[:‏‎]+", text)
            if len(parts) >= 2:
                d["country_of_origin"] = parts[-1].strip()

    # 变体数
    variants = soup.select("#twister_feature_div li[data-defaultasin]")
    if variants:
        d["variant_option_count"] = len(variants)

    # 其他卖家
    olp = soup.select_one("#olp_feature_div, #aod-offer-list")
    if olp:
        om = re.search(r"(\d+)\s+(?:new|neu|nouveau)", olp.get_text(), re.I)
        if om:
            d["other_sellers_count"] = int(om.group(1))

    # 运费
    delivery_el = soup.select_one("#mir-layout-DELIVERY_BLOCK, #deliveryBlockMessage")
    if delivery_el:
        dtxt = delivery_el.get_text(" ", strip=True)
        if re.search(r"\bFREE\b|Kostenlose|KOSTENLOS", dtxt, re.I):
            d["shipping_fee"] = "FREE"
            d["shipping_fee_value"] = 0.0
        else:
            fee_m = re.search(
                r"(?:für|for|:)\s*([\d,.]+)\s*(?:\xa0)?([€$£])|([€$£])\s*([\d,.]+)", dtxt)
            if fee_m:
                raw = (fee_m.group(1) or fee_m.group(4)).replace(",", ".")
                try:
                    d["shipping_fee_value"] = float(raw)
                    d["shipping_fee"] = fee_m.group(0).strip()
                except ValueError:
                    pass

    # 配送模式
    for sel in ("#merchant-info", "#merchantInfoFeature",
                ".offer-display-feature-text", "#tabular-buybox"):
        mel = soup.select_one(sel)
        if mel:
            mtxt = mel.get_text(" ", strip=True)
            if re.search(r"Fulfilled by Amazon|Versand durch Amazon|Expédié par Amazon|Amazonが発送", mtxt, re.I):
                d["fulfillment_type"] = "FBA"
            else:
                d["fulfillment_type"] = "FBM"
            break

    return d


def _check_detail_filters(detail: dict, filters: dict) -> bool:
    """检查详情页字段是否满足筛选条件。返回True=通过，False=不符合。"""
    def _range_check(val, fmin_key, fmax_key):
        fmin = filters.get(fmin_key, 0)
        fmax = filters.get(fmax_key, 0)
        if val is None:
            return True
        if fmin and val < fmin:
            return False
        if fmax and val > fmax:
            return False
        return True

    if not _range_check(detail.get("bsr_main_rank"), "bsr_main_min", "bsr_main_max"):
        return False
    if not _range_check(detail.get("bsr_sub_rank"), "bsr_sub_min", "bsr_sub_max"):
        return False
    if not _range_check(detail.get("variant_option_count"), "variant_min", "variant_max"):
        return False
    if not _range_check(detail.get("other_sellers_count"), "sellers_min", "sellers_max"):
        return False

    # 重量 (解析数值，统一为 lb)
    wmin = filters.get("weight_min", 0)
    wmax = filters.get("weight_max", 0)
    if wmin or wmax:
        w = detail.get("item_weight")
        if w:
            wm = re.search(r"([\d,.]+)", w)
            if wm:
                wv = float(wm.group(1).replace(",", "."))
                if "kg" in w.lower() or "kilogramm" in w.lower():
                    wv *= 2.205
                elif "ounce" in w.lower() or "oz" in w.lower():
                    wv /= 16
                elif "gramm" in w.lower() or (" g" in w.lower() and "kg" not in w.lower()):
                    wv *= 0.0022
                if wmin and wv < wmin:
                    return False
                if wmax and wv > wmax:
                    return False

    # 尺寸
    dl = filters.get("dim_l", 0)
    dw = filters.get("dim_w", 0)
    dh = filters.get("dim_h", 0)
    if dl or dw or dh:
        dims_raw = detail.get("item_dimensions", "")
        if dims_raw:
            nums = [float(x.replace(",", ".")) for x in re.findall(r"[\d,.]+", dims_raw)]
            if "cm" in dims_raw.lower():
                nums = [n / 2.54 for n in nums]
            nums.sort(reverse=True)
            while len(nums) < 3:
                nums.append(0)
            if dl and nums[0] > dl:
                return False
            if dw and nums[1] > dw:
                return False
            if dh and nums[2] > dh:
                return False

    # 运费
    sf = filters.get("shipping_fee", "")
    if sf == "free" and detail.get("shipping_fee_value") is not None and detail["shipping_fee_value"] > 0:
        return False
    if sf == "paid" and detail.get("shipping_fee_value") is not None and detail["shipping_fee_value"] == 0:
        return False
    if sf == "custom":
        sop = filters.get("shipping_op", "lte")
        sval = filters.get("shipping_val", 0)
        sfv = detail.get("shipping_fee_value")
        if sfv is not None and sval > 0:
            if sop == "lte" and sfv > sval:
                return False
            if sop == "gte" and sfv < sval:
                return False

    # 配送模式
    ft = filters.get("fulfillment_type", "")
    if ft and detail.get("fulfillment_type") and detail["fulfillment_type"] != ft:
        return False

    # 产地
    country = filters.get("country", "")
    if country and detail.get("country_of_origin"):
        if country.lower() not in detail["country_of_origin"].lower():
            return False

    # 上架日期
    date_range = filters.get("date_range", "")
    if date_range and detail.get("date_first_available"):
        try:
            dfa = datetime.strptime(detail["date_first_available"], "%Y-%m-%d")
            if date_range == "custom":
                df = filters.get("date_from", "")
                dt = filters.get("date_to", "")
                if df and dfa < datetime.strptime(df, "%Y-%m-%d"):
                    return False
                if dt and dfa > datetime.strptime(dt, "%Y-%m-%d"):
                    return False
            else:
                days = int(date_range)
                if (datetime.now() - dfa).days > days:
                    return False
        except (ValueError, TypeError):
            pass

    return True


def enrich_with_details(products: list, session: requests.Session,
                        delay: float, filters: dict = None):
    """对列表页抓到的商品逐个请求详情页，补全字段并 UPDATE 到数据库。
    不符合筛选条件的商品从数据库删除。"""
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
                continue
            detail = parse_detail_fields(r.text)
            if not detail:
                continue
            detail["detail_scraped"] = 1

            # 详情页筛选
            if filters and not _check_detail_filters(detail, filters):
                with _db_lock:
                    conn = db_conn()
                    try:
                        conn.execute("DELETE FROM product_sightings WHERE asin=?", (asin,))
                        conn.commit()
                    finally:
                        conn.close()
                with _stats_lock:
                    _stats["products_saved"] -= 1
                continue

            # UPDATE DB
            sets = ", ".join(f"{k}=?" for k in detail)
            vals = list(detail.values()) + [asin]
            with _db_lock:
                conn = db_conn()
                try:
                    conn.execute(f"UPDATE product_sightings SET {sets} WHERE asin=?", vals)
                    conn.commit()
                finally:
                    conn.close()
            p.update(detail)
        except (KeyError, TypeError, ValueError, AttributeError) as e:
            _log.error(f"  [detail] {asin} 解析异常: {e}\n{traceback.format_exc()}")
        except Exception as e:
            _log.error(f"  [detail] {asin} 未知异常: {e}\n{traceback.format_exc()}")
        time.sleep(delay + random.uniform(delay * 0.3, delay * 0.8))


# ── Worker 主循环 ───────────────────────────────────────────────────

def process_node(node: dict, lists: list, review_max: int,
                 min_list_size: int, session: requests.Session,
                 price_min: float = 0.0, price_max: float = 0.0,
                 review_min: int = 0,
                 rating_min: float = 0.0, rating_max: float = 0.0,
                 max_pages: int = 2, delay: float = 2.0,
                 detail_filters: dict = None):
    """处理单个节点的所有榜单。"""
    node_id = node["node_id"]
    slug    = extract_slug(node["url"])
    name    = node["name"]
    depth   = node["depth"]

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

        all_products = parse_products(
            html, node_id, name, slug, depth,
            list_type, total, review_max, price_min, price_max,
            review_min, rating_min, rating_max
        )

        for pg in range(2, max_pages + 1):
            rp = _safe_get(session, url_base + f"?pg={pg}", referer=url_base)
            if rp is None or rp.status_code != 200:
                break
            all_products += parse_products(
                rp.text, node_id, name, slug, depth,
                list_type, total, review_max, price_min, price_max,
                review_min, rating_min, rating_max
            )
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
              detail_filters: dict = None):
    """主入口：单线程顺序抓取。从最深层类目开始，逐层向上。"""
    if slugs:
        nodes = get_nodes_by_slugs(slugs, lists)
    else:
        nodes = get_descendant_nodes(root_ids, lists)
    _stats["total_nodes"] = len(nodes)
    _stats["products_dup"] = 0
    with _seen_lock:
        _seen_asins.clear()

    if not nodes:
        _log.warning("[fetch_products] 无目标节点，退出")
        return

    _validate_proxy_pool()
    session = _make_session()

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
                     detail_filters)
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
    parser.add_argument("--list-total-min", type=int, default=0)
    parser.add_argument("--list-total-max", type=int, default=0)
    parser.add_argument("--shipping-fee", default="")
    parser.add_argument("--shipping-op",  default="lte")
    parser.add_argument("--shipping-val", type=float, default=0)
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
    args = parser.parse_args()

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
        "shipping_fee": args.shipping_fee, "shipping_op": args.shipping_op,
        "shipping_val": args.shipping_val,
        "fulfillment_type": args.fulfillment_type,
        "country": args.country,
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
    )

