"""
fetch_new_arrivals.py — 最新到货商品抓取（多 worker 并发）

走 Amazon 搜索接口 /s?rh=n:{node_id}&s=date-desc-rank 抓取按上架时间排序的商品列表，
然后进详情页提取 BSR / 上架时间 / review 数等字段，筛选信号新品。

两阶段：
  Phase 1: 搜索列表页 → 提取 ASIN（多页翻页）
  Phase 2: 详情页 → 提取 BSR / 上架时间 / review → 过滤信号产品 → 入库

用法:
  python fetch_new_arrivals.py --site DE                          # DE 全部类目
  python fetch_new_arrivals.py --site DE --roots 16435051         # 指定根节点
  python fetch_new_arrivals.py --site DE --depth 3 4 5            # 只跑 L3-L5
  python fetch_new_arrivals.py --site DE --max-pages 5            # 每类目最多5页
  python fetch_new_arrivals.py --site DE --phase1-only            # 仅跑列表页收集ASIN
"""

import json, os, re, sys, time, random, sqlite3, threading, argparse
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import requests, urllib3
urllib3.disable_warnings()
from bs4 import BeautifulSoup

from config import (
    HEADERS, DATA_DIR, DB_FILE, PROXY_POOL_FILE,
    PROXY_ENABLED, PROXY_VERIFY, get_marketplace,
)

# ── 运行时站点配置 ──
_mp = get_marketplace("US")
_SITE = "US"
_DOMAIN = _mp["domain"]
_LANG = _mp["lang"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ── 过滤阈值 ──
MAX_LISTING_AGE_DAYS = 30
MAX_REVIEW_COUNT_LIST = 50
MAX_BSR_MAIN_RANK = 200000
MAX_PAGES_PER_NODE = 10


def _update_thresholds(max_age, max_bsr, review_max_list):
    global MAX_LISTING_AGE_DAYS, MAX_BSR_MAIN_RANK, MAX_REVIEW_COUNT_LIST
    MAX_LISTING_AGE_DAYS = max_age
    MAX_BSR_MAIN_RANK = max_bsr
    MAX_REVIEW_COUNT_LIST = review_max_list


# 德语月份映射
DE_MONTHS = {
    "Januar": 1, "Februar": 2, "März": 3, "April": 4,
    "Mai": 5, "Juni": 6, "Juli": 7, "August": 8,
    "September": 9, "Oktober": 10, "November": 11, "Dezember": 12,
}

# 日语月份不需要映射，格式为 2024/6/13

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 代理池（复用 fetch_subtree 的 ProxyPool 模式）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ProxyPool:
    def __init__(self):
        self._q = Queue()
        self._all = []
        if PROXY_ENABLED and os.path.exists(PROXY_POOL_FILE):
            with open(PROXY_POOL_FILE, encoding="utf-8") as f:
                entries = json.load(f)
            for p in entries:
                self._q.put(p)
                self._all.append(p)
            print(f"[pool] 加载 {len(entries)} 个代理端口")
        else:
            print("[pool] 代理未启用，使用直连")

    @property
    def size(self):
        return len(self._all)

    def acquire(self, timeout=30):
        return self._q.get(timeout=timeout)

    def release(self, entry):
        self._q.put(entry)


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
    image_url       TEXT,
    product_url     TEXT,
    node_id         TEXT,
    category_name   TEXT,
    category_depth  INTEGER,
    site            TEXT DEFAULT 'US',
    is_signal       INTEGER DEFAULT 0,
    scraped_at      TEXT DEFAULT (datetime('now')),
    UNIQUE(asin, node_id, site)
)
"""


def _init_db():
    conn = sqlite3.connect(DB_FILE, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()
    conn.close()


def _save_products(products: list) -> int:
    if not products:
        return 0
    sql = """
        INSERT OR IGNORE INTO new_arrivals
        (asin, title, price, price_value, rating, review_count,
         listing_date, listing_age_days,
         bsr_main_category, bsr_main_rank, bsr_sub,
         image_url, product_url,
         node_id, category_name, category_depth, site, is_signal)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    saved = 0
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=15)
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            for p in products:
                try:
                    conn.execute(sql, (
                        p["asin"], p.get("title"), p.get("price"),
                        p.get("price_value"), p.get("rating"),
                        p.get("review_count", 0),
                        p.get("listing_date"), p.get("listing_age_days"),
                        p.get("bsr_main_category"), p.get("bsr_main_rank"),
                        json.dumps(p.get("bsr_sub", []), ensure_ascii=False) if p.get("bsr_sub") else None,
                        p.get("image_url"), p.get("product_url"),
                        p["node_id"], p.get("category_name"),
                        p.get("category_depth"), p["site"],
                        1 if p.get("is_signal") else 0,
                    ))
                    saved += 1
                except sqlite3.IntegrityError:
                    pass
            conn.commit()
        finally:
            conn.close()
    return saved


def _load_nodes(site: str, depths: list[int] | None = None,
                root_ids: list[str] | None = None) -> list[dict]:
    conn = sqlite3.connect(DB_FILE, timeout=15)
    conn.row_factory = sqlite3.Row
    if root_ids:
        ph = ",".join("?" * len(root_ids))
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
            "SELECT node_id, name, depth FROM categories WHERE site = ? AND depth > 0 ORDER BY depth, name",
            (site,)
        ).fetchall()
    conn.close()
    result = [{"node_id": r["node_id"], "name": r["name"], "depth": r["depth"]} for r in rows]
    if depths:
        result = [n for n in result if n["depth"] in depths]
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# HTTP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _make_session(worker_id: int, proxy_entry: dict | None) -> requests.Session:
    session = requests.Session()
    ua = USER_AGENTS[worker_id % len(USER_AGENTS)]
    session.headers.update({
        **HEADERS,
        "User-Agent": ua,
        "Accept-Language": _LANG,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    })
    if proxy_entry:
        session.proxies.update({"http": proxy_entry["proxy"], "https": proxy_entry["proxy"]})
    return session


def _safe_get(session: requests.Session, url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=18, verify=PROXY_VERIFY)
            if r.status_code == 200:
                if "captcha" in r.text.lower() or "Type the characters" in r.text:
                    print(f"    [CAPTCHA] 等待 30s 后重试", flush=True)
                    time.sleep(30 + random.uniform(0, 15))
                    continue
                return r.text
            if r.status_code == 429:
                wait = 60 + random.uniform(0, 30)
                print(f"    [429] 限速 {wait:.0f}s", flush=True)
                time.sleep(wait)
            elif r.status_code == 503:
                time.sleep(15 + random.uniform(0, 10))
            else:
                return None
        except requests.RequestException as e:
            wait = 5 * (2 ** attempt) + random.uniform(0, 3)
            time.sleep(wait)
    return None


def _warmup(session: requests.Session):
    try:
        session.get(f"{_DOMAIN}/", timeout=10, verify=PROXY_VERIFY)
        time.sleep(1 + random.uniform(0, 1))
    except Exception:
        pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Phase 1: 搜索列表页解析
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _build_search_url(node_id: str, page: int = 1) -> str:
    url = f"{_DOMAIN}/s?rh=n%3A{node_id}&s=date-desc-rank"
    if page > 1:
        url += f"&page={page}"
    return url


def _parse_listing_page(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    if "captcha" in html.lower() or "Klicke auf die Schaltfläche" in html:
        return {"status": "captcha", "asins": [], "has_next": False}

    cards = soup.select('[data-component-type="s-search-result"]')
    asins = []
    for card in cards:
        asin = card.get("data-asin", "").strip()
        if not asin:
            continue
        # 粗过滤: review 数过大的老产品直接跳过
        if MAX_REVIEW_COUNT_LIST:
            review_count = 0
            # 格式1: 链接文本 "(123)"
            review_link = card.select_one('a[href*="customerReviews"], a[href*="#reviews"]')
            if review_link:
                m = re.search(r"([\d,.]+)", review_link.get_text(strip=True))
                if m:
                    try:
                        review_count = int(m.group(1).replace(",", "").replace(".", ""))
                    except ValueError:
                        pass
            # 格式2: span
            if review_count == 0:
                review_el = card.select_one(".a-size-base.s-underline-text")
                if review_el:
                    m = re.search(r"([\d,.]+)", review_el.get_text(strip=True))
                    if m:
                        try:
                            review_count = int(m.group(1).replace(",", "").replace(".", ""))
                        except ValueError:
                            pass
            if review_count > MAX_REVIEW_COUNT_LIST:
                continue
        asins.append(asin)

    has_next = bool(soup.select_one(".s-pagination-next:not(.s-pagination-disabled)"))
    return {"status": "ok", "asins": asins, "has_next": has_next}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Phase 2: 详情页解析
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _parse_detail_page(html: str, asin: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")

    title_el = soup.select_one("#productTitle")
    title = title_el.get_text(strip=True) if title_el else ""

    price = ""
    price_value = None
    price_el = soup.select_one("#corePrice_feature_div .a-offscreen, .a-price .a-offscreen")
    if price_el:
        price = price_el.get_text(strip=True)
        m = re.search(r"[\d,.]+", price)
        if m:
            ps = m.group().replace(".", "").replace(",", ".")
            try:
                price_value = float(ps)
            except ValueError:
                pass

    review_count = 0
    review_el = soup.select_one("#acrCustomerReviewText")
    if review_el:
        m = re.search(r"([\d,.]+)", review_el.get_text())
        if m:
            try:
                review_count = int(m.group(1).replace(",", "").replace(".", ""))
            except ValueError:
                pass

    rating = None
    rating_el = soup.select_one("#acrPopover .a-icon-alt")
    if rating_el:
        m = re.search(r"([\d,\.]+)", rating_el.get_text())
        if m:
            try:
                rating = float(m.group(1).replace(",", "."))
            except ValueError:
                pass

    image_url = ""
    img_el = soup.select_one("#landingImage, #imgBlkFront")
    if img_el:
        image_url = img_el.get("data-old-hires") or img_el.get("src", "")

    # ── 上架时间 ──
    listing_date = None
    listing_age_days = None
    detail_rows = soup.select(
        "#detailBullets_feature_div li, "
        "#productDetails_techSpec_section_1 tr, "
        "#productDetails_detailBullets_sections1 tr, "
        "#prodDetails tr"
    )
    for row in detail_rows:
        text = row.get_text(" ", strip=True)
        if not any(k in text for k in [
            "Datum der Ersten", "Date First Available", "Erstmals verfügbar",
            "Date de mise en ligne", "発売日", "この商品の最初のレビュー投稿日",
        ]):
            continue
        # 德语格式: 13. Juni 2026
        dm = re.search(
            r"(\d{1,2})\.\s*(Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s*(\d{4})",
            text
        )
        if dm:
            try:
                listing_date = datetime(int(dm.group(3)), DE_MONTHS[dm.group(2)], int(dm.group(1)))
            except (ValueError, KeyError):
                pass
        # 英语格式: June 13, 2026
        if not listing_date:
            em = re.search(
                r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
                text
            )
            if em:
                try:
                    listing_date = datetime.strptime(f"{em.group(1)} {em.group(2)} {em.group(3)}", "%B %d %Y")
                except ValueError:
                    pass
        # 日语格式: 2024/6/13
        if not listing_date:
            jm = re.search(r"(\d{4})/(\d{1,2})/(\d{1,2})", text)
            if jm:
                try:
                    listing_date = datetime(int(jm.group(1)), int(jm.group(2)), int(jm.group(3)))
                except ValueError:
                    pass
        if listing_date:
            listing_age_days = (datetime.now() - listing_date).days
            break

    # ── BSR ──
    bsr_main = None
    bsr_sub = []
    bsr_section = (
        soup.select_one("#prodDetails")
        or soup.select_one("#detailBulletsWrapper_feature_div")
        or soup.select_one("#productDetails_db_sections")
    )
    if bsr_section:
        bsr_text = bsr_section.get_text(" ")
        patterns = [
            r"Nr\.\s*([\d\.]+)\s+in\s+([^\(#\n]+)",
            r"#([\d,]+)\s+in\s+([^\(#\n]+)",
        ]
        matches = []
        for pattern in patterns:
            for m in re.finditer(pattern, bsr_text):
                rank_str = m.group(1).replace(".", "").replace(",", "")
                category = m.group(2).strip().rstrip("( ")
                try:
                    rank = int(rank_str)
                    matches.append((category, rank))
                except ValueError:
                    pass
        if matches:
            bsr_main = matches[0]
            bsr_sub = matches[1:]

    # ── 过滤 ──
    if bsr_main is None and not bsr_sub:
        return None
    if listing_age_days is not None and listing_age_days > MAX_LISTING_AGE_DAYS:
        return None
    if bsr_main and bsr_main[1] > MAX_BSR_MAIN_RANK:
        return None

    is_signal = (
        listing_age_days is not None
        and listing_age_days <= 7
        and review_count < 10
        and (not bsr_main or bsr_main[1] <= 50000)
    )

    return {
        "asin": asin,
        "title": title,
        "price": price,
        "price_value": price_value,
        "rating": rating,
        "review_count": review_count,
        "listing_date": listing_date.strftime("%Y-%m-%d") if listing_date else None,
        "listing_age_days": listing_age_days,
        "bsr_main_category": bsr_main[0] if bsr_main else None,
        "bsr_main_rank": bsr_main[1] if bsr_main else None,
        "bsr_sub": bsr_sub if bsr_sub else None,
        "image_url": image_url,
        "product_url": f"{_DOMAIN}/dp/{asin}",
        "is_signal": is_signal,
    }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Worker
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

lock = threading.Lock()
stats = {
    "nodes_done": 0, "nodes_total": 0,
    "asins_found": 0, "asins_unique": 0,
    "details_ok": 0, "details_filtered": 0, "details_error": 0,
    "signals": 0, "saved": 0,
    "captcha": 0, "p1_error": 0,
}
all_asins = {}  # asin -> {"node_id": ..., "name": ..., "depth": ...}


def _worker_phase1(worker_id: int, task_q: Queue, pool: ProxyPool,
                   max_pages: int):
    proxy_entry = pool.acquire() if pool.size > 0 else None
    session = _make_session(worker_id, proxy_entry)
    _warmup(session)

    while True:
        try:
            node = task_q.get(timeout=3)
        except Empty:
            break

        node_id = node["node_id"]
        node_asins = []

        for page in range(1, max_pages + 1):
            url = _build_search_url(node_id, page)
            html = _safe_get(session, url)
            time.sleep(random.uniform(1.5, 3.0))

            if html is None:
                with lock:
                    stats["p1_error"] += 1
                break

            parsed = _parse_listing_page(html)
            if parsed["status"] == "captcha":
                with lock:
                    stats["captcha"] += 1
                break

            node_asins.extend(parsed["asins"])
            if not parsed["has_next"]:
                break

        with lock:
            stats["nodes_done"] += 1
            stats["asins_found"] += len(node_asins)
            for asin in node_asins:
                if asin not in all_asins:
                    all_asins[asin] = {
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
    proxy_entry = pool.acquire() if pool.size > 0 else None
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

        product = _parse_detail_page(html, asin)
        with lock:
            if product is None:
                stats["details_filtered"] += 1
            else:
                stats["details_ok"] += 1
                if product["is_signal"]:
                    stats["signals"] += 1
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
    print(f"  [P1 {n}/{t}] ASIN总={stats['asins_found']} "
          f"去重={stats['asins_unique']} captcha={stats['captcha']} "
          f"err={stats['p1_error']}", flush=True)


def _print_p2_progress():
    done = stats["details_ok"] + stats["details_filtered"] + stats["details_error"]
    total = stats["asins_unique"]
    print(f"  [P2 {done}/{total}] 命中={stats['details_ok']} "
          f"过滤={stats['details_filtered']} 信号={stats['signals']} "
          f"入库={stats['saved']} err={stats['details_error']}", flush=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Main
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def main():
    global _mp, _SITE, _DOMAIN, _LANG

    parser = argparse.ArgumentParser(description="Amazon 最新到货商品抓取")
    parser.add_argument("--site", default="DE", help="站点代码: US, DE, JP, UK, FR")
    parser.add_argument("--roots", nargs="+", help="根节点 node_id（默认全部类目）")
    parser.add_argument("--depth", nargs="+", type=int, help="只跑指定层级")
    parser.add_argument("--max-pages", type=int, default=MAX_PAGES_PER_NODE,
                        help=f"每类目最大翻页数（默认{MAX_PAGES_PER_NODE}）")
    parser.add_argument("--max-age", type=int, default=MAX_LISTING_AGE_DAYS,
                        help=f"上架天数上限（默认{MAX_LISTING_AGE_DAYS}）")
    parser.add_argument("--max-bsr", type=int, default=MAX_BSR_MAIN_RANK,
                        help=f"大类BSR上限（默认{MAX_BSR_MAIN_RANK}）")
    parser.add_argument("--review-max-list", type=int, default=MAX_REVIEW_COUNT_LIST,
                        help=f"列表页review数粗过滤（默认{MAX_REVIEW_COUNT_LIST}）")
    parser.add_argument("--phase1-only", action="store_true", help="仅跑P1收集ASIN")
    parser.add_argument("--sample", type=int, default=0, help="只测试N个节点")
    args = parser.parse_args()

    _SITE = args.site.upper()
    _mp = get_marketplace(_SITE)
    _DOMAIN = _mp["domain"]
    _LANG = _mp["lang"]

    _update_thresholds(args.max_age, args.max_bsr, args.review_max_list)

    _init_db()
    pool = ProxyPool()
    num_workers = max(pool.size, 1)

    nodes = _load_nodes(_SITE, depths=args.depth, root_ids=args.roots)
    if args.sample > 0:
        random.shuffle(nodes)
        nodes = nodes[:args.sample]
    stats["nodes_total"] = len(nodes)

    print(f"=== 最新到货抓取 [{_mp['name']}] ===")
    print(f"节点数: {len(nodes)}, Worker数: {num_workers}, 最大翻页: {args.max_pages}")
    print(f"过滤: 上架≤{MAX_LISTING_AGE_DAYS}天, BSR≤{MAX_BSR_MAIN_RANK}, 列表review≤{MAX_REVIEW_COUNT_LIST}")
    print()

    # ── Phase 1: 搜索列表页 ──
    print("━━━ Phase 1: 搜索列表页收集 ASIN ━━━", flush=True)
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
    print(f"\n[P1 完成] {p1_time:.0f}s — 节点={stats['nodes_done']}, "
          f"ASIN总={stats['asins_found']}, 去重={stats['asins_unique']}, "
          f"captcha={stats['captcha']}", flush=True)

    if args.phase1_only or not all_asins:
        print("[结束] phase1-only 模式或无 ASIN")
        return

    # ── Phase 2: 详情页（降并发，详情页反爬更严） ──
    p2_workers = min(num_workers, max(num_workers // 2, 4))
    print(f"\n━━━ Phase 2: {len(all_asins)} 个 ASIN 详情页解析 (worker={p2_workers}) ━━━", flush=True)
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
    print(f"\n[P2 完成] {p2_time:.0f}s — 命中={stats['details_ok']}, "
          f"过滤={stats['details_filtered']}, 信号产品={stats['signals']}, "
          f"入库={stats['saved']}")

    print(f"\n=== 总计 {total_time:.0f}s ===")
    print(f"  P1: {stats['nodes_done']}节点 → {stats['asins_unique']} ASIN")
    print(f"  P2: {stats['details_ok']}命中 / {stats['details_filtered']}过滤 / {stats['details_error']}失败")
    print(f"  信号产品: {stats['signals']}")
    print(f"  入库: {stats['saved']} 条")

    _export_summary()


def _export_summary():
    conn = sqlite3.connect(DB_FILE, timeout=15)
    conn.row_factory = sqlite3.Row
    total = conn.execute("SELECT COUNT(*) FROM new_arrivals WHERE site=?", (_SITE,)).fetchone()[0]
    signals = conn.execute("SELECT COUNT(*) FROM new_arrivals WHERE site=? AND is_signal=1", (_SITE,)).fetchone()[0]
    top = conn.execute(
        "SELECT asin, title, price, review_count, listing_age_days, bsr_main_rank, bsr_main_category "
        "FROM new_arrivals WHERE site=? AND is_signal=1 ORDER BY bsr_main_rank ASC LIMIT 20",
        (_SITE,)
    ).fetchall()
    conn.close()

    print(f"\n=== DB 汇总: {total} 条记录, {signals} 个信号产品 ===")
    if top:
        print(f"\nTop 信号产品 (BSR 最优):")
        for r in top:
            age = f"{r['listing_age_days']}天" if r["listing_age_days"] is not None else "?"
            bsr = f"#{r['bsr_main_rank']}" if r["bsr_main_rank"] else "?"
            print(f"  {r['asin']}  {bsr:>8}  {age:>4}  rev={r['review_count']:<4} "
                  f"{r['price'] or '?':>10}  {(r['title'] or '')[:50]}")


if __name__ == "__main__":
    main()
