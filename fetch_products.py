"""
fetch_products.py — 商品抓取脚本（独立进程）
从 categories.db 读取有效节点，抓取 3 个 SSR 榜单的商品数据
用法: python fetch_products.py
"""
import sqlite3, requests, threading, time, sys, os, re, json, argparse
from queue import Queue, Empty
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── 配置 ────────────────────────────────────────────────────────────
BASE    = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "data", "categories.db")

# 默认值（可被看板 API 参数覆盖）
DEFAULT_REVIEW_MAX    = 10
DEFAULT_MIN_LIST_SIZE = 100
DEFAULT_PRICE_MIN     = 0.0
DEFAULT_PRICE_MAX     = 0.0
DEFAULT_DELAY         = 2.0   # 请求间隔（秒）
DEFAULT_LISTS         = ["new-releases", "bestsellers", "most-wished-for"]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml",
}

LIST_LABELS = {
    "new-releases": "新品榜",
    "bestsellers": "畅销榜",
    "most-wished-for": "心愿单",
}

_db_lock = threading.Lock()
_stats = {"total_nodes": 0, "done_nodes": 0, "skipped": 0,
          "products_found": 0, "products_saved": 0, "errors": 0}
_stats_lock = threading.Lock()


# ── DB 工具 ─────────────────────────────────────────────────────────

def db_conn():
    c = sqlite3.connect(DB_PATH, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def get_descendant_nodes(root_ids: list, lists: list) -> list:
    """根据选中的根节点 node_id，查出所有后代叶子节点。"""
    conn = db_conn()
    # 先取根节点的 URL 前缀
    placeholders = ",".join("?" * len(root_ids))
    roots = conn.execute(
        f"SELECT node_id, url FROM categories WHERE node_id IN ({placeholders})",
        root_ids
    ).fetchall()

    if not roots:
        conn.close()
        return []

    # 用 URL 前缀 LIKE 查所有后代
    like_clauses = []
    for r in roots:
        prefix = r["url"].rstrip("/") + "/"
        like_clauses.append(f"url LIKE '{prefix}%'")
    # 也包含根节点自身
    like_clauses.append(f"node_id IN ({placeholders})")

    valid_map = {
        "new-releases": "nr_valid=1",
        "bestsellers": "bs_valid=1",
        "most-wished-for": "mw_valid=1",
    }
    valid_clauses = " OR ".join(valid_map[l] for l in lists if l in valid_map)

    sql = f"""
        SELECT node_id, url, name, depth FROM categories
        WHERE node_id IS NOT NULL
          AND ({" OR ".join(like_clauses)})
          AND ({valid_clauses})
        ORDER BY depth, name
    """
    rows = conn.execute(sql, root_ids).fetchall()
    conn.close()
    result = [dict(r) for r in rows]
    print(f"[fetch_products] 选中 {len(root_ids)} 个根节点 → {len(result)} 个后代节点")
    return result


def extract_slug(url: str) -> str:
    """从类目 URL 提取 slug。"""
    parts = url.rstrip("/").split("/")
    try:
        gp_idx = parts.index("gp")
        return parts[gp_idx + 2]
    except (ValueError, IndexError):
        return ""


def save_products(products: list):
    """批量写入 product_sightings 表（去重 UPSERT）。"""
    if not products:
        return 0
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


# ── HTML 解析 ───────────────────────────────────────────────────────

def extract_list_total(html: str) -> int:
    """从页面提取榜单商品总数。匹配 '1-50 of 87 results' 或 'of 100'。"""
    m = re.search(r'of\s+([\d,]+)\s+results?', html, re.IGNORECASE)
    if m:
        return int(m.group(1).replace(",", ""))
    # 备选：找 zg 标题区域的数字
    m2 = re.search(r'showing\s+\d+\s*-\s*\d+\s+of\s+([\d,]+)', html, re.IGNORECASE)
    if m2:
        return int(m2.group(1).replace(",", ""))
    return 0


def parse_products(html: str, node_id: str, category_name: str,
                   category_slug: str, category_depth: int,
                   list_type: str, list_total: int,
                   review_max: int,
                   price_min: float = 0.0,
                   price_max: float = 0.0) -> list:
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
        p["product_url"] = ("https://www.amazon.com" + href) if href.startswith("/") else href

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
                p["price"] = float(m_price.group().replace(",", ""))

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
            m_rt = re.search(r"([\d.]+)\s+out", rt)
            if m_rt:
                p["rating"] = float(m_rt.group(1))

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
        if rc >= review_max:
            continue

        # ── 价格筛选 ──
        price = p.get("price")
        if price is not None:
            if price_min > 0 and price < price_min:
                continue
            if price_max > 0 and price > price_max:
                continue

        # 来源信息
        p["node_id"] = node_id
        p["category_name"] = category_name
        p["category_slug"] = category_slug
        p["category_depth"] = category_depth
        p["list_type"] = list_type
        p["list_total"] = list_total

        products.append(p)

    return products


print("[fetch_products] 模块加载完成", flush=True)


# ── Worker 主循环 ───────────────────────────────────────────────────

def process_node(node: dict, lists: list, review_max: int,
                 min_list_size: int, session: requests.Session,
                 price_min: float = 0.0, price_max: float = 0.0,
                 delay: float = 2.0):
    """处理单个节点的所有榜单。"""
    node_id = node["node_id"]
    slug    = extract_slug(node["url"])
    name    = node["name"]
    depth   = node["depth"]

    for list_type in lists:
        # 检查该节点对应榜单是否有效（从 URL 前缀构建）
        url_p1 = f"https://www.amazon.com/gp/{list_type}/{slug}/{node_id}/"

        try:
            r = session.get(url_p1, timeout=15)
        except Exception as e:
            with _stats_lock:
                _stats["errors"] += 1
            continue

        if r.status_code != 200:
            continue

        time.sleep(delay)

        html = r.text

        # ── 活体检测 ──
        total = extract_list_total(html)
        if total < min_list_size:
            with _stats_lock:
                _stats["skipped"] += 1
            continue

        # ── 解析 page 1 ──
        page1_products = parse_products(
            html, node_id, name, slug, depth,
            list_type, total, review_max, price_min, price_max
        )

        # ── 请求 page 2 ──
        page2_products = []
        try:
            r2 = session.get(url_p1 + "?pg=2", timeout=15)
            if r2.status_code == 200:
                page2_products = parse_products(
                    r2.text, node_id, name, slug, depth,
                    list_type, total, review_max, price_min, price_max
                )
            time.sleep(delay)
        except Exception:
            pass

        all_products = page1_products + page2_products

        with _stats_lock:
            _stats["products_found"] += len(all_products)

        if all_products:
            saved = save_products(all_products)
            with _stats_lock:
                _stats["products_saved"] += saved

    with _stats_lock:
        _stats["done_nodes"] += 1


def run_batch(root_ids: list, lists: list, review_max: int,
              min_list_size: int, delay: float = 2.0,
              price_min: float = 0.0, price_max: float = 0.0):
    """主入口：单线程顺序抓取。"""
    nodes = get_descendant_nodes(root_ids, lists)
    _stats["total_nodes"] = len(nodes)

    if not nodes:
        print("[fetch_products] 无目标节点，退出", flush=True)
        return

    session = requests.Session()
    session.headers.update(HEADERS)

    t0 = time.time()
    price_info = ""
    if price_min > 0 or price_max > 0:
        price_info = f" 价格${price_min:.0f}-${price_max:.0f}" if price_max > 0 else f" 价格>${price_min:.0f}"
    print(f"[fetch_products] 开始抓取: {len(nodes)} 节点 × {len(lists)} 榜单, "
          f"评论<{review_max}, 最少{min_list_size}商品{price_info}, 延迟{delay}s",
          flush=True)

    for node in nodes:
        process_node(node, lists, review_max, min_list_size, session,
                     price_min, price_max, delay)
        n = _stats["done_nodes"]
        total = _stats["total_nodes"]
        if n % 10 == 0 or n == total:
            elapsed = time.time() - t0
            rate = n / elapsed if elapsed > 0 else 0
            print(f"  [{n}/{total}] {rate:.1f}节点/s "
                  f"找到:{_stats['products_found']} "
                  f"录入:{_stats['products_saved']} "
                  f"跳过:{_stats['skipped']}", flush=True)

    elapsed = time.time() - t0
    print(f"\n[fetch_products] 完成！")
    print(f"  耗时: {elapsed:.0f}s")
    print(f"  节点: {_stats['done_nodes']}/{_stats['total_nodes']}")
    print(f"  找到: {_stats['products_found']} 个符合条件商品")
    print(f"  录入: {_stats['products_saved']} 条（去重后）")
    print(f"  跳过: {_stats['skipped']} 个冷门榜单")
    print(f"  错误: {_stats['errors']}", flush=True)

    export_excel()


def export_excel():
    """导出去重后的商品到 Excel。"""
    try:
        import openpyxl
    except ImportError:
        print("[fetch_products] 需要 openpyxl: pip install openpyxl", flush=True)
        return

    conn = db_conn()
    # 按 ASIN 聚合，记录出现过的榜单
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
        print("[fetch_products] 无数据可导出", flush=True)
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
    print(f"[fetch_products] Excel 已导出: {excel_path} ({len(rows)} 行)", flush=True)


# ── CLI 入口 ────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Amazon 榜单商品抓取")
    parser.add_argument("--roots", nargs="+", required=True,
                        help="根节点 node_id 列表")
    parser.add_argument("--review-max", type=int, default=DEFAULT_REVIEW_MAX)
    parser.add_argument("--min-list",   type=int, default=DEFAULT_MIN_LIST_SIZE)
    parser.add_argument("--price-min",  type=float, default=DEFAULT_PRICE_MIN)
    parser.add_argument("--price-max",  type=float, default=DEFAULT_PRICE_MAX)
    parser.add_argument("--delay",      type=float, default=DEFAULT_DELAY)
    parser.add_argument("--lists", nargs="+", default=DEFAULT_LISTS)
    args = parser.parse_args()

    run_batch(
        root_ids=args.roots,
        lists=args.lists,
        review_max=args.review_max,
        min_list_size=args.min_list,
        delay=args.delay,
        price_min=args.price_min,
        price_max=args.price_max,
    )

