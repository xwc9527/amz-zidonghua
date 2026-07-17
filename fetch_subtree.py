"""
fetch_subtree.py — 按需抓取类目子树（多 worker 并发 + 端口池）

用法:
  python fetch_subtree.py home-garden              # BFS + 面包屑
  python fetch_subtree.py home-garden --skip-breadcrumb  # 仅 BFS
  python fetch_subtree.py --breadcrumb-only         # 仅面包屑（多 worker 并发）
  python fetch_subtree.py --all                     # 所有 L1 大类
"""
import html as htmlmod
import json, os, re, sys, time, random, sqlite3, threading, argparse
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import requests, urllib3
urllib3.disable_warnings()
from bs4 import BeautifulSoup

from config import (
    HEADERS, DATA_DIR, DB_FILE, PROXY_POOL_FILE,
    PROXY_ENABLED, PROXY_VERIFY, get_marketplace,
)

# L1 根节点可读名回退（页面 zg-selected/h1 解析失败时用，避免把 slug 当展示名）
L1_DISPLAY_NAMES = {
    "US": {
        "appliances": "Appliances",
        "arts-crafts": "Arts, Crafts & Sewing",
        "automotive": "Automotive",
        "baby-products": "Baby",
        "beauty": "Beauty & Personal Care",
        "electronics": "Electronics",
        "hi": "Tools & Home Improvement",
        "home-garden": "Home & Kitchen",
        "kitchen": "Kitchen & Dining",
        "lawn-garden": "Patio, Lawn & Garden",
        "musical-instruments": "Musical Instruments",
        "office-products": "Office Products",
        "pc": "Computers & Accessories",
        "pet-supplies": "Pet Supplies",
        "photo": "Camera & Photo Products",
        "sporting-goods": "Sports & Outdoors",
        "toys-and-games": "Toys & Games",
        "wireless": "Cell Phones & Accessories",
    },
    "DE": {
        "appliances": "Elektro-Großgeräte",
        "automotive": "Auto & Motorrad",
        "baby": "Baby",
        "beauty": "Kosmetik",
        "ce-de": "Elektronik & Foto",
        "computers": "Computer & Zubehör",
        "diy": "Baumarkt",
        "drugstore": "Drogerie & Körperpflege",
        "garden": "Garten",
        "kitchen": "Küche, Haushalt & Wohnen",
        "lighting": "Beleuchtung",
        "musical-instruments": "Musikinstrumente & DJ-Equipment",
        "officeproduct": "Bürobedarf & Schreibwaren",
        "pet-supplies": "Haustier",
        "photo": "Kamera & Foto",
        "sports": "Sport & Freizeit",
        "toys": "Spielzeug",
    },
    "JP": {
        "appliances": "大型家電",
        "automotive": "車＆バイク",
        "baby": "ベビー＆マタニティ",
        "beauty": "ビューティー",
        "computers": "パソコン・周辺機器",
        "diy": "DIY・工具・ガーデン",
        "electronics": "家電＆カメラ",
        "hobby": "ホビー",
        "hpc": "ドラッグストア",
        "kitchen": "ホーム＆キッチン",
        "musical-instruments": "楽器・音響機器",
        "office-products": "文房具・オフィス用品",
        "pet-supplies": "ペット用品",
        "sports": "スポーツ＆アウトドア",
        "toys": "おもちゃ",
    },
}

# 运行时站点配置（由 CLI --site 设置）
_mp     = get_marketplace("US")
_SITE   = "US"
_DOMAIN = _mp["domain"]
_LANG   = _mp["lang"]

CACHE_DIR = os.path.join(DATA_DIR, "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

BFS_BATCH_SIZE = 100
BC_BATCH_SIZE  = 20
CHECKPOINT_FILE = os.path.join(DATA_DIR, "crawl_checkpoint.json")

CHART_PREFIXES = [
    "/gp/new-releases/",
    "/gp/bestsellers/",
    "/gp/movers-and-shakers/",
    "/gp/most-wished-for/",
    "/gp/most-gifted/",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:126.0) Gecko/20100101 Firefox/126.0",
]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 代理端口池
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
            print("[pool] 代理未启用或池文件不存在，使用直连")

    @property
    def size(self):
        return len(self._all)

    def acquire(self, timeout=30):
        try:
            return self._q.get(timeout=timeout)
        except Empty:
            return None

    def release(self, entry):
        if entry:
            self._q.put(entry)

    def all_entries(self):
        return list(self._all)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# DB 工具
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_db_lock = threading.Lock()


def _db_batch_insert(nodes: list[dict]) -> int:
    if not nodes:
        return 0
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        try:
            conn.execute("ALTER TABLE categories ADD COLUMN site TEXT DEFAULT 'US'")
        except sqlite3.OperationalError:
            pass
        added = 0
        for n in nodes:
            cur = conn.execute(
                "INSERT OR IGNORE INTO categories "
                "(name, url, node_id, depth, source, explored, parent_node_id, slug, site) "
                "VALUES(?, ?, ?, ?, ?, 1, ?, ?, ?)",
                (n["name"], normalize_url(n["url"]), n.get("node_id"),
                 n.get("depth", 0), n.get("source", "subtree"),
                 n.get("parent_node_id"), n.get("slug", ""), _SITE)
            )
            added += cur.rowcount
        conn.commit()
        conn.close()
    return added


def _db_mark_bc_checked(node_ids: list[str]):
    if not node_ids:
        return
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        conn.executemany(
            "UPDATE categories SET breadcrumb_checked=1 WHERE node_id=?",
            [(nid,) for nid in node_ids]
        )
        conn.commit()
        conn.close()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 解析工具
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def normalize_url(url: str) -> str:
    url = url.split("?")[0]
    url = re.sub(r"/ref=.*$", "/", url)
    if not url.endswith("/"):
        url += "/"
    return url


def extract_node_id(url: str) -> str | None:
    m = re.search(r"/(\d{3,})", url)
    return m.group(1) if m else None


def extract_slug(url: str) -> str | None:
    m = re.search(r"/gp/(?:new-releases|bestsellers|movers-and-shakers|most-wished-for|most-gifted)/([a-z][a-z0-9-]+)", url)
    if m:
        return m.group(1)
    m = re.search(r"/zg(?:bs|ns)/([a-z][a-z0-9-]+)", url)
    return m.group(1) if m else None


def parse_asins(html: str, limit: int = 5) -> list[str]:
    asins = []
    for m in re.finditer(r'/dp/([A-Z0-9]{10})', html):
        asin = m.group(1)
        if asin not in asins:
            asins.append(asin)
        if len(asins) >= limit:
            break
    return asins


def parse_breadcrumb(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("#wayfinding-breadcrumbs_feature_div")
    if not container:
        return []
    results = []
    for a in container.select("a"):
        name = a.get_text(strip=True)
        m = re.search(r"node=(\d+)", a.get("href", ""))
        if name and m:
            results.append({"name": name, "node_id": m.group(1)})
    return results


def parse_sidebar_children(html: str, page_url: str, **_kwargs) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("ul[class*='zg-browse-root']")
    if not root:
        return []

    selected = root.select_one("[class*='zg-selected']")
    if selected:
        cur_li = selected.find_parent("li")
        next_li = cur_li.find_next_sibling("li") if cur_li else None
        container = next_li.select_one("ul[class*='zg-browse-group']") if next_li else None
    else:
        groups = root.select("ul[class*='zg-browse-group']")
        container = groups[-1] if groups else None

    if not container:
        return []

    seen, results = set(), []
    for li in container.find_all("li", recursive=False):
        if "browse-up" in " ".join(li.get("class", [])):
            continue
        a = li.select_one("a[href*='/gp/']") or li.select_one("a[href*='/zgbs/']") or li.select_one("a[href*='/zgns/']") or li.select_one("a[href]")
        if not a:
            continue
        name = a.get_text(strip=True).replace('\xa0', ' ').replace('​', '').strip()
        href = a.get("href", "")
        if not name or not href or name.isdigit():
            continue
        if href.startswith("/"):
            href = _DOMAIN + href
        href = normalize_url(href)
        url_slug = extract_slug(href)
        if href in seen:
            continue
        seen.add(href)
        results.append({
            "name":    name,
            "url":     href,
            "node_id": extract_node_id(href),
            "slug":    url_slug or "",
        })
    return results


def _safe_get(session: requests.Session, url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=15, verify=PROXY_VERIFY)
            if r.status_code == 200:
                if "zg-browse" not in r.text and "Type the characters" in r.text:
                    print(f"    [CAPTCHA] 等待 30s 后重试", flush=True)
                    time.sleep(30 + random.uniform(0, 15))
                    continue
                return r.text
            if r.status_code == 429:
                wait = 60 + random.uniform(0, 30)
                print(f"    [429] 限速 {wait:.0f}s", flush=True)
                time.sleep(wait)
            elif r.status_code == 503:
                time.sleep(20 + random.uniform(0, 10))
            else:
                print(f"    [HTTP {r.status_code}] {url}", flush=True)
                return None
        except requests.RequestException as e:
            wait = 5 * (2 ** attempt) + random.uniform(0, 3)
            print(f"    [异常] attempt {attempt+1}: {e} → {wait:.0f}s", flush=True)
            time.sleep(wait)
    print(f"    [放弃] {url}", flush=True)
    return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 断点管理
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _save_checkpoint(data: dict):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _load_checkpoint() -> dict | None:
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    with open(CHECKPOINT_FILE, encoding="utf-8") as f:
        return json.load(f)


def _clear_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Phase 1: 多 worker 并发 BFS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def crawl_slug(slug: str, proxy_entries: list[dict], max_depth: int = 99):
    """多 worker 并发 BFS 抓取一个 L1 大类子树，每 100 条写入 DB。"""
    num_workers = len(proxy_entries) if proxy_entries else 1

    # 从 DB 加载当前 slug 子树的已有节点（支持续跑 + 补全）
    conn = sqlite3.connect(DB_FILE, timeout=10)
    try:
        conn.execute("ALTER TABLE categories ADD COLUMN site TEXT DEFAULT 'US'")
    except sqlite3.OperationalError:
        pass
    slug_patterns = [f"%/gp/{p.strip('/')}/{slug}/%" for p in CHART_PREFIXES]
    like_clauses = " OR ".join(["url LIKE ?"] * len(slug_patterns))
    existing = conn.execute(
        f"SELECT url, node_id, name, depth FROM categories "
        f"WHERE node_id IS NOT NULL AND site = ? AND ({like_clauses})",
        [_SITE] + slug_patterns
    ).fetchall()
    conn.close()
    visited_urls = set()
    visited_ids  = {r[1] for r in existing if r[1]}

    root_url = normalize_url(f"{_DOMAIN}/gp/new-releases/{slug}/")
    task_q = Queue()

    # 确保 depth=0 根节点存在，尝试从页面获取本地化类目名
    root_name = slug
    try:
        session0 = _make_session(0)
        root_html = _safe_get(session0, root_url)
        if root_html:
            soup = BeautifulSoup(root_html, "html.parser")
            # 榜单页左侧导航树中“选中”的节点即当前根类目名（locale 无关，无榜单前缀）
            # Amazon 用 CSS-module 哈希类名，形如 _p13n-zg-nav-tree-all_style_zg-selected__XXXX
            # 内部含无障碍隐藏子标签 <span class="zg-visually-hidden">(Current)</span>，需剔除
            sel = soup.select_one('span[class*="zg-selected"], span.zg_selected')
            if sel:
                for hidden in sel.select('[class*="visually-hidden"]'):
                    hidden.decompose()
            if sel and sel.get_text(strip=True):
                root_name = htmlmod.unescape(sel.get_text(strip=True)).replace("\xa0", " ").strip()
            else:
                # 回退：h1 形如“Neuerscheinungen in Haustier”/“New Releases in X”，去掉前缀
                h1 = soup.select_one("h1")
                if h1 and h1.get_text(strip=True):
                    h1_text = htmlmod.unescape(h1.get_text(strip=True)).strip()
                    m = re.search(r"\bin\s+(.+)$", h1_text, re.I)
                    root_name = m.group(1).strip() if m else h1_text
    except Exception:
        pass
    if not root_name or root_name == slug:
        root_name = (L1_DISPLAY_NAMES.get(_SITE) or {}).get(slug, slug)
    root_node = {
        "name": root_name,
        "url": root_url,
        "node_id": slug,
        "depth": 0,
        "source": "subtree",
        "slug": slug,
        "parent_node_id": None,
    }
    _db_batch_insert([root_node])
    # INSERT OR IGNORE 不会刷新已有根节点名；显式写回可读名
    with _db_lock:
        conn_rn = sqlite3.connect(DB_FILE, timeout=10)
        conn_rn.execute(
            "UPDATE categories SET name=? WHERE site=? AND depth=0 AND (node_id=? OR slug=?)",
            (root_name, _SITE, slug, slug),
        )
        conn_rn.commit()
        conn_rn.close()

    if existing:
        existing_ids = {r[1] for r in existing if r[1]}
        child_parent_ids = set()
        conn2 = sqlite3.connect(DB_FILE, timeout=10)
        for r in conn2.execute(
            f"SELECT DISTINCT parent_node_id FROM categories "
            f"WHERE parent_node_id IS NOT NULL AND site = ? AND ({like_clauses})",
            [_SITE] + slug_patterns
        ).fetchall():
            child_parent_ids.add(r[0])
        conn2.close()
        enqueued = 0
        for url, node_id, name, depth in existing:
            is_parent = node_id in child_parent_ids
            if not is_parent:
                task_q.put({"url": url, "name": name, "node_id": node_id, "depth": depth, "parent_node_id": None})
                enqueued += 1
        skipped = len(existing) - enqueued
        print(f"  [{slug}] DB {len(existing)} 节点, 跳过 {skipped} 个已展开父节点, 入队 {enqueued} 个待探索", flush=True)
    else:
        task_q.put({"url": root_url, "name": root_name, "node_id": None, "depth": 0, "parent_node_id": None})
    visited_urls.add(root_url)

    lock = threading.Lock()
    pending_nodes = []       # 待写入 DB 的缓冲区
    total_added = [0]
    total_found = [0]
    errors = []
    in_flight = [0]          # 正在处理的任务数
    bfs_stats = {"nodes_processed": 0, "requests_made": 0, "requests_ok": 0,
                 "requests_fail": 0, "start_time": time.time()}
    proxy_stats = {}
    for i in range(num_workers):
        px = proxy_entries[i] if proxy_entries else None
        port = px["proxy"].split(":")[-1] if px else "direct"
        proxy_stats[i] = {"port": port, "ok": 0, "fail": 0, "429": 0, "captcha": 0}

    def _flush():
        if not pending_nodes:
            return
        batch = list(pending_nodes)
        pending_nodes.clear()
        added = _db_batch_insert(batch)
        total_added[0] += added
        print(f"  [{slug}] 批量写入 DB: {added} 条 (累计 {total_added[0]})", flush=True)

    def bfs_worker(proxy_entry, worker_id):
        session = requests.Session()
        ua = USER_AGENTS[worker_id % len(USER_AGENTS)]
        session.headers.update({**HEADERS, "User-Agent": ua, "Accept-Language": _LANG})
        if proxy_entry:
            session.proxies.update({"http": proxy_entry["proxy"], "https": proxy_entry["proxy"]})

        while True:
            try:
                node = task_q.get(timeout=5)
            except Empty:
                with lock:
                    if in_flight[0] == 0:
                        return
                continue

            with lock:
                in_flight[0] += 1

            try:
                url = node["url"]
                depth = node["depth"]
                if depth >= max_depth:
                    continue

                all_children = []
                seen_child_ids = set()
                node_nid = node.get("node_id")
                node_slug = extract_slug(url) or slug
                node_req_count = 0

                if node_nid:
                    chart_urls = [normalize_url(f"{_DOMAIN}{p}{node_slug}/{node_nid}/") for p in CHART_PREFIXES]
                else:
                    chart_urls = [url]

                def _fetch_one(u):
                    return u, _safe_get(session, u)

                with ThreadPoolExecutor(max_workers=len(chart_urls)) as pool_ex:
                    futures = {pool_ex.submit(_fetch_one, u): u for u in chart_urls}
                    for fut in as_completed(futures):
                        chart_url, html = fut.result()
                        node_req_count += 1
                        with lock:
                            bfs_stats["requests_made"] += 1
                            if html:
                                bfs_stats["requests_ok"] += 1
                                proxy_stats[worker_id]["ok"] += 1
                            else:
                                bfs_stats["requests_fail"] += 1
                                proxy_stats[worker_id]["fail"] += 1
                        if not html:
                            continue
                        found = parse_sidebar_children(html, chart_url)
                        for c in found:
                            cid = c.get("node_id")
                            if cid and cid not in seen_child_ids:
                                seen_child_ids.add(cid)
                                all_children.append(c)

                if not all_children and not node_nid:
                    html = _safe_get(session, url)
                    if html:
                        all_children = parse_sidebar_children(html, url)
                children = all_children
                parent_id = node.get("node_id") or slug

                new_count = 0
                with lock:
                    for c in children:
                        c_url = normalize_url(c["url"])
                        c_nid = c.get("node_id")
                        if c_url in visited_urls:
                            continue
                        if c_nid and c_nid in visited_ids:
                            continue
                        visited_urls.add(c_url)
                        if c_nid:
                            visited_ids.add(c_nid)
                        c["depth"] = depth + 1
                        c["parent_node_id"] = parent_id
                        c["source"] = "subtree"
                        pending_nodes.append(c)
                        task_q.put(c)
                        new_count += 1
                        total_found[0] += 1

                    if len(pending_nodes) >= BFS_BATCH_SIZE:
                        _flush()

                with lock:
                    bfs_stats["nodes_processed"] += 1
                    np = bfs_stats["nodes_processed"]
                    elapsed = time.time() - bfs_stats["start_time"]
                    if np % 100 == 0:
                        rpm = np / elapsed * 60 if elapsed > 0 else 0
                        req_rpm = bfs_stats["requests_made"] / elapsed * 60 if elapsed > 0 else 0
                        ts = time.strftime("%H:%M:%S")
                        print(f"  [{ts}] 📊 已处理 {np} 节点 | {rpm:.1f} 节点/分 | {req_rpm:.0f} 请求/分 | "
                              f"请求 {bfs_stats['requests_ok']}✓ {bfs_stats['requests_fail']}✗ | "
                              f"新节点 {total_found[0]} | 队列 {task_q.qsize()}", flush=True)
                        ip_parts = [f"W{wid}({ps['port']}): {ps['ok']}✓{ps['fail']}✗"
                                    for wid, ps in sorted(proxy_stats.items())]
                        print(f"  [{ts}] 🌐 IP池  " + "  ".join(ip_parts), flush=True)

                time.sleep(random.uniform(0.3, 0.8))
                if new_count > 0 or depth <= 1:
                    ts = time.strftime("%H:%M:%S")
                    print(f"  [{ts}] [W{worker_id}] L{depth} {node['name']}  子+{new_count}  请求×{node_req_count}  队列:{task_q.qsize()}  总:{total_found[0]}", flush=True)

            finally:
                with lock:
                    in_flight[0] -= 1
                task_q.task_done()

    print(f"[{slug}] BFS 启动: {num_workers} worker", flush=True)
    _save_checkpoint({"slug": slug, "phase": "bfs", "started_at": time.strftime("%Y-%m-%d %H:%M:%S")})

    threads = []
    for i in range(num_workers):
        px = proxy_entries[i] if proxy_entries else None
        t = threading.Thread(target=bfs_worker, args=(px, i), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.2)

    for t in threads:
        t.join()

    # 写入剩余缓冲
    with lock:
        _flush()

    elapsed = time.time() - bfs_stats["start_time"]
    mins = elapsed / 60
    rpm = bfs_stats["nodes_processed"] / mins if mins > 0 else 0
    print(f"[{slug}] BFS 完成: {total_found[0]} 个新节点, {len(errors)} 个错误, DB 累计写入 {total_added[0]}", flush=True)
    print(f"  耗时 {mins:.1f} 分钟 | 处理 {bfs_stats['nodes_processed']} 节点 ({rpm:.1f}/分) | "
          f"HTTP 请求 {bfs_stats['requests_made']} ({bfs_stats['requests_ok']}✓ {bfs_stats['requests_fail']}✗)", flush=True)
    for wid, ps in sorted(proxy_stats.items()):
        total_w = ps["ok"] + ps["fail"]
        fail_rate = ps["fail"] / total_w * 100 if total_w > 0 else 0
        print(f"  W{wid} ({ps['port']}): {total_w} 请求, {ps['ok']}✓ {ps['fail']}✗, 失败率 {fail_rate:.1f}%", flush=True)
    _save_checkpoint({"slug": slug, "phase": "bfs_done", "total": total_found[0],
                      "finished_at": time.strftime("%Y-%m-%d %H:%M:%S")})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Phase 2: 多 worker 并发面包屑补全（全量，不抽样）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run_breadcrumb():
    """从 DB 读取未检查的叶子节点，多 worker 并发查面包屑发现隐藏类目。"""
    pool = ProxyPool()
    if pool.size == 0:
        print("[breadcrumb] 无可用代理，退出")
        return

    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.row_factory = sqlite3.Row
    all_nodes = [dict(r) for r in conn.execute(
        "SELECT name, url, node_id, depth, slug, parent_node_id, breadcrumb_checked "
        "FROM categories WHERE node_id IS NOT NULL AND site = ?",
        (_SITE,)
    ).fetchall()]
    conn.close()

    targets = [n for n in all_nodes if not n.get("breadcrumb_checked")]

    known_ids = {n["node_id"] for n in all_nodes}
    node_map  = {n["node_id"]: n for n in all_nodes}

    print(f"[breadcrumb] DB {len(all_nodes)} 节点, {len(targets)} 个未检查, {pool.size} worker")

    if not targets:
        print("[breadcrumb] 无待检查节点，退出")
        return

    task_q = Queue()
    for node in targets:
        task_q.put(node)

    lock = threading.Lock()
    pending_nodes = []
    checked_batch = []
    stats = {"done": 0, "found": 0, "written": 0, "total": len(targets), "start_time": time.time()}
    bc_proxy_stats = {}
    for i, px in enumerate(pool.all_entries()):
        port = px["proxy"].split(":")[-1]
        bc_proxy_stats[i] = {"port": port, "ok": 0, "fail": 0, "asins": 0}

    def _flush_bc():
        if pending_nodes:
            batch = list(pending_nodes)
            pending_nodes.clear()
            added = _db_batch_insert(batch)
            stats["written"] += added
        if checked_batch:
            _db_mark_bc_checked(list(checked_batch))
            checked_batch.clear()

    asin_q = Queue()
    global_seen_asins = set()
    phase1_done = [False]
    nodes_collected = [0]

    def _process_crumbs(crumbs_list, worker_id):
        for crumbs in crumbs_list:
            last_known_id = None
            for j, crumb in enumerate(crumbs):
                nid = crumb["node_id"]
                if nid in known_ids:
                    last_known_id = nid
                    continue
                if j > 0 and crumbs[j-1]["node_id"] in known_ids:
                    parent_nid = crumbs[j-1]["node_id"]
                elif last_known_id:
                    parent_nid = last_known_id
                else:
                    continue
                parent_node = node_map.get(parent_nid)
                depth = (parent_node["depth"] + 1) if parent_node else 1
                parent_slug = parent_node.get("slug", "") if parent_node else ""
                new_node = {
                    "name": crumb["name"],
                    "url": normalize_url(f"{_DOMAIN}/gp/new-releases/{parent_slug}/{nid}/"),
                    "node_id": nid,
                    "slug": parent_slug,
                    "depth": depth,
                    "parent_node_id": parent_nid,
                    "source": "breadcrumb",
                }
                pending_nodes.append(new_node)
                known_ids.add(nid)
                node_map[nid] = new_node
                stats["found"] += 1
                print(f"  [W{worker_id}] 发现: {crumb['name']} ({nid}) depth={depth} parent={parent_nid}", flush=True)

    # Phase 1: 扫榜单页收集 ASIN
    def collect_worker(proxy_entry, worker_id):
        session = requests.Session()
        ua = USER_AGENTS[worker_id % len(USER_AGENTS)]
        session.headers.update({**HEADERS, "User-Agent": ua, "Accept-Language": _LANG})
        session.proxies.update({"http": proxy_entry["proxy"], "https": proxy_entry["proxy"]})

        while True:
            try:
                leaf = task_q.get(timeout=3)
            except Empty:
                return

            try:
                leaf_nid = leaf["node_id"]
                leaf_slug = leaf.get("slug", "")
                node_asins = []

                for prefix in CHART_PREFIXES:
                    chart_url = normalize_url(f"{_DOMAIN}{prefix}{leaf_slug}/{leaf_nid}/")
                    html = _safe_get(session, chart_url)
                    with lock:
                        if html:
                            bc_proxy_stats[worker_id]["ok"] += 1
                        else:
                            bc_proxy_stats[worker_id]["fail"] += 1
                    if not html:
                        continue
                    for asin in parse_asins(html, limit=10):
                        with lock:
                            if asin not in global_seen_asins:
                                global_seen_asins.add(asin)
                                node_asins.append(asin)
                    time.sleep(random.uniform(0.3, 0.6))

                if not node_asins:
                    with lock:
                        checked_batch.append(leaf_nid)
                else:
                    for asin in node_asins:
                        asin_q.put((asin, leaf_nid))

                with lock:
                    nodes_collected[0] += 1
                    nc = nodes_collected[0]
                if nc % 50 == 0:
                    ts = time.strftime("%H:%M:%S")
                    print(f"  [{ts}] P1收集: {nc}/{stats['total']}  ASIN队列: {asin_q.qsize()}", flush=True)

            finally:
                task_q.task_done()

    # Phase 2: 并行访问 ASIN 产品页提取面包屑
    def asin_worker(proxy_entry, worker_id):
        session = requests.Session()
        ua = USER_AGENTS[worker_id % len(USER_AGENTS)]
        session.headers.update({**HEADERS, "User-Agent": ua, "Accept-Language": _LANG})
        session.proxies.update({"http": proxy_entry["proxy"], "https": proxy_entry["proxy"]})

        node_crumbs = {}  # leaf_nid -> [crumbs_list]

        while True:
            try:
                asin, leaf_nid = asin_q.get(timeout=5)
            except Empty:
                if phase1_done[0]:
                    break
                continue

            try:
                prod_html = _safe_get(session, f"{_DOMAIN}/dp/{asin}")
                with lock:
                    bc_proxy_stats[worker_id]["asins"] += 1
                    if prod_html:
                        bc_proxy_stats[worker_id]["ok"] += 1
                    else:
                        bc_proxy_stats[worker_id]["fail"] += 1
                if prod_html:
                    crumbs = parse_breadcrumb(prod_html)
                    if crumbs:
                        with lock:
                            if leaf_nid not in node_crumbs:
                                node_crumbs[leaf_nid] = []
                            node_crumbs[leaf_nid].append(crumbs)
                            _process_crumbs([crumbs], worker_id)
                            if len(pending_nodes) >= BC_BATCH_SIZE:
                                _flush_bc()

                time.sleep(random.uniform(0.3, 0.5))
            finally:
                asin_q.task_done()

        with lock:
            for nid in node_crumbs:
                if nid not in [c for c in checked_batch]:
                    checked_batch.append(nid)
            if len(checked_batch) >= BC_BATCH_SIZE:
                _flush_bc()

    # Phase 2 进度汇报线程
    def progress_reporter():
        while not phase1_done[0] or not asin_q.empty():
            time.sleep(15)
            elapsed = time.time() - stats["start_time"]
            total_asins = sum(ps["asins"] for ps in bc_proxy_stats.values())
            asin_rpm = total_asins / elapsed * 60 if elapsed > 0 else 0
            ts = time.strftime("%H:%M:%S")
            print(f"  [{ts}] P2访问: ASIN {total_asins}个 ({asin_rpm:.0f}/分)  队列剩余: {asin_q.qsize()}  发现: {stats['found']}", flush=True)
            ip_parts = [f"W{wid}({ps['port']}): {ps['ok']}✓{ps['fail']}✗ asin:{ps['asins']}"
                        for wid, ps in sorted(bc_proxy_stats.items())]
            print(f"  [{ts}] 🌐 IP池  " + "  ".join(ip_parts), flush=True)

    _save_checkpoint({"phase": "breadcrumb", "total_nodes": len(targets),
                      "started_at": time.strftime("%Y-%m-%d %H:%M:%S")})

    entries = pool.all_entries()

    # 启动 Phase 1: 收集 ASIN
    print(f"[breadcrumb] P1: 启动 {len(entries)} 个收集 worker", flush=True)
    p1_threads = []
    for i, px in enumerate(entries):
        t = threading.Thread(target=collect_worker, args=(px, i), daemon=True)
        t.start()
        p1_threads.append(t)
        time.sleep(0.2)

    # 启动 Phase 2: 并行访问 ASIN (与 P1 同时运行)
    print(f"[breadcrumb] P2: 启动 {len(entries)} 个 ASIN worker", flush=True)
    p2_threads = []
    for i, px in enumerate(entries):
        t = threading.Thread(target=asin_worker, args=(px, i), daemon=True)
        t.start()
        p2_threads.append(t)
        time.sleep(0.2)

    # 启动进度汇报线程
    reporter = threading.Thread(target=progress_reporter, daemon=True)
    reporter.start()

    # 等 P1 完成
    for t in p1_threads:
        t.join()
    phase1_done[0] = True
    stats["done"] = nodes_collected[0]
    print(f"[breadcrumb] P1 完成: {nodes_collected[0]} 节点收集完毕, ASIN 队列剩余 {asin_q.qsize()}", flush=True)

    # 等 P2 完成
    for t in p2_threads:
        t.join()

    with lock:
        _flush_bc()

    elapsed = time.time() - stats["start_time"]
    mins = elapsed / 60
    total_asins = sum(ps["asins"] for ps in bc_proxy_stats.values())
    asin_rpm = total_asins / mins if mins > 0 else 0
    total_ok = sum(ps["ok"] for ps in bc_proxy_stats.values())
    total_fail = sum(ps["fail"] for ps in bc_proxy_stats.values())
    print(f"\n{'='*50}")
    print(f"面包屑完成: {nodes_collected[0]}/{stats['total']} 节点")
    print(f"发现隐藏类目: {stats['found']}  写入 DB: {stats['written']}")
    print(f"耗时 {mins:.1f} 分钟 | ASIN {total_asins}个 ({asin_rpm:.0f}/分) | HTTP {total_ok}✓ {total_fail}✗")
    for wid, ps in sorted(bc_proxy_stats.items()):
        total_w = ps["ok"] + ps["fail"]
        fail_rate = ps["fail"] / total_w * 100 if total_w > 0 else 0
        print(f"  W{wid} ({ps['port']}): {total_w} 请求, {ps['ok']}✓ {ps['fail']}✗, 失败率 {fail_rate:.1f}%, ASIN访问 {ps['asins']}")
    print(f"{'='*50}\n", flush=True)

    _save_checkpoint({"phase": "breadcrumb_done", "total_nodes": stats["total"],
                      "checked": nodes_collected[0], "found": stats["found"],
                      "written": stats["written"],
                      "finished_at": time.strftime("%Y-%m-%d %H:%M:%S")})


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 主入口
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def run(slugs: list[str], max_depth: int = 99, skip_breadcrumb: bool = False, no_proxy: bool = False):
    if no_proxy:
        entries = []
        print("[pool] 代理已禁用，使用直连 (1 worker)")
    else:
        pool = ProxyPool()
        entries = pool.all_entries()

    for slug in slugs:
        print(f"\n{'='*50}")
        print(f"开始: {slug}")
        print(f"{'='*50}")
        crawl_slug(slug, entries, max_depth=max_depth)

    # 面包屑阶段已从工作流移除：无界 asin_q 会撑爆内存导致 OOM，
    # 且实测 发现=0 毫无收益。仅保留 BFS。

    _clear_checkpoint()
    conn = sqlite3.connect(DB_FILE, timeout=10)
    total = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
    conn.close()
    print(f"\n完成! DB 总计: {total} 个节点")


SKIP_SLUG_KEYWORDS = {
    "books", "book", "buch", "fremdsprachig", "lesen",
    "digital", "kindle", "audible", "ebook", "e-book", "digital-text",
    "dmusic", "music-artist", "music",
    "movie", "movies", "film", "dvd", "blu-ray", "prime-video", "instant-video",
    "videogames", "video-games", "videospiele", "game-download",
    "software", "mobile-apps",
    "gift-card", "gift-cards", "geschenkgut",
    "amazon-devices", "amazon-renewed", "amazon-warehouse",
    "collectible-coins", "entertainment-collectibles", "unique-finds",
    "handmade",
    "boost",
    # ── 服装鞋帽箱包珠宝（用户要求排除） ──
    "fashion", "clothing", "bekleidung", "shoes", "schuhe",
    "jewelry", "schmuck", "watches", "uhren",
    "luggage", "handbags", "koffer",
    # ── 收藏品（钱币/球星卡，非标准品） ──
    "coins", "collectible", "collectibles",
}

KEEP_SLUG_KEYWORDS = {
    "musical-instruments", "musikinstrumente",
    "appliances", "elektro-grossgerate",
}


def _is_skip_slug(slug: str) -> bool:
    slug_lower = slug.lower()
    for kw in KEEP_SLUG_KEYWORDS:
        if kw in slug_lower:
            return False
    for kw in SKIP_SLUG_KEYWORDS:
        if kw in slug_lower:
            return True
    return False


def discover_l1_slugs(domain: str, lang: str) -> list[str]:
    session = requests.Session()
    ua = USER_AGENTS[0]
    session.headers.update({**HEADERS, "User-Agent": ua, "Accept-Language": lang})
    url = f"{domain}/gp/new-releases/"
    html = _safe_get(session, url)
    if not html:
        print(f"[discover] 无法访问 {url}，尝试 bestsellers 入口", flush=True)
        html = _safe_get(session, f"{domain}/gp/bestsellers/")
    if not html:
        print("[discover] 无法获取根页面", flush=True)
        return []
    soup = BeautifulSoup(html, "html.parser")
    root_ul = soup.select_one("ul[class*='zg-browse-root']")
    if not root_ul:
        root_ul = soup.select_one("#zg_browseRoot")
    if not root_ul:
        print("[discover] 未找到侧边栏类目列表", flush=True)
        return []
    slugs = []
    for a in root_ul.select("a[href]"):
        href = a.get("href", "")
        slug = extract_slug(href)
        if slug and slug not in slugs:
            slugs.append(slug)
    print(f"[discover] 从页面发现 {len(slugs)} 个 L1 类目", flush=True)
    filtered = [s for s in slugs if not _is_skip_slug(s)]
    skipped = [s for s in slugs if _is_skip_slug(s)]
    if skipped:
        print(f"[discover] 过滤掉 {len(skipped)} 个非标类目: {', '.join(skipped)}", flush=True)
    print(f"[discover] 保留 {len(filtered)} 个实体商品类目: {', '.join(filtered)}", flush=True)
    return filtered

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="按需抓取 Amazon 类目子树")
    parser.add_argument("slugs", nargs="*", help="L1 大类 slug")
    parser.add_argument("--site", default="US", help="站点代码: US, DE, JP, UK, FR")
    parser.add_argument("--all", action="store_true", help="抓取所有 L1 大类（仅 US）")
    parser.add_argument("--max-depth", type=int, default=99)
    parser.add_argument("--skip-breadcrumb", action="store_true",
                        help="仅 BFS，跳过面包屑")
    parser.add_argument("--breadcrumb-only", action="store_true",
                        help="仅面包屑补全（多 worker 并发）")
    parser.add_argument("--no-proxy", action="store_true",
                        help="禁用代理池，直连")
    args = parser.parse_args()

    mp = get_marketplace(args.site)
    _SITE   = args.site.upper()
    _DOMAIN = mp["domain"]
    _LANG   = mp["lang"]
    print(f"[站点] {mp['name']} ({_SITE}) → {_DOMAIN}")

    if args.breadcrumb_only:
        run_breadcrumb()
        sys.exit(0)

    if args.all:
        slugs = discover_l1_slugs(_DOMAIN, _LANG)
        if not slugs:
            print(f"[{_SITE}] 自动发现失败，请手动指定 slug")
            sys.exit(1)
    elif args.slugs:
        slugs = args.slugs
    else:
        print("用法: python fetch_subtree.py home-garden --site US")
        print("      python fetch_subtree.py kuche-haushalt-wohnen --site DE")
        print("      python fetch_subtree.py --breadcrumb-only --site JP")
        print("      python fetch_subtree.py --all")
        sys.exit(1)

    run(slugs, max_depth=args.max_depth, skip_breadcrumb=args.skip_breadcrumb,
        no_proxy=args.no_proxy)
