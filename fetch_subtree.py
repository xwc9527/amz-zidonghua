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
from queue import Queue, Empty, Full
from concurrent.futures import ThreadPoolExecutor, as_completed

from subtree_pipeline_experiment import BoundedParsePipeline

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

# 单个 chart URL 最大补抓次数（每次补抓必须由不同 worker/IP 消费）
MAX_URL_RETRY = 3
# 有界补抓队列容量：防止失败 URL 无限堆积撑爆内存
RETRY_QUEUE_MAX = 20000
# Proven independent configuration: two node streams per proxy feed a bounded
# eight-process parser without changing the fixed-proxy producer/consumer model.
NODE_STREAMS_PER_PROXY = 2
PARSE_PROCESS_WORKERS = 8
PARSE_MAX_PENDING_PAGES = 256
RESULT_COORDINATOR_WORKERS = 32

CHART_PREFIXES = [
    "/gp/new-releases/",
    "/gp/bestsellers/",
    "/gp/movers-and-shakers/",
    "/gp/most-wished-for/",
    "/gp/most-gifted/",
]

# 每个 worker 预建的 Session lane 数 = 榜单入口数，节点内 5 路并发时每路独立 Session
NUM_LANES = len(CHART_PREFIXES)

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
                payload = json.load(f)
            # Legacy snapshots were a bare list. The current daemon publishes
            # metadata plus the usable proxy list under ``entries``.
            entries = payload.get("entries", []) if isinstance(payload, dict) else payload
            if not isinstance(entries, list):
                raise ValueError(f"invalid proxy pool format: {PROXY_POOL_FILE}")
            for p in entries:
                if not isinstance(p, dict) or not p.get("proxy"):
                    continue
                self._q.put(p)
                self._all.append(p)
            print(f"[pool] 加载 {len(self._all)} 个代理端口")
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
        try:
            for n in nodes:
                depth = n.get("depth", 0)
                parent_node_id = n.get("parent_node_id") or ""
                cur = conn.execute(
                    "INSERT INTO categories "
                    "(name, url, node_id, depth, source, explored, parent_node_id, slug, site) "
                    "VALUES(?, ?, ?, ?, ?, 1, ?, ?, ?) "
                    "ON CONFLICT(site, node_id, parent_node_id) DO UPDATE SET "
                    "name=excluded.name, url=excluded.url",
                    (n["name"], normalize_url(n["url"]), n.get("node_id"),
                     depth, n.get("source", "subtree"), parent_node_id,
                     n.get("slug", ""), _SITE)
                )
                added += cur.rowcount
            conn.commit()
        finally:
            conn.close()
    return added


def _db_mark_bc_checked(node_ids: list[str]):
    if not node_ids:
        return
    with _db_lock:
        conn = sqlite3.connect(DB_FILE, timeout=10)
        conn.executemany(
            "UPDATE categories SET breadcrumb_checked=1 WHERE site=? AND node_id=?",
            [(_SITE, nid) for nid in node_ids]
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


def _slug_url_patterns(slug: str) -> list[str]:
    """Return SQL LIKE patterns for every chart without duplicating ``/gp``."""
    return [f"%{prefix}{slug}/%" for prefix in CHART_PREFIXES]


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


def _safe_get(
    session: requests.Session, url: str, retries: int = 1
) -> tuple[str | None, str]:
    """返回 (html, reason)。成功时 reason 为 ""；失败时给出失败原因，供上层决定补抓。

    关键：最后一次尝试失败后**不再休眠**（避免明知要放弃仍空等），
    直接返回失败原因，交由上层把该 URL 投入补抓队列换 IP 重试。
    """
    reason = "UNKNOWN"
    for attempt in range(retries):
        is_last = attempt == retries - 1
        try:
            r = session.get(url, timeout=15, verify=PROXY_VERIFY)
            if r.status_code == 200:
                if "zg-browse" not in r.text and "Type the characters" in r.text:
                    reason = "CAPTCHA"
                    if is_last:
                        break
                    print(f"    [CAPTCHA] 等待 30s 后重试", flush=True)
                    time.sleep(30 + random.uniform(0, 15))
                    continue
                return r.text, ""
            if r.status_code == 429:
                reason = "RATE_LIMITED"
                if is_last:
                    break
                wait = 60 + random.uniform(0, 30)
                print(f"    [429] 限速 {wait:.0f}s", flush=True)
                time.sleep(wait)
            elif r.status_code == 403:
                reason = "FORBIDDEN"
                if is_last:
                    break
                time.sleep(5 + random.uniform(0, 3))
            elif r.status_code == 503:
                reason = "HTTP_503"
                if is_last:
                    break
                time.sleep(20 + random.uniform(0, 10))
            else:
                # 4xx/其它：非 IP 级瞬时问题，重试同一 URL 无意义，直接返回。
                print(f"    [HTTP {r.status_code}] {url}", flush=True)
                return None, f"HTTP_{r.status_code}"
        except requests.RequestException as e:
            reason = "NETWORK"
            if is_last:
                print(f"    [异常] attempt {attempt+1}: {e} → 放弃(不再休眠)", flush=True)
                break
            wait = 5 * (2 ** attempt) + random.uniform(0, 3)
            print(f"    [异常] attempt {attempt+1}: {e} → {wait:.0f}s", flush=True)
            time.sleep(wait)
    return None, reason


def _is_retryable_chart_failure(reason: str) -> bool:
    """Only failures that may change with another exit IP enter the retry queue."""
    return reason in {"RATE_LIMITED", "FORBIDDEN", "CAPTCHA", "NETWORK", "HTTP_503"}


def _is_confirmed_absent(reason: str) -> bool:
    """Amazon uses these responses for chart entries that do not exist."""
    return reason in {"HTTP_404", "HTTP_410"}


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
    num_workers = len(proxy_entries) * NODE_STREAMS_PER_PROXY if proxy_entries else 1

    # 从 DB 加载当前 slug 子树的已有节点（支持续跑 + 补全）
    conn = sqlite3.connect(DB_FILE, timeout=10)
    try:
        conn.execute("ALTER TABLE categories ADD COLUMN site TEXT DEFAULT 'US'")
    except sqlite3.OperationalError:
        pass
    slug_patterns = _slug_url_patterns(slug)
    like_clauses = " OR ".join(["url LIKE ?"] * len(slug_patterns))
    existing = conn.execute(
        f"SELECT url, node_id, name, depth, parent_node_id FROM categories "
        f"WHERE node_id IS NOT NULL AND site = ? AND (slug = ? OR {like_clauses})",
        [_SITE, slug] + slug_patterns
    ).fetchall()
    conn.close()
    visited_urls = {normalize_url(r[0]) for r in existing if r[0]}
    visited_ids  = {r[1] for r in existing if r[1]}
    visited_edges = {(r[1], r[4] or "") for r in existing if r[1]}

    root_url = normalize_url(f"{_DOMAIN}/gp/new-releases/{slug}/")
    task_q = Queue()

    # 确保 depth=0 根节点存在，尝试从页面获取本地化类目名
    root_name = slug
    try:
        session0 = requests.Session()
        session0.headers.update({**HEADERS, "User-Agent": USER_AGENTS[0], "Accept-Language": _LANG})
        root_html, _ = _safe_get(session0, root_url)
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
        if root_name == slug:
            print(f"  [{slug}] warning: could not resolve root name from page, falling back to slug", flush=True)
    except Exception as e:
        print(f"  [{slug}] root name fetch failed: {e}, falling back to slug", flush=True)
    if not root_name or root_name == slug:
        root_name = (L1_DISPLAY_NAMES.get(_SITE) or {}).get(slug, slug)
    root_node = {
        "name": root_name,
        "url": root_url,
        "node_id": slug,
        "depth": 0,
        "source": "subtree",
        "slug": slug,
        "parent_node_id": "",
    }
    _db_batch_insert([root_node])

    if existing:
        existing_ids = {r[1] for r in existing if r[1]}
        child_parent_ids = set()
        conn2 = sqlite3.connect(DB_FILE, timeout=10)
        for r in conn2.execute(
            f"SELECT DISTINCT parent_node_id FROM categories "
            f"WHERE parent_node_id != '' AND site = ? AND (slug = ? OR {like_clauses})",
            [_SITE, slug] + slug_patterns
        ).fetchall():
            child_parent_ids.add(r[0])
        conn2.close()
        enqueued = 0
        for url, node_id, name, depth, _parent_node_id in existing:
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
    errors = []              # 补抓耗尽的 URL：{url, parent_id, reason, attempts}
    worker_failures = []     # unexpected worker exceptions; never report a complete crawl
    in_flight = [0]          # 正在处理的任务数（节点任务 + 补抓任务）
    # 有界补抓队列：失败的单个 chart URL 进入这里，由任意空闲 worker 换 IP 重试
    retry_q: Queue = Queue(maxsize=RETRY_QUEUE_MAX)
    bfs_stats = {"nodes_processed": 0, "requests_made": 0, "requests_ok": 0,
                 "requests_fail": 0, "start_time": time.time()}
    proxy_stats = {}
    for i in range(num_workers):
        px = proxy_entries[i // NODE_STREAMS_PER_PROXY] if proxy_entries else None
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

    def _record_request(worker_id, ok):
        bfs_stats["requests_made"] += 1
        if ok:
            bfs_stats["requests_ok"] += 1
            proxy_stats[worker_id]["ok"] += 1
        else:
            bfs_stats["requests_fail"] += 1
            proxy_stats[worker_id]["fail"] += 1

    def _enqueue_retry(chart_url, parent_id, depth, name, reason, attempt, failed_proxy_key):
        """把失败的单个 chart URL 投入有界补抓队列。队列满时不阻塞：记为耗尽。"""
        task = {
            "kind": "chart",
            "chart_url": chart_url,
            "parent_id": parent_id,
            "depth": depth,
            "name": name,
            "reason": reason,
            "attempt": attempt,
            "failed_proxy_key": failed_proxy_key,
        }
        try:
            retry_q.put_nowait(task)
            return True
        except Full:
            with lock:
                errors.append({"url": chart_url, "parent_id": parent_id,
                               "reason": "RETRY_QUEUE_FULL", "attempts": attempt})
            print(f"  [补抓队列满，丢弃] {chart_url} reason={reason}", flush=True)
            return False

    def _merge_children(children, parent_id, depth):
        """把解析出的子类目并入图（去重）；新节点入 task_q 继续 BFS。返回新增数。"""
        new_count = 0
        with lock:
            for c in children:
                c_url = normalize_url(c["url"])
                c_nid = c.get("node_id")
                edge_key = (c_nid or c_url, parent_id)
                if edge_key in visited_edges:
                    continue
                visited_edges.add(edge_key)
                visited_urls.add(c_url)
                c["depth"] = depth + 1
                c["parent_node_id"] = parent_id
                c["source"] = "subtree"
                pending_nodes.append(c)
                # Persist every real parent edge, but expand a node only
                # once per slug crawl to avoid duplicate HTTP work.
                if not c_nid or c_nid not in visited_ids:
                    if c_nid:
                        visited_ids.add(c_nid)
                    task_q.put({"kind": "node", **c})
                new_count += 1
                total_found[0] += 1
            if len(pending_nodes) >= BFS_BATCH_SIZE:
                _flush()
        return new_count

    def _next_task(worker_id, proxy_key):
        """统一取任务：优先补抓，再取节点。在锁内原子判断终止，避免竞态误判完成。

        返回 (kind, payload) / "WAIT"（有任务在途，稍后再试）/ None（全部完成）。
        补抓队列与节点队列都清空、且没有任何在途任务时才判定站点完成——
        满足“失败 URL 补抓完成或明确耗尽后才算完成”。
        """
        with lock:
            # A retry must be consumed by another worker/IP. Rotate tasks that
            # originated from this worker instead of immediately retrying them
            # on the same failed exit. With a single worker there is no alternate
            # IP, so bounded same-worker retry is the only possible fallback.
            retry_count = retry_q.qsize()
            for _ in range(retry_count):
                try:
                    task = retry_q.get_nowait()
                except Empty:
                    break
                if len(proxy_entries) <= 1 or task.get("failed_proxy_key") != proxy_key:
                    in_flight[0] += 1
                    return ("chart", task)
                retry_q.put_nowait(task)
            try:
                node = task_q.get_nowait()
                in_flight[0] += 1
                return ("node", node)
            except Empty:
                pass
            if in_flight[0] == 0:
                return None
            return "WAIT"

    def _record_node_complete(node, depth, node_req_count, new_count, worker_id):
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
                      f"新节点 {total_found[0]} | 队列 {task_q.qsize()} | 补抓 {retry_q.qsize()}", flush=True)
                ip_parts = [f"W{wid}({ps['port']}): {ps['ok']}✓{ps['fail']}✗"
                            for wid, ps in sorted(proxy_stats.items())]
                print(f"  [{ts}] 🌐 IP池  " + "  ".join(ip_parts), flush=True)
        if new_count > 0 or depth <= 1:
            ts = time.strftime("%H:%M:%S")
            print(f"  [{ts}] [W{worker_id}] L{depth} {node.get('name','')}  子+{new_count}  请求×{node_req_count}  队列:{task_q.qsize()}  总:{total_found[0]}", flush=True)

    def _finish_node_parse(node, parent_id, depth, node_req_count, parse_jobs, worker_id):
        """Result-coordinator task: wait for CPU parsing, then merge atomically."""
        try:
            all_children = []
            seen_child_ids = set()
            for _chart_url, parse_future in parse_jobs:
                for child in parse_future.result():
                    child_id = child.get("node_id")
                    if child_id and child_id not in seen_child_ids:
                        seen_child_ids.add(child_id)
                        all_children.append(child)
            new_count = _merge_children(all_children, parent_id, depth)
            _record_node_complete(node, depth, node_req_count, new_count, worker_id)
        except BaseException as exc:
            with lock:
                worker_failures.append((worker_id, "parse", repr(exc)))
        finally:
            with lock:
                in_flight[0] -= 1

    def _process_node(node, lane_sessions, worker_id, proxy_key):
        url = node["url"]
        depth = node["depth"]
        if depth >= max_depth:
            return
        node_nid = node.get("node_id")
        node_slug = extract_slug(url) or slug
        parent_id = node_nid or slug

        if node_nid:
            chart_urls = [normalize_url(f"{_DOMAIN}{p}{node_slug}/{node_nid}/") for p in CHART_PREFIXES]
        else:
            chart_urls = [url]

        parse_jobs = []
        node_req_count = 0

        # 节点内多路并发：每条 chart 绑定一条独立 lane Session（复用连接、不跨线程共享）
        def _fetch_lane(lane, u):
            html, reason = _safe_get(lane_sessions[lane], u)
            return lane, u, html, reason

        with ThreadPoolExecutor(max_workers=len(chart_urls)) as pool_ex:
            futures = [
                pool_ex.submit(_fetch_lane, i % NUM_LANES, u)
                for i, u in enumerate(chart_urls)
            ]
            for fut in as_completed(futures):
                _lane, chart_url, html, reason = fut.result()
                node_req_count += 1
                with lock:
                    _record_request(worker_id, bool(html))
                if not html:
                    # 补抓粒度是失败的单个 URL，而不是整个节点：成功的入口正常并入，
                    # 失败的入口投入补抓队列换其它 IP 重试。
                    if _is_retryable_chart_failure(reason):
                        _enqueue_retry(
                            chart_url, parent_id, depth, node.get("name", ""),
                            reason, attempt=1, failed_proxy_key=proxy_key,
                        )
                    elif not _is_confirmed_absent(reason):
                        with lock:
                            errors.append({"url": chart_url, "parent_id": parent_id,
                                           "reason": reason, "attempts": 1})
                    continue
                parse_jobs.append((chart_url, parse_pipeline.submit(html, chart_url, _DOMAIN)))

        # The single root request stays synchronous because its fallback reuses
        # this worker's lane Session. Normal five-chart nodes never wait here.
        if not node_nid:
            all_children = []
            for _chart_url, parse_future in parse_jobs:
                all_children.extend(parse_future.result())
            # 无 node_id 的根任务：并发无所获时回退单请求
            if not all_children:
                html, _reason = _safe_get(lane_sessions[0], url)
                if html:
                    all_children = parse_pipeline.submit(html, url, _DOMAIN).result()
            new_count = _merge_children(all_children, parent_id, depth)
            _record_node_complete(node, depth, node_req_count, new_count, worker_id)
            return

        # Keep completion detection correct: the fetch task will decrement one
        # in-flight slot, while this extra slot remains until parsing, merging,
        # and child enqueueing have all completed.
        with lock:
            in_flight[0] += 1
        try:
            result_executor.submit(
                _finish_node_parse,
                node, parent_id, depth, node_req_count, parse_jobs, worker_id,
            )
        except BaseException:
            with lock:
                in_flight[0] -= 1
            raise

        # Preserve the old per-stream pacing without blocking the parse stage.
        time.sleep(random.uniform(0.3, 0.8))

    def _process_chart_retry(task, lane_sessions, worker_id, proxy_key):
        chart_url = task["chart_url"]
        parent_id = task["parent_id"]
        depth = task["depth"]
        attempt = task["attempt"]
        # 由不同 worker（不同 IP）消费本任务；lane 轮换以复用不同连接。
        html, reason = _safe_get(lane_sessions[attempt % NUM_LANES], chart_url)
        with lock:
            _record_request(worker_id, bool(html))
        if html:
            children = parse_pipeline.submit(html, chart_url, _DOMAIN).result()
            _merge_children(children, parent_id, depth)
            return
        if _is_confirmed_absent(reason):
            return
        if _is_retryable_chart_failure(reason) and attempt < MAX_URL_RETRY:
            _enqueue_retry(
                chart_url, parent_id, depth, task.get("name", ""), reason,
                attempt=attempt + 1, failed_proxy_key=proxy_key,
            )
        else:
            with lock:
                errors.append({"url": chart_url, "parent_id": parent_id,
                               "reason": reason, "attempts": attempt})
            print(f"  [补抓耗尽] {chart_url} reason={reason} attempts={attempt}", flush=True)

    def bfs_worker(proxy_entry, worker_id, proxy_index):
        # 每个 worker 预建 NUM_LANES 条长期 Session（分别对应 5 个榜单入口 lane），
        # 复用连接；节点内 5 路并发时每路用独立 Session，消除跨线程共享 Session。
        lane_sessions = []
        ua = USER_AGENTS[proxy_index % len(USER_AGENTS)]
        proxy_key = (
            proxy_entry.get("exit_ip") or proxy_entry.get("proxy")
            if proxy_entry else "direct"
        )
        for lane in range(NUM_LANES):
            s = requests.Session()
            s.headers.update({**HEADERS, "User-Agent": ua, "Accept-Language": _LANG})
            if proxy_entry:
                s.proxies.update({"http": proxy_entry["proxy"], "https": proxy_entry["proxy"]})
            lane_sessions.append(s)

        try:
            while True:
                item = _next_task(worker_id, proxy_key)
                if item is None:
                    return
                if item == "WAIT":
                    time.sleep(0.1)
                    continue
                kind, payload = item
                try:
                    if kind == "node":
                        _process_node(payload, lane_sessions, worker_id, proxy_key)
                    else:
                        _process_chart_retry(payload, lane_sessions, worker_id, proxy_key)
                except BaseException as exc:
                    with lock:
                        worker_failures.append((worker_id, kind, repr(exc)))
                    return
                finally:
                    with lock:
                        in_flight[0] -= 1
        finally:
            for s in lane_sessions:
                try:
                    s.close()
                except Exception:
                    pass

    print(
        f"[{slug}] BFS 启动: {num_workers} worker "
        f"({NODE_STREAMS_PER_PROXY} streams/IP) + {PARSE_PROCESS_WORKERS} parser processes",
        flush=True,
    )
    _save_checkpoint({"slug": slug, "phase": "bfs", "started_at": time.strftime("%Y-%m-%d %H:%M:%S")})

    parse_pipeline = BoundedParsePipeline(PARSE_PROCESS_WORKERS, PARSE_MAX_PENDING_PAGES)
    result_executor = ThreadPoolExecutor(max_workers=RESULT_COORDINATOR_WORKERS)
    threads = []
    try:
        for i in range(num_workers):
            proxy_index = i // NODE_STREAMS_PER_PROXY if proxy_entries else 0
            px = proxy_entries[proxy_index] if proxy_entries else None
            t = threading.Thread(target=bfs_worker, args=(px, i, proxy_index), daemon=True)
            t.start()
            threads.append(t)
            time.sleep(0.02)

        for t in threads:
            t.join()
    finally:
        result_executor.shutdown(wait=True, cancel_futures=True)
        parse_pipeline.shutdown(wait=True, cancel_futures=True)

    # 写入剩余缓冲
    with lock:
        _flush()

    elapsed = time.time() - bfs_stats["start_time"]
    mins = elapsed / 60
    rpm = bfs_stats["nodes_processed"] / mins if mins > 0 else 0
    crawl_state = "完成" if not errors and not worker_failures else "未完成"
    print(f"[{slug}] BFS {crawl_state}: {total_found[0]} 个新节点, {len(errors)} 个补抓耗尽 URL, DB 累计写入 {total_added[0]}", flush=True)
    print(f"  耗时 {mins:.1f} 分钟 | 处理 {bfs_stats['nodes_processed']} 节点 ({rpm:.1f}/分) | "
          f"HTTP 请求 {bfs_stats['requests_made']} ({bfs_stats['requests_ok']}✓ {bfs_stats['requests_fail']}✗)", flush=True)
    if worker_failures:
        for worker_id, kind, detail in worker_failures:
            print(f"  ⚠️ W{worker_id} {kind} worker异常: {detail}", flush=True)
        raise RuntimeError(
            f"[{slug}] incomplete: {len(worker_failures)} worker exception(s); "
            "site completion is blocked"
        )
    if errors:
        reason_counts = {}
        for e in errors:
            reason_counts[e["reason"]] = reason_counts.get(e["reason"], 0) + 1
        reason_summary = ", ".join(f"{k}×{v}" for k, v in sorted(reason_counts.items()))
        print(f"  ⚠️ 补抓耗尽 {len(errors)} 个 URL（已达 {MAX_URL_RETRY} 次上限）: {reason_summary}", flush=True)
        for e in errors[:20]:
            print(f"     - {e['url']} reason={e['reason']} attempts={e['attempts']}", flush=True)
        if len(errors) > 20:
            print(f"     … 其余 {len(errors) - 20} 个略", flush=True)
    for wid, ps in sorted(proxy_stats.items()):
        total_w = ps["ok"] + ps["fail"]
        fail_rate = ps["fail"] / total_w * 100 if total_w > 0 else 0
        print(f"  W{wid} ({ps['port']}): {total_w} 请求, {ps['ok']}✓ {ps['fail']}✗, 失败率 {fail_rate:.1f}%", flush=True)
    if errors:
        raise RuntimeError(
            f"[{slug}] incomplete: {len(errors)} chart URL(s) exhausted; "
            "site completion is blocked"
        )
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
                    html, _ = _safe_get(session, chart_url)
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
                prod_html, _ = _safe_get(session, f"{_DOMAIN}/dp/{asin}")
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
    html, _ = _safe_get(session, url)
    if not html:
        print(f"[discover] 无法访问 {url}，尝试 bestsellers 入口", flush=True)
        html, _ = _safe_get(session, f"{domain}/gp/bestsellers/")
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
