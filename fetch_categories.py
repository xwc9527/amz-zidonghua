# fetch_categories.py — 阶段1：构建完整类目节点树
#
# 两遍扫描：
#   第一遍：侧边栏 BFS，不进产品页，覆盖所有可见节点（L1-L4）
#   第二遍：对叶子节点取1个ASIN进产品页，提取面包屑发现隐藏深层节点
#
# 存储：data/categories.db (SQLite)
# 运行：python -u fetch_categories.py

import json
import os
import re
import sys
import time
import random
import sqlite3
import requests
from bs4 import BeautifulSoup

# Windows 控制台默认 GBK 编码，强制输出 UTF-8 避免特殊字符崩溃
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from config import (
    HEADERS, DATA_DIR, DB_FILE, NEW_RELEASES_ROOT,
    DELAY_PRODUCT_MIN, DELAY_PRODUCT_MAX,
)

os.makedirs(DATA_DIR, exist_ok=True)

STATUS_FILE = os.path.join(DATA_DIR, "status.json")           # 状态文件（看板读取）

# 全局 Session：复用 TCP+TLS 连接，省去每次握手 ~0.5-0.8s
SESSION = requests.Session()
SESSION.headers.update(HEADERS)

# 停止控制：通过 stdin 接收 stop 指令
import threading
import queue as _queue_mod
_stop_flag = threading.Event()   # set() = 终止爬虫

def _start_stdin_listener():
    """后台线程监听 stdin，收到 stop 指令时终止爬虫。"""
    def _listen():
        try:
            for line in sys.stdin:
                if line.strip().lower() == "stop":
                    _stop_flag.set()
                    print("[控制] 停止信号收到，正在退出...", flush=True)
                    break
        except Exception:
            pass
    t = threading.Thread(target=_listen, daemon=True)
    t.start()

# 全局速率限制器：所有线程共享，确保请求间隔 ≥ 0.7s（1.43 req/s）
_rate_lock = threading.Lock()
_rate_last = 0.0
RATE_INTERVAL = 0.4

def _rate_wait():
    """线程安全的全局速率限制。"""
    global _rate_last
    with _rate_lock:
        now = time.time()
        gap = RATE_INTERVAL - (now - _rate_last)
        if gap > 0:
            time.sleep(gap)
        _rate_last = time.time()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 工具层：HTTP、延迟、URL规范化
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def safe_get(url: str, retries: int = 3, is_product: bool = False) -> str | None:
    """
    带指数退避的 GET 请求。
    is_product=True 时使用更保守的延迟（产品页风险高于类目页）。
    返回 HTML 字符串，失败返回 None（调用方跳过该 URL）。
    """
    for attempt in range(retries):
        try:
            # 停止信号检查 + 全局速率限制
            if _stop_flag.is_set():
                return None
            _rate_wait()

            resp = SESSION.get(url, timeout=15)

            if resp.status_code == 200:
                return resp.text

            if resp.status_code == 429:
                wait = 60 + random.uniform(0, 30)
                print(f"  [429] 限速，等 {wait:.0f}s ...", flush=True)
                time.sleep(wait)

            elif resp.status_code == 503:
                wait = 20 + random.uniform(0, 10)
                print(f"  [503] 等 {wait:.0f}s ...", flush=True)
                time.sleep(wait)

            else:
                print(f"  [HTTP {resp.status_code}] {url}", flush=True)
                return None

        except requests.RequestException as e:
            wait = 5 * (2 ** attempt) + random.uniform(0, 3)
            print(f"  [异常] {e} → {wait:.0f}s 后重试", flush=True)
            time.sleep(wait)

    print(f"  [放弃] {url}", flush=True)
    return None


# 类目页延迟：1-1.5s（低风险，与普通用户点击导航频率一致）
CATEGORY_DELAY_MIN = 1.0
CATEGORY_DELAY_MAX = 1.5


def delay(is_product: bool = False):
    """请求后随机等待。"""
    lo = DELAY_PRODUCT_MIN if is_product else CATEGORY_DELAY_MIN
    hi = DELAY_PRODUCT_MAX if is_product else CATEGORY_DELAY_MAX
    time.sleep(random.uniform(lo, hi))


def _write_status(total: int, queue_size: int, phase: str):
    """写状态文件供看板读取。"""
    status = {
        "total": total,
        "queue": queue_size,
        "phase": phase,
        "paused": False,
        "time": time.strftime("%H:%M:%S"),
    }
    try:
        with open(STATUS_FILE, "w", encoding="utf-8") as f:
            json.dump(status, f, ensure_ascii=False)
    except Exception:
        pass


def normalize_url(url: str) -> str:
    """
    去掉查询字符串和 /ref=... 后缀，确保同一页面不被重复抓取。
    例：/gp/new-releases/kitchen/ref=zg_nav_0 → /gp/new-releases/kitchen/
    """
    url = url.split("?")[0]
    url = re.sub(r"/ref=.*$", "/", url)
    if not url.endswith("/"):
        url += "/"
    return url


def extract_node_id(url: str) -> str | None:
    """从新品榜 URL 提取数字 node_id（L1 slug 格式返回 None）。"""
    m = re.search(r"/gp/new-releases/[^/]+/(\d+)/", url)
    return m.group(1) if m else None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 解析层：侧边栏、ASIN、面包屑
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def clean_name(text: str) -> str:
    """清理类目名称，去掉非打印字符和特殊空格。"""
    return text.replace('\xa0', ' ').replace('\u200b', '').strip()


def parse_sidebar_links(html: str) -> list[dict]:
    """
    从新品榜页面侧边栏提取子类目链接。
    返回：[{"name": "Cookware", "url": "https://...", "node_id": "289812"}]
    """
    soup = BeautifulSoup(html, "html.parser")
    seen, results = set(), []

    for a in soup.select("a[href*='/gp/new-releases/']"):
        name = clean_name(a.get_text(strip=True))
        href = a.get("href", "")
        if not name or not href:
            continue

        if href.startswith("/"):
            href = "https://www.amazon.com" + href
        href = normalize_url(href)

        if href in seen:
            continue
        seen.add(href)

        # 过滤纯数字名字（分页链接如"1" "2"...）
        if name.isdigit():
            continue

        results.append({
            "name":    name,
            "url":     href,
            "node_id": extract_node_id(href),
        })

    return results


def parse_asins(html: str, limit: int = 3) -> list[str]:
    """
    从新品榜页面提取前 N 个 ASIN，备用于面包屑发现。
    从同一次请求的 HTML 提取，不额外发请求。
    """
    asins = []
    for m in re.finditer(r"/dp/([A-Z0-9]{10})", html):
        asin = m.group(1)
        if asin not in asins:
            asins.append(asin)
        if len(asins) >= limit:
            break
    return asins


def parse_breadcrumb(html: str) -> list[dict]:
    """
    从产品详情页提取面包屑，返回完整路径（含隐藏深层节点）。
    返回：[{"name": "Peelers", "node_id": "16439871"}, ...]
    """
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


def verify_node_has_new_releases(node_id: str) -> bool:
    """
    验证 node_id 是否有对应的新品榜页面（HTTP 200）。
    用流式读取，只读 1KB 就关闭，避免下载完整页面。
    """
    url = f"https://www.amazon.com/gp/new-releases/home-garden/{node_id}/"
    try:
        r = SESSION.get(url, timeout=10, stream=True)
        next(r.iter_content(1024), None)
        r.close()
        return r.status_code == 200
    except requests.RequestException:
        return False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第一遍：侧边栏 BFS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase1_sidebar_bfs() -> dict[str, list[str]]:
    """
    2-worker 并发 BFS，全局速率限制。
    发现的子节点直接写入 SQLite，断电也不丢数据。
    返回 asin_cache 供第二遍面包屑发现使用。
    """
    # 从 DB 加载队列（explored=0 的节点）
    raw_queue = db_get_queue()
    if not raw_queue:
        # 首次运行，插入根节点
        db_insert([{"name": "Root", "url": normalize_url(NEW_RELEASES_ROOT),
                    "node_id": None, "depth": 0}], explored=0)
        raw_queue = db_get_queue()

    print(f"[续跑] 队列: {len(raw_queue)} 个待处理", flush=True)

    # 线程安全数据结构
    task_q = _queue_mod.Queue()
    for item in raw_queue:
        task_q.put(item)

    visited_lock = threading.Lock()
    visited = db_get_all_urls()          # 从 DB 获取所有已知 URL

    nodes_lock = threading.Lock()
    asin_cache = {}

    in_flight = [0]
    in_flight_lock = threading.Lock()
    root_url = normalize_url(NEW_RELEASES_ROOT)

    NUM_WORKERS = 4
    print(f"[第一遍] {NUM_WORKERS}-worker 并发 BFS 开始 ...", flush=True)

    def worker():
        while True:
            try:
                node = task_q.get(timeout=15)
            except _queue_mod.Empty:
                with in_flight_lock:
                    if in_flight[0] == 0:
                        return
                continue

            with in_flight_lock:
                in_flight[0] += 1

            try:
                url = normalize_url(node["url"])
                depth = node.get("depth", 0)

                with visited_lock:
                    if url in visited:
                        db_mark_explored(url)
                        continue
                    visited.add(url)

                html = safe_get(url)
                if not html:
                    db_mark_explored(url)
                    continue

                asins = parse_asins(html, limit=3)
                children = parse_sidebar_links(html)

                with visited_lock:
                    new_children = [c for c in children if normalize_url(c["url"]) not in visited]

                for c in new_children:
                    c["depth"] = depth + 1

                # 新节点直接写入 SQLite（explored=0，即自动加入队列）
                to_insert = [c for c in new_children if normalize_url(c["url"]) != root_url]
                added = db_insert(to_insert, explored=0)

                with visited_lock:
                    for c in new_children:
                        visited.add(normalize_url(c["url"]))

                for c in new_children:
                    task_q.put(c)

                # 标记当前节点为已探索
                db_mark_explored(url)

                if url != root_url and asins:
                    with nodes_lock:
                        asin_cache[url] = asins

                stats = db_stats()
                qsize = task_q.qsize()
                print(f"  [L{depth}] {clean_name(node['name'])}  子+{len(new_children)}  队列:{qsize}  总:{stats['total']}", flush=True)
                db_update_status("sidebar_bfs")

            finally:
                with in_flight_lock:
                    in_flight[0] -= 1
                task_q.task_done()

    # 启动 worker 线程
    threads = []
    for _ in range(NUM_WORKERS):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    stats = db_stats()
    print(f"[第一遍完成] 总计 {stats['total']} 个节点", flush=True)
    return asin_cache


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第二遍：叶子节点面包屑发现
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def phase2_breadcrumb_discovery(asin_cache: dict):
    """
    找出叶子节点（无子节点的类目），通过产品页面包屑发现隐藏的 L5/L6 节点。
    新发现的节点直接写入 SQLite。
    """
    # 从 DB 读取所有已探索节点
    conn = db_conn()
    try:
        rows = conn.execute(
            "SELECT name, url, node_id, depth FROM categories WHERE explored=1"
        ).fetchall()
        nodes = [dict(r) for r in rows]
    finally:
        conn.close()

    # 找叶子节点：URL 不是其他节点 URL 的前缀
    all_urls = {normalize_url(n["url"]) for n in nodes}
    parent_urls = set()
    for url in all_urls:
        for other in all_urls:
            if other != url and other.startswith(url):
                parent_urls.add(url)
                break
    leaves = [n for n in nodes if normalize_url(n["url"]) not in parent_urls and n.get("node_id")]

    known_ids = {n["node_id"] for n in nodes if n.get("node_id")}
    found_total = 0

    print(f"\n[第二遍] 叶子节点 {len(leaves)} 个，开始面包屑发现 ...", flush=True)

    for i, leaf in enumerate(leaves, 1):
        url   = normalize_url(leaf["url"])
        asins = asin_cache.get(url, [])
        if not asins:
            print(f"  [{i}/{len(leaves)}] {leaf['name']} 无缓存ASIN，跳过", flush=True)
            continue

        asin     = asins[0]
        prod_url = f"https://www.amazon.com/dp/{asin}"
        html     = safe_get(prod_url, is_product=True)
        if not html:
            delay(is_product=True)
            continue

        crumbs = parse_breadcrumb(html)
        found  = 0
        for crumb in crumbs:
            nid = crumb["node_id"]
            if nid in known_ids:
                continue

            if verify_node_has_new_releases(nid):
                nr_url = f"https://www.amazon.com/gp/new-releases/home-garden/{nid}/"
                db_insert([{"name": crumb["name"], "url": nr_url,
                           "node_id": nid, "source": "breadcrumb"}], explored=1)
                known_ids.add(nid)
                found += 1
                found_total += 1
                print(f"    [发现] {crumb['name']} ({nid})", flush=True)
            delay(is_product=True)

        print(f"  [{i}/{len(leaves)}] {leaf['name']}  新发现:{found}", flush=True)
        db_update_status("breadcrumb")
        delay(is_product=True)

    print(f"[第二遍完成] 新增 {found_total} 个深层节点", flush=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 存储层：SQLite（替代 JSON 文件）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_db_lock = threading.Lock()   # SQLite 写锁（多线程共享同一连接）


def db_conn() -> sqlite3.Connection:
    """获取/创建线程本地的 DB 连接。"""
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def _parent_url(url: str) -> str | None:
    """从 url 推导父节点 URL。"""
    u = url.rstrip('/')
    if not u:
        return None
    parent = u.rsplit('/', 1)[0] + '/'
    return parent if '/gp/new-releases/' in parent else None


def db_insert(nodes: list[dict], explored: int = 0) -> int:
    """批量插入节点，跳过已存在的 URL。返回实际新增数量。"""
    if not nodes:
        return 0
    with _db_lock:
        conn = db_conn()
        added = 0
        try:
            for n in nodes:
                url = normalize_url(n.get("url", ""))
                if not url:
                    continue
                try:
                    conn.execute(
                        "INSERT OR IGNORE INTO categories"
                        "(name, url, node_id, depth, source, explored) "
                        "VALUES(?, ?, ?, ?, ?, ?)",
                        (n.get("name", ""), url, n.get("node_id"),
                         n.get("depth", 0), n.get("source", "sidebar"),
                         explored)
                    )
                    if conn.total_changes:
                        added += conn.total_changes
                except sqlite3.IntegrityError:
                    pass
            conn.commit()
        finally:
            conn.close()
        return added


def db_mark_explored(url: str):
    """标记某个 URL 为已探索（兼容有/无尾斜杠）。"""
    normalized = normalize_url(url)
    bare = normalized.rstrip("/")
    with _db_lock:
        conn = db_conn()
        try:
            conn.execute(
                "UPDATE categories SET explored=1 WHERE url IN (?, ?)",
                (normalized, bare)
            )
            conn.commit()
        finally:
            conn.close()


def db_get_queue() -> list[dict]:
    """获取所有未探索的节点（即 BFS 队列）。"""
    conn = db_conn()
    try:
        rows = conn.execute(
            "SELECT name, url, node_id, depth FROM categories WHERE explored=0 ORDER BY depth, id"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def db_get_all_urls() -> set:
    """获取所有已知 URL 集合（去重用）。"""
    conn = db_conn()
    try:
        rows = conn.execute("SELECT url FROM categories").fetchall()
        return {r[0] for r in rows}
    finally:
        conn.close()


def db_stats() -> dict:
    """获取统计信息。"""
    conn = db_conn()
    try:
        total = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        queue = conn.execute("SELECT COUNT(*) FROM categories WHERE explored=0").fetchone()[0]
        has_id = conn.execute("SELECT COUNT(*) FROM categories WHERE node_id IS NOT NULL").fetchone()[0]
        bc = conn.execute("SELECT COUNT(*) FROM categories WHERE source='breadcrumb'").fetchone()[0]
        return {"total": total, "queue": queue, "has_id": has_id, "breadcrumb": bc}
    finally:
        conn.close()


def db_update_status(phase: str):
    """更新 run_status 表。"""
    stats = db_stats()
    with _db_lock:
        conn = db_conn()
        try:
            conn.execute(
                "UPDATE run_status SET phase=?, paused=?, total=?, queue=?, updated_at=CURRENT_TIMESTAMP WHERE id=1",
                (phase, 0, stats["total"], stats["queue"])
            )
            conn.commit()
        finally:
            conn.close()
    # 同时写 status.json 供看板读取
    _write_status(stats["total"], stats["queue"], phase)



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 入口
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

if __name__ == "__main__":
    # 启动 stdin 控制监听（接收 dashboard_server 的 pause/resume/stop 指令）
    _start_stdin_listener()

    # 检查数据库
    stats = db_stats()
    print(f"[启动] 数据库: {DB_FILE}", flush=True)
    print(f"[启动] 已有 {stats['total']} 个节点, 队列 {stats['queue']} 个待处理", flush=True)

    # 第一遍（并发 BFS）— 直接从 SQLite 读队列、写结果
    asin_cache = phase1_sidebar_bfs()

    # 第二遍 — 面包屑发现深层节点
    phase2_breadcrumb_discovery(asin_cache)

    stats = db_stats()
    db_update_status("done")
    print(f"\n[全部完成] 总节点数: {stats['total']}  →  {DB_FILE}", flush=True)

