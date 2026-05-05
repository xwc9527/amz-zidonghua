"""
fix_remaining.py - 只修复剩余未标记节点（非 BFS，直接访问）
4 线程 + 0.5s 全局速率限制 → 预计 ~15 分钟完成
"""
import sqlite3, requests, re, time, sys, threading
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = 'data/categories.db'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

# 全局速率限制
_rate_lock = threading.Lock()
_rate_last = 0.0
RATE_INTERVAL = 0.5

def rate_wait():
    global _rate_last
    with _rate_lock:
        now = time.time()
        gap = RATE_INTERVAL - (now - _rate_last)
        if gap > 0:
            time.sleep(gap)
        _rate_last = time.time()


def get_unfixed():
    """获取所有 true_depth IS NULL 的节点"""
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, name, url, node_id FROM categories WHERE true_depth IS NULL"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def parse_nav_path(html, target_url):
    """
    从导航树中提取当前节点的层级路径。
    返回 (true_depth, parent_node_id) 或 None
    """
    soup = BeautifulSoup(html, 'html.parser')
    left = soup.select_one('#zg-left-col')
    if not left:
        return None

    # 收集所有导航节点及其层级
    path = []  # [(nid_or_slug, name, depth)]

    def walk(el, depth):
        for li in el.find_all('li', recursive=False):
            a = li.find('a')
            if not a:
                continue
            name = a.get_text(strip=True)
            href = a.get('href', '')
            if not name or name.isdigit() or name in ('Any Department', 'See More'):
                sub = li.find('ul', recursive=False)
                if sub:
                    walk(sub, depth)
                continue

            m_nid = re.search(r'/(\d+)', href)
            m_slug = re.search(r'/gp/new-releases/([a-z][a-z0-9-]+)/?$',
                               href.split('?')[0].split('/ref=')[0])
            nid = m_nid.group(1) if m_nid else None
            slug = m_slug.group(1) if m_slug else None
            key = nid or slug

            path.append((key, name, depth))

            sub = li.find('ul', recursive=False)
            if sub:
                walk(sub, depth + 1)

    top = left.find('ul')
    if top:
        walk(top, 1)

    if not path:
        return None

    # 当前节点 = 路径最深层中最后一个（导航树通常展示到当前节点同级）
    # 策略：找 target_url 的 node_id 或 slug 在 path 里的位置
    target_bare = target_url.rstrip('/').lower()
    m_tnid = re.search(r'/(\d+)$', target_bare)
    m_tslug = re.search(r'/gp/new-releases/([a-z][a-z0-9-]+)$', target_bare)
    target_key = (m_tnid.group(1) if m_tnid else None) or (m_tslug.group(1) if m_tslug else None)

    if not target_key:
        return None

    # 在 path 中找到 target
    for i, (key, name, depth) in enumerate(path):
        if key == target_key:
            parent_key = path[i-1][0] if i > 0 else None
            return (depth, parent_key)

    # 如果没找到精确匹配，用最深层推断
    max_depth = max(p[2] for p in path)
    # 当前节点应该在最深层
    for key, name, depth in path:
        if depth == max_depth:
            parent_key = None
            for pk, pn, pd in path:
                if pd == depth - 1:
                    parent_key = pk
            return (max_depth, parent_key)

    return None


def process_node(node, session, stats, db_lock):
    """处理单个未修复节点"""
    url = node['url']
    if not url:
        return

    rate_wait()

    try:
        r = session.get(url, timeout=10)
    except Exception:
        with db_lock:
            stats['errors'] += 1
        return

    if r.status_code != 200:
        with db_lock:
            stats['errors'] += 1
        return

    result = parse_nav_path(r.text, url)
    if not result:
        with db_lock:
            stats['no_nav'] += 1
        return

    true_depth, parent_nid = result

    with db_lock:
        conn = sqlite3.connect(DB, timeout=10)
        conn.execute(
            "UPDATE categories SET true_depth=?, parent_node_id=? WHERE id=?",
            (true_depth, parent_nid, node['id'])
        )
        conn.commit()
        conn.close()
        stats['fixed'] += 1


def run():
    unfixed = get_unfixed()
    total = len(unfixed)
    print(f"[开始] 待修复节点: {total}")

    if total == 0:
        print("全部已修复！")
        return

    stats = {'fixed': 0, 'errors': 0, 'no_nav': 0, 'done': 0}
    db_lock = threading.Lock()
    t0 = time.time()

    NUM_WORKERS = 4
    import queue as Q
    q = Q.Queue()
    for node in unfixed:
        q.put(node)

    def worker():
        s = requests.Session()
        s.headers.update(HEADERS)
        while True:
            try:
                node = q.get(timeout=2)
            except Q.Empty:
                return
            process_node(node, s, stats, db_lock)
            with db_lock:
                stats['done'] += 1
                done = stats['done']
            if done % 50 == 0:
                elapsed = time.time() - t0
                print(f"  [{done}/{total}] 修复:{stats['fixed']} 失败:{stats['errors']} 无导航:{stats['no_nav']} | {elapsed:.0f}s",
                      flush=True)
            q.task_done()

    threads = []
    for _ in range(NUM_WORKERS):
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    elapsed = time.time() - t0
    print(f"\n[完成] {elapsed:.0f}s")
    print(f"  修复: {stats['fixed']}")
    print(f"  失败: {stats['errors']}")
    print(f"  无导航: {stats['no_nav']}")

    # 最终统计
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    filled = conn.execute("SELECT COUNT(*) FROM categories WHERE true_depth IS NOT NULL").fetchone()[0]
    total_all = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
    print(f"\n  覆盖率: {filled}/{total_all} ({100*filled/total_all:.1f}%)")
    print("\n  true_depth 分布:")
    for r in conn.execute("SELECT true_depth, COUNT(*) as cnt FROM categories WHERE true_depth IS NOT NULL GROUP BY true_depth ORDER BY true_depth"):
        print(f"    L{r['true_depth']}: {r['cnt']}")
    remaining = total_all - filled
    if remaining > 0:
        print(f"\n  仍未修复: {remaining}")
        for r in conn.execute("SELECT name, url FROM categories WHERE true_depth IS NULL LIMIT 5"):
            print(f"    {r['name']} | {r['url']}")
    conn.close()


if __name__ == "__main__":
    run()
