"""
fix_hierarchy.py v2 - 修复类目树层级关系（修复 URL 去重 + 匹配覆盖）
BFS 从 root 开始，解析每页 #zg-left-col 导航树
已修复的 true_depth 不会丢失（幂等 UPDATE）
"""
import sqlite3, requests, re, time, sys
from bs4 import BeautifulSoup
from collections import deque

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB = 'data/categories.db'
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}
DELAY = 2.0
ROOT = "https://www.amazon.com/gp/new-releases/"

session = requests.Session()
session.headers.update(HEADERS)


def ensure_columns():
    conn = sqlite3.connect(DB)
    for col in ['parent_node_id TEXT', 'true_depth INTEGER']:
        try:
            conn.execute(f"ALTER TABLE categories ADD COLUMN {col}")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()


def norm(url):
    """强力 URL 标准化：去查询串、ref=、尾斜杠统一、全小写路径"""
    url = url.split('?')[0].split('/ref=')[0]
    url = url.rstrip('/')
    # 统一为不带尾斜杠的小写形式（用于去重）
    return url.lower()


def norm_for_fetch(url):
    """用于实际发请求的 URL（保留原始大小写，确保尾斜杠）"""
    url = url.split('?')[0].split('/ref=')[0]
    if not url.endswith('/'):
        url += '/'
    return url


def parse_nav(html):
    """解析 #zg-left-col 导航树"""
    soup = BeautifulSoup(html, 'html.parser')
    left = soup.select_one('#zg-left-col')
    if not left:
        return [], []

    results = []       # (key, name, parent_key, depth, url)
    child_urls = []    # 需要继续 BFS 的 URL

    def walk(el, parent_key, depth):
        for li in el.find_all('li', recursive=False):
            a = li.find('a')
            if not a:
                continue
            name = a.get_text(strip=True)
            href = a.get('href', '')
            if not name or name.isdigit() or name in ('Any Department', 'See More'):
                sub = li.find('ul', recursive=False)
                if sub:
                    walk(sub, parent_key, depth)
                continue

            # 提取 key：优先 node_id，否则 slug
            m_nid = re.search(r'/(\d+)', href)
            m_slug = re.search(r'/gp/new-releases/([a-z][a-z0-9-]+)/?$',
                               href.split('?')[0].split('/ref=')[0])
            nid = m_nid.group(1) if m_nid else None
            slug = m_slug.group(1) if m_slug else None
            key = nid or slug

            full = href if href.startswith('http') else 'https://www.amazon.com' + href
            full_norm = norm_for_fetch(full)

            results.append((key, name, parent_key, depth, full_norm))

            sub = li.find('ul', recursive=False)
            if sub:
                walk(sub, key, depth + 1)
                for child_li in sub.find_all('li', recursive=False):
                    ca = child_li.find('a')
                    if ca:
                        ch = ca.get('href', '')
                        if ch:
                            cf = ch if ch.startswith('http') else 'https://www.amazon.com' + ch
                            child_urls.append(norm_for_fetch(cf))

    top = left.find('ul')
    if top:
        walk(top, None, 1)

    return results, child_urls


def upsert_db(records):
    """修复层级 + 查缺补漏：先 UPDATE，匹配不到就 INSERT"""
    conn = sqlite3.connect(DB, timeout=10)
    updated = 0
    inserted = 0
    for (key, name, parent_key, depth, url) in records:
        if not key:
            continue
        try:
            # 1. 按 node_id 精确匹配
            c = conn.execute(
                "UPDATE categories SET true_depth=?, parent_node_id=? WHERE node_id=?",
                (depth, parent_key, key)
            )
            if c.rowcount > 0:
                updated += c.rowcount
                continue

            # 2. 按 URL 匹配（有尾斜杠 / 无尾斜杠都试）
            url_bare = url.rstrip('/')
            url_slash = url_bare + '/'
            c = conn.execute(
                "UPDATE categories SET true_depth=?, parent_node_id=? WHERE url IN (?,?)",
                (depth, parent_key, url_bare, url_slash)
            )
            if c.rowcount > 0:
                updated += c.rowcount
                continue

            # 3. 按 name + 模糊 URL 匹配
            c = conn.execute(
                "UPDATE categories SET true_depth=?, parent_node_id=? "
                "WHERE name=? AND true_depth IS NULL AND url LIKE ?",
                (depth, parent_key, name, f"%/gp/new-releases/%{key}%")
            )
            if c.rowcount > 0:
                updated += c.rowcount
                continue

            # 4. 都匹配不到 → INSERT 新节点
            nid = key if key.isdigit() else None
            conn.execute(
                "INSERT OR IGNORE INTO categories "
                "(name, url, node_id, true_depth, parent_node_id, depth, source, explored) "
                "VALUES (?, ?, ?, ?, ?, ?, 'fix_hierarchy', 1)",
                (name, url, nid, depth, parent_key, depth)
            )
            inserted += 1
        except Exception:
            pass
    conn.commit()
    conn.close()
    return updated, inserted


def run():
    ensure_columns()

    queue = deque()
    visited = set()
    total_updated = 0
    total_inserted = 0
    pages = 0

    # 1. Root 页面
    print(f"[开始] 访问 root ...")
    r = session.get(ROOT, timeout=15)
    records, child_urls = parse_nav(r.text)
    u, i = upsert_db(records)
    total_updated += u
    total_inserted += i
    pages += 1
    visited.add(norm(ROOT))
    print(f"  L1 类目: {len(records)} 个, 更新 {u} 新增 {i}")

    # 所有 L1 页面加入队列
    for rec in records:
        url = rec[4]
        nk = norm(url)
        if nk not in visited:
            queue.append(url)

    # 2. BFS
    print(f"\n[BFS] 开始层级扫描 ...")
    t0 = time.time()

    while queue:
        url = queue.popleft()
        nk = norm(url)
        if nk in visited:
            continue
        visited.add(nk)

        try:
            r = session.get(url, timeout=15)
        except Exception:
            time.sleep(DELAY)
            continue

        if r.status_code != 200:
            time.sleep(DELAY)
            continue

        records, child_urls = parse_nav(r.text)
        if records:
            u, i = upsert_db(records)
            total_updated += u
            total_inserted += i

        # 把新发现的子页面加入队列
        for cu in child_urls:
            cnk = norm(cu)
            if cnk not in visited:
                queue.append(cu)

        pages += 1
        if pages % 20 == 0:
            elapsed = time.time() - t0
            print(f"  [{pages}页 | {elapsed:.0f}s] 队列:{len(queue)} 已访问:{len(visited)} 更新:{total_updated} 新增:{total_inserted}",
                  flush=True)

        time.sleep(DELAY)

    # 3. 统计
    elapsed = time.time() - t0
    print(f"\n[完成] {pages} 页, {elapsed:.0f}s, 更新 {total_updated} 条, 新增 {total_inserted} 条")

    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    print("\n  true_depth 分布:")
    for r in conn.execute(
        "SELECT true_depth, COUNT(*) as cnt FROM categories "
        "WHERE true_depth IS NOT NULL GROUP BY true_depth ORDER BY true_depth"
    ):
        print(f"    L{r['true_depth']}: {r['cnt']}")
    filled = conn.execute("SELECT COUNT(*) FROM categories WHERE true_depth IS NOT NULL").fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
    unfixed = total - filled
    print(f"\n  覆盖率: {filled}/{total} ({100*filled/total:.1f}%)")
    if unfixed > 0:
        print(f"  未修复: {unfixed} 条")
        print("  未修复样本:")
        for r in conn.execute("SELECT name, url, node_id FROM categories WHERE true_depth IS NULL LIMIT 5"):
            print(f"    {r['name']} | url={r['url']}")
    conn.close()


if __name__ == "__main__":
    run()
