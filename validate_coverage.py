"""
validate_coverage.py - 验证类目覆盖完整性
不修改 DB，只统计 fix_hierarchy BFS 遇到的 node_id 中有多少不在现有 DB 里
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

def norm(url):
    return url.split('?')[0].split('/ref=')[0].rstrip('/') + '/'

def get_known_ids():
    conn = sqlite3.connect(DB)
    ids = set(r[0] for r in conn.execute("SELECT node_id FROM categories WHERE node_id IS NOT NULL"))
    conn.close()
    return ids

def parse_nav(html):
    soup = BeautifulSoup(html, 'html.parser')
    left = soup.select_one('#zg-left-col')
    if not left:
        return []
    results = []
    def walk(el):
        for li in el.find_all('li', recursive=False):
            a = li.find('a')
            if not a:
                continue
            name = a.get_text(strip=True)
            href = a.get('href', '')
            if not name or name.isdigit() or name in ('Any Department', 'See More'):
                sub = li.find('ul', recursive=False)
                if sub: walk(sub)
                continue
            m_nid = re.search(r'/(\d+)', href)
            nid = m_nid.group(1) if m_nid else None
            full = href if href.startswith('http') else 'https://www.amazon.com' + href
            full = norm(full)
            results.append((nid, name, full))
            sub = li.find('ul', recursive=False)
            if sub: walk(sub)
    top = left.find('ul')
    if top: walk(top)
    return results

def run():
    known_ids = get_known_ids()
    print(f"DB 中已知 node_id: {len(known_ids)} 个")

    queue = deque()
    visited = set()
    missing_nodes = {}   # nid -> (name, url)
    pages = 0

    # Root
    r = session.get(ROOT, timeout=15)
    records = parse_nav(r.text)
    visited.add(norm(ROOT))
    for nid, name, url in records:
        if url not in visited:
            queue.append((url, nid, name))
    pages += 1

    print(f"L1 发现 {len(records)} 个，开始扫描...\n")

    while queue:
        url, nid, name = queue.popleft()
        url_n = norm(url)
        if url_n in visited:
            continue
        visited.add(url_n)

        # 检查当前节点是否在 DB 里
        if nid and nid not in known_ids:
            missing_nodes[nid] = (name, url)

        try:
            r = session.get(url, timeout=15)
        except Exception:
            time.sleep(DELAY)
            continue

        if r.status_code != 200:
            time.sleep(DELAY)
            continue

        records = parse_nav(r.text)
        for rec_nid, rec_name, rec_url in records:
            if rec_nid and rec_nid not in known_ids:
                missing_nodes[rec_nid] = (rec_name, rec_url)
            if rec_url not in visited:
                queue.append((rec_url, rec_nid, rec_name))

        pages += 1
        if pages % 50 == 0:
            print(f"  [{pages}页] 队列:{len(queue)} 遗漏:{len(missing_nodes)}", flush=True)

        time.sleep(DELAY)

    print(f"\n=== 验证完成 ===")
    print(f"访问页面: {pages}")
    print(f"遗漏节点: {len(missing_nodes)}")
    if missing_nodes:
        print(f"\n前20个遗漏节点:")
        for nid, (name, url) in list(missing_nodes.items())[:20]:
            print(f"  [{nid}] {name}")
            print(f"    {url}")

if __name__ == "__main__":
    run()
