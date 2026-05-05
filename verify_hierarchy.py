"""验证面包屑方案 - 修正版"""
import sqlite3, requests, re, time, sys
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

conn = sqlite3.connect('data/categories.db')
conn.row_factory = sqlite3.Row

samples = conn.execute("""
    SELECT name, url, node_id, depth 
    FROM categories 
    WHERE node_id IS NOT NULL AND depth > 0
    ORDER BY RANDOM() LIMIT 3
""").fetchall()

session = requests.Session()
session.headers.update(HEADERS)

for s in samples:
    print(f"\n{'='*60}")
    print(f"Node: {s['name']}  (DB depth={s['depth']}, id={s['node_id']})")
    print(f"URL:  {s['url']}")
    
    try:
        r = session.get(s['url'], timeout=15)
        if r.status_code != 200:
            print(f"  HTTP {r.status_code}")
            continue
        
        soup = BeautifulSoup(r.text, 'html.parser')
        
        # 找左侧层级导航 - 多种选择器
        selectors = [
            '#zg_browseRoot',
            '#zg-left-col',
            '.a-section .a-spacing-none ul',
            '[role=navigation]',
            '#nav-subnav',
        ]
        
        nav = None
        for sel in selectors:
            nav = soup.select_one(sel)
            if nav:
                print(f"  匹配选择器: {sel}")
                break
        
        if not nav:
            # 兜底：打印所有含 new-releases 的链接看结构
            print("  未找到导航容器，转储所有 new-releases 链接:")
            all_links = soup.select('a[href*="/gp/new-releases/"]')
            seen = set()
            for a in all_links[:20]:
                name = a.get_text(strip=True)
                href = a.get('href', '')
                if name and name not in seen and not name.isdigit():
                    seen.add(name)
                    # 看缩进层级
                    parents = []
                    el = a
                    while el:
                        if el.name == 'ul':
                            parents.append('ul')
                        elif el.name == 'li':
                            parents.append('li')
                        el = el.parent
                    indent = parents.count('ul')
                    nid = re.search(r'/(\d+)', href)
                    print(f"    {'  '*indent}{name}  (indent={indent}, node={nid.group(1) if nid else '-'})")
        else:
            # 解析导航树结构
            print(f"  导航树内容:")
            def walk(el, depth=0):
                if el.name == 'a':
                    name = el.get_text(strip=True)
                    href = el.get('href', '')
                    nid = re.search(r'/(\d+)', href)
                    is_bold = el.find_parent('b') is not None or 'zg_selected' in el.get('class', [])
                    marker = ' <<<' if is_bold else ''
                    if name and not name.isdigit():
                        print(f"    {'  '*depth}{name} (node={nid.group(1) if nid else '-'}){marker}")
                for child in (el.children if hasattr(el, 'children') else []):
                    if hasattr(child, 'name'):
                        next_depth = depth + 1 if child.name == 'ul' else depth
                        walk(child, next_depth)
            walk(nav)
                
    except Exception as e:
        print(f"  Error: {e}")
    
    time.sleep(2)

conn.close()
print("\n=== Done ===")
