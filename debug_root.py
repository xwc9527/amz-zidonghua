"""调试：看 root 页面导航结构"""
import requests, re, sys
from bs4 import BeautifulSoup
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36"}
r = requests.get("https://www.amazon.com/gp/new-releases/", headers=HEADERS, timeout=15)
soup = BeautifulSoup(r.text, 'html.parser')

# 检查各种选择器
for sel in ['#zg-left-col', '#zg_browseRoot', '.zg_selected', '#zg-right-col', '[role=tree]', '[role=navigation]']:
    el = soup.select_one(sel)
    print(f"[{sel}] {'Found' if el else 'NOT found'}")

# 打印所有 new-releases 链接
links = soup.select('a[href*="/gp/new-releases/"]')
seen = set()
print(f"\n=== new-releases 链接 ({len(links)} 个) ===")
for a in links[:50]:
    name = a.get_text(strip=True)
    href = a.get('href', '')
    if name and name not in seen and not name.isdigit() and len(name) < 80:
        seen.add(name)
        m = re.search(r'/gp/new-releases/([^/]+)', href)
        slug = m.group(1) if m else '-'
        print(f"  {name} => slug={slug}")

# 看 #zg-left-col 结构
lc = soup.select_one('#zg-left-col')
if lc:
    print(f"\n=== #zg-left-col 原始文本(前500字) ===")
    print(lc.get_text()[:500])
    print(f"\n=== ul/li 结构 ===")
    for ul in lc.select('ul'):
        for li in ul.find_all('li', recursive=False):
            a = li.find('a')
            if a:
                print(f"  {a.get_text(strip=True)}")
