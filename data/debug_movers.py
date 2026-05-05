from bs4 import BeautifulSoup
import re, json

html = open(r'c:\Users\47763\Desktop\amz选品\data\sample_movers-and-shakers.html', encoding='utf-8').read()
soup = BeautifulSoup(html, 'html.parser')

grid = soup.select_one('.p13n-desktop-grid')
if not grid:
    print("No grid found"); exit()

# 找所有 dp 链接
links = grid.select('a[href*="/dp/"]')
print(f"dp links in grid: {len(links)}")

# 每个 dp link 往上找父容器
for link in links[:3]:
    href = link.get("href", "")
    m = re.search(r'/dp/([A-Z0-9]{10})', href)
    asin = m.group(1) if m else "?"
    
    # 向上3层找商品容器
    container = link
    for _ in range(8):
        container = container.parent
        if container is None:
            break
    
    print(f"\nASIN: {asin}")
    print(f"  Container classes: {container.get('class', []) if container else 'None'}")
    
    # 提取容器内的文本和关键元素
    if container:
        # 价格
        price = container.select_one('.a-price .a-offscreen, ._cDEzb_p13n-sc-price_3mJ9Z')
        print(f"  Price: {price.get_text(strip=True) if price else 'N/A'}")
        
        # 名字
        name = container.select_one('div._cDEzb_p13n-sc-css-line-clamp-3_g3dy1, div._cDEzb_p13n-sc-css-line-clamp-1_1Fn1y')
        if not name:
            name = container.select_one('a > span > div')
        print(f"  Name: {name.get_text(strip=True)[:60] if name else 'N/A'}")
        
        # 评分
        rating = container.select_one('.a-icon-alt')
        print(f"  Rating: {rating.get_text(strip=True) if rating else 'N/A'}")
        
        # 涨幅（飙升榜特有）
        pct = container.select_one('[class*="zg-percent"], [class*="percent"]')
        if not pct:
            # 看 span 有百分号的
            for span in container.select('span'):
                txt = span.get_text(strip=True)
                if '%' in txt:
                    pct = span
                    break
        print(f"  Pct change: {pct.get_text(strip=True) if pct else 'N/A'}")
        
        # 图片
        img = container.select_one('img')
        print(f"  Image: {img.get('src','')[:60] if img else 'N/A'}")
