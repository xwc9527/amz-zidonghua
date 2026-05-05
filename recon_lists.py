"""
recon_lists.py — 侦察 4 个 Amazon 榜单页面可提取的全部字段
随机选一个有效类目，分别抓取 4 个榜单，解析出所有商品数据字段
"""
import sqlite3, requests, sys, os, json, re
from bs4 import BeautifulSoup

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE    = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "data", "categories.db")
OUT_DIR = os.path.join(BASE, "data")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

LISTS = [
    ("new-releases",       "新品榜"),
    ("bestsellers",        "畅销榜"),
    ("movers-and-shakers", "飙升榜"),
    ("most-wished-for",    "心愿单"),
]

# 选一个已验证有效的类目
conn = sqlite3.connect(DB_PATH)
row = conn.execute("""
    SELECT node_id, url, name FROM categories
    WHERE nr_valid=1 AND bs_valid=1 AND ms_valid=1 AND mw_valid=1
    ORDER BY RANDOM() LIMIT 1
""").fetchone()
conn.close()

if not row:
    print("没有找到全部 4 个榜单都有效的类目，先用 kitchen 测试")
    node_id, slug, cat_name = "289812", "kitchen", "Kitchen"
else:
    node_id = row[0]
    parts = row[1].rstrip("/").split("/")
    gp_idx = parts.index("gp")
    slug = parts[gp_idx + 2]
    cat_name = row[2]

print(f"测试类目: {cat_name} (node_id={node_id}, slug={slug})")
print("=" * 80)

session = requests.Session()
session.headers.update(HEADERS)

all_results = {}

for prefix, label in LISTS:
    url = f"https://www.amazon.com/gp/{prefix}/{slug}/{node_id}/"
    print(f"\n{'─' * 40}")
    print(f"▶ {label} ({prefix})")
    print(f"  URL: {url}")

    try:
        r = session.get(url, timeout=15)
        print(f"  HTTP: {r.status_code}, 大小: {len(r.text):,} bytes")
    except Exception as e:
        print(f"  请求失败: {e}")
        continue

    if r.status_code != 200:
        print(f"  跳过（非200）")
        continue

    # 保存原始 HTML 到文件（供后续调试）
    html_path = os.path.join(OUT_DIR, f"sample_{prefix}.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(r.text)
    print(f"  已保存: {html_path}")

    soup = BeautifulSoup(r.text, "html.parser")

    # 方法1: 找 zg-grid-general-faceout 容器（标准榜单商品卡片）
    items = soup.select("[id^='gridItemRoot']")
    if not items:
        items = soup.select(".zg-grid-general-faceout")
    if not items:
        items = soup.select(".a-list-item .zg-item-immersion")

    print(f"  找到商品卡片: {len(items)} 个")

    products = []
    for idx, item in enumerate(items[:3]):  # 只取前3个详细分析
        product = {"_idx": idx + 1}

        # ASIN（从链接提取）
        link = item.select_one("a[href*='/dp/']")
        if link:
            href = link.get("href", "")
            m = re.search(r"/dp/([A-Z0-9]{10})", href)
            if m:
                product["asin"] = m.group(1)
            product["product_url"] = "https://www.amazon.com" + href if href.startswith("/") else href

        # 商品名
        name_el = item.select_one(".p13n-sc-truncate, .p13n-sc-truncated, ._cDEzb_p13n-sc-css-line-clamp-3_g3dy1, .zg-text-center-align")
        if name_el:
            product["name"] = name_el.get_text(strip=True)
        else:
            name_el = item.select_one("a > span > div")
            if name_el:
                product["name"] = name_el.get_text(strip=True)

        # 图片
        img = item.select_one("img")
        if img:
            product["image_url"] = img.get("src", "")

        # 价格
        price_el = item.select_one(".p13n-sc-price, ._cDEzb_p13n-sc-price_3mJ9Z, .a-color-price")
        if price_el:
            product["price"] = price_el.get_text(strip=True)
        else:
            price_whole = item.select_one(".a-price .a-price-whole")
            price_frac  = item.select_one(".a-price .a-price-fraction")
            if price_whole:
                product["price"] = f"${price_whole.get_text(strip=True)}{price_frac.get_text(strip=True) if price_frac else ''}"

        # 评分
        rating_el = item.select_one(".a-icon-alt")
        if rating_el:
            product["rating"] = rating_el.get_text(strip=True)

        # 评论数
        review_el = item.select_one("a.a-size-small, span.a-size-small")
        if review_el:
            product["review_count"] = review_el.get_text(strip=True)

        # 排名（#1, #2...）
        rank_el = item.select_one(".zg-badge-text, .p13n-sc-uncoverable-faceout span.a-badge-text")
        if rank_el:
            product["rank"] = rank_el.get_text(strip=True)

        # 飙升榜特有：涨幅百分比
        pct_el = item.select_one(".zg-percent-change, .zg-sales-movement")
        if pct_el:
            product["pct_change"] = pct_el.get_text(strip=True)

        products.append(product)

    all_results[label] = {
        "prefix": prefix,
        "url": url,
        "item_count": len(items),
        "sample_products": products,
    }

    # 打印详细字段
    if products:
        print(f"\n  前{len(products)}个商品详情:")
        for p in products:
            print(f"    #{p.get('_idx','-')}: {p.get('name','?')[:50]}")
            for k, v in p.items():
                if k not in ("_idx", "name"):
                    val = str(v)[:80]
                    print(f"       {k}: {val}")
    else:
        print("  ⚠ 无法解析商品，HTML 结构可能不同")
        # 打印所有 class 名帮助调试
        all_classes = set()
        for el in soup.select("[class]"):
            for cls in el.get("class", []):
                if "zg" in cls.lower() or "p13n" in cls.lower():
                    all_classes.add(cls)
        print(f"  相关 CSS 类名: {sorted(all_classes)[:20]}")

# 保存汇总 JSON
summary_path = os.path.join(OUT_DIR, "recon_results.json")
with open(summary_path, "w", encoding="utf-8") as f:
    json.dump(all_results, f, ensure_ascii=False, indent=2)
print(f"\n\n{'=' * 80}")
print(f"汇总已保存: {summary_path}")

# 打印字段对比表
print(f"\n{'=' * 80}")
print("字段可用性对比:")
all_keys = set()
for v in all_results.values():
    for p in v.get("sample_products", []):
        all_keys.update(p.keys())
all_keys.discard("_idx")

print(f"{'字段':20s}", end="")
for _, label in LISTS:
    print(f"  {label:8s}", end="")
print()

for key in sorted(all_keys):
    print(f"{key:20s}", end="")
    for _, label in LISTS:
        has = any(key in p for p in all_results.get(label, {}).get("sample_products", []))
        print(f"  {'✅':8s}" if has else f"  {'❌':8s}", end="")
    print()
