# config.py — 全局配置，所有脚本从这里读，不在业务脚本里硬编码

import os

# ── 路径 ──────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DATA_DIR   = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# ── 请求头（固定UA，不依赖fake_useragent） ────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ── 请求延迟（秒）────────────────────────────────────────────────
DELAY_MIN = 2.0   # 类目页最小延迟
DELAY_MAX = 4.0   # 类目页最大延迟
DELAY_PRODUCT_MIN = 2.5   # 产品详情页最小延迟（风险更高）
DELAY_PRODUCT_MAX = 4.5   # 产品详情页最大延迟

# ── 多站点配置 ────────────────────────────────────────────────────
MARKETPLACES = {
    "US": {
        "domain": "https://www.amazon.com",
        "lang":   "en-US,en;q=0.9",
        "name":   "美国站",
        "currency": "$",
        "decimal_sep": ".",
        "rating_pattern": r"([\d.]+)\s+out",
        "results_pattern": r"of\s+([\d,]+)\s+results?",
    },
    "DE": {
        "domain": "https://www.amazon.de",
        "lang":   "de-DE,de;q=0.9,en;q=0.5",
        "name":   "德国站",
        "currency": "€",
        "decimal_sep": ",",
        "rating_pattern": r"([\d,]+)\s+von",
        "results_pattern": r"([\d.]+)\s+Ergebnisse",
    },
    "JP": {
        "domain": "https://www.amazon.co.jp",
        "lang":   "ja-JP,ja;q=0.9,en;q=0.5",
        "name":   "日本站",
        "currency": "¥",
        "decimal_sep": ".",
        "rating_pattern": r"5つ星のうち([\d.]+)",
        "results_pattern": r"([\d,]+)\s*件中",
    },
    "UK": {
        "domain": "https://www.amazon.co.uk",
        "lang":   "en-GB,en;q=0.9",
        "name":   "英国站",
        "currency": "£",
        "decimal_sep": ".",
        "rating_pattern": r"([\d.]+)\s+out",
        "results_pattern": r"of\s+([\d,]+)\s+results?",
    },
    "FR": {
        "domain": "https://www.amazon.fr",
        "lang":   "fr-FR,fr;q=0.9,en;q=0.5",
        "name":   "法国站",
        "currency": "€",
        "decimal_sep": ",",
        "rating_pattern": r"([\d,]+)\s+sur",
        "results_pattern": r"([\d\s]+)\s+résultats?",
    },
}

def get_marketplace(site: str = "US") -> dict:
    site = site.upper()
    if site not in MARKETPLACES:
        raise ValueError(f"未知站点: {site}，可选: {', '.join(MARKETPLACES)}")
    return MARKETPLACES[site]

# ── 阶段1：类目树 ─────────────────────────────────────────────────
DB_FILE            = os.path.join(DATA_DIR, "categories.db")


# ── 阶段2：新品榜扫描 ─────────────────────────────────────────────
RAW_PRODUCTS_FILE  = os.path.join(DATA_DIR, "raw_products.json")

# 初步过滤条件（宽松，精细过滤在阶段4）
FILTER_PRICE_MIN   = 15    # 美元
FILTER_PRICE_MAX   = 60    # 美元
FILTER_REVIEWS_MAX = 100   # 评论数上限（阶段2粗筛）

# ── 阶段3：竞争度 ─────────────────────────────────────────────────
PRODUCTS_SCORED_FILE = os.path.join(DATA_DIR, "products_scored.json")

# ── 阶段4：输出 ───────────────────────────────────────────────────
EXCEL_FILE         = os.path.join(OUTPUT_DIR, "候选品.xlsx")

# 精细过滤条件
FILTER_COMPETITION_MAX = 5000
FILTER_REVIEWS_FINAL   = 50

# ── 代理池（start_lb_proxy.py 生成的 proxy_pool.json）────────────────
PROXY_ENABLED   = True
PROXY_POOL_FILE = os.path.join(DATA_DIR, "proxy_pool.json")
PROXY_VERIFY    = False

# ── 阶段5：商品抓取（榜单批量扫描）─────────────────────────────────
# 通用筛选条件（看板 UI 可覆盖）
PRODUCT_REVIEW_MAX    = 10    # 评论数上限（< 此值才录入）
PRODUCT_MIN_LIST_SIZE = 100   # 榜单最少商品数（活体检测）
PRODUCT_PRICE_MIN     = 0.0   # 价格下限（0 = 不限）
PRODUCT_PRICE_MAX     = 0.0   # 价格上限（0 = 不限）

# 抓取哪些榜单
PRODUCT_LISTS = ["new-releases", "bestsellers", "most-wished-for"]

# 并发
PRODUCT_WORKERS = 10

# 输出
PRODUCTS_DB_TABLE = "product_sightings"
PRODUCTS_EXCEL    = os.path.join(DATA_DIR, "products.xlsx")

