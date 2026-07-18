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

# ── 多站点配置（与看板下拉对齐；fba_supported 见 fba_fees_us.FBA_SUPPORTED）──
def _mp(domain, lang, name, currency, decimal_sep=".", rating=r"([\d.]+)\s+out",
        results=r"of\s+([\d,]+)\s+results?", fba_supported=False):
    return {
        "domain": domain, "lang": lang, "name": name, "currency": currency,
        "decimal_sep": decimal_sep, "rating_pattern": rating,
        "results_pattern": results, "fba_supported": fba_supported,
    }

MARKETPLACES = {
    "US": _mp("https://www.amazon.com", "en-US,en;q=0.9", "美国站", "$", fba_supported=True),
    "UK": _mp("https://www.amazon.co.uk", "en-GB,en;q=0.9", "英国站", "£", fba_supported=True),
    "DE": _mp("https://www.amazon.de", "de-DE,de;q=0.9,en;q=0.5", "德国站", "€", ",",
              r"([\d,]+)\s+von", r"([\d.]+)\s+Ergebnisse", True),
    "FR": _mp("https://www.amazon.fr", "fr-FR,fr;q=0.9,en;q=0.5", "法国站", "€", ",",
              r"([\d,]+)\s+sur", r"([\d\s]+)\s+résultats?", True),
    "IT": _mp("https://www.amazon.it", "it-IT,it;q=0.9,en;q=0.5", "意大利站", "€", ",",
              r"([\d,]+)\s+su", r"([\d.]+)\s+risultati", True),
    "ES": _mp("https://www.amazon.es", "es-ES,es;q=0.9,en;q=0.5", "西班牙站", "€", ",",
              r"([\d,]+)\s+de", r"([\d.]+)\s+resultados", True),
    "JP": _mp("https://www.amazon.co.jp", "ja-JP,ja;q=0.9,en;q=0.5", "日本站", "¥",
              rating=r"5つ星のうち([\d.]+)", results=r"([\d,]+)\s*件中", fba_supported=True),
    "NL": _mp("https://www.amazon.nl", "nl-NL,nl;q=0.9,en;q=0.5", "荷兰站", "€", ",", fba_supported=True),
    "SE": _mp("https://www.amazon.se", "sv-SE,sv;q=0.9,en;q=0.5", "瑞典站", "kr", ",", fba_supported=True),
    "PL": _mp("https://www.amazon.pl", "pl-PL,pl;q=0.9,en;q=0.5", "波兰站", "zł", ",", fba_supported=True),
    "BE": _mp("https://www.amazon.com.be", "fr-BE,fr;q=0.9,nl;q=0.8,en;q=0.5", "比利时站", "€", ",", fba_supported=True),
    "CA": _mp("https://www.amazon.ca", "en-CA,en;q=0.9", "加拿大站", "CA$", fba_supported=True),
    "AU": _mp("https://www.amazon.com.au", "en-AU,en;q=0.9", "澳大利亚站", "A$", fba_supported=True),
    "IN": _mp("https://www.amazon.in", "en-IN,en;q=0.9", "印度站", "₹", fba_supported=True),
    "MX": _mp("https://www.amazon.com.mx", "es-MX,es;q=0.9,en;q=0.5", "墨西哥站", "MX$", ",",
              fba_supported=True),
    "BR": _mp("https://www.amazon.com.br", "pt-BR,pt;q=0.9,en;q=0.5", "巴西站", "R$", ",",
              fba_supported=True),
    "SG": _mp("https://www.amazon.sg", "en-SG,en;q=0.9", "新加坡站", "S$", fba_supported=True),
    "SA": _mp("https://www.amazon.sa", "ar-AE,ar;q=0.9,en;q=0.5", "沙特站", "SAR ",
              fba_supported=True),
    "AE": _mp("https://www.amazon.ae", "en-AE,en;q=0.9", "阿联酋站", "AED ", fba_supported=True),
    "TR": _mp("https://www.amazon.com.tr", "tr-TR,tr;q=0.9,en;q=0.5", "土耳其站", "₺", ",",
              fba_supported=True),
    "EG": _mp("https://www.amazon.eg", "ar-EG,ar;q=0.9,en;q=0.5", "埃及站", "E£", fba_supported=True),
}

def get_marketplace(site: str = "US") -> dict:
    site = site.upper()
    if site not in MARKETPLACES:
        raise ValueError(f"未知站点: {site}，可选: {', '.join(MARKETPLACES)}")
    return MARKETPLACES[site]

# ── 阶段1：类目树 ─────────────────────────────────────────────────
# 可用环境变量 DB_FILE / AMZ_DB_FILE 覆盖（测试隔离用）；默认正式库路径不变
DB_FILE            = (
    os.environ.get("AMZ_DB_FILE")
    or os.environ.get("DB_FILE")
    or os.path.join(DATA_DIR, "categories.db")
)


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
PRODUCT_LISTS = ["new-releases", "bestsellers", "movers-and-shakers", "most-wished-for", "most-gifted"]

# 并发
PRODUCT_WORKERS = 10

# 输出
PRODUCTS_DB_TABLE = "product_sightings"
PRODUCTS_EXCEL    = os.path.join(DATA_DIR, "products.xlsx")
