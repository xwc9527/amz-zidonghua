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

# ── 阶段1：类目树 ─────────────────────────────────────────────────
CATEGORIES_FILE    = os.path.join(DATA_DIR, "categories.json")   # 旧，保留兼容
DB_FILE            = os.path.join(DATA_DIR, "categories.db")      # 新，主存储
NEW_RELEASES_ROOT  = "https://www.amazon.com/gp/new-releases/"

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
