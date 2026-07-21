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

_MARKETPLACE_CURRENCY_CODES = {
    "US": "USD", "UK": "GBP", "DE": "EUR", "FR": "EUR", "IT": "EUR",
    "ES": "EUR", "JP": "JPY", "NL": "EUR", "SE": "SEK", "PL": "PLN",
    "BE": "EUR", "CA": "CAD", "AU": "AUD", "IN": "INR", "MX": "MXN",
    "BR": "BRL", "SG": "SGD", "SA": "SAR", "AE": "AED", "TR": "TRY",
    "EG": "EGP",
}
for _site_code, _currency_code in _MARKETPLACE_CURRENCY_CODES.items():
    MARKETPLACES[_site_code]["currency_code"] = _currency_code

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

# ── 代理池 ──────────────────────────────────────────────────────────
def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or str(raw).strip() == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


def _default_clash_home() -> str:
    override = os.environ.get("CLASH_VERGE_HOME", "").strip()
    if override:
        return override
    appdata = os.environ.get("APPDATA", "").strip()
    if appdata:
        return os.path.join(appdata, "io.github.clash-verge-rev.clash-verge-rev")
    return ""


def _default_mihomo_bin() -> str:
    override = os.environ.get("MIHOMO_BIN", "").strip()
    if override:
        return override
    candidates = [
        os.path.join(os.environ.get("ProgramFiles", r"C:\Program Files"), "Clash Verge", "verge-mihomo.exe"),
        os.path.join(os.environ.get("ProgramFiles", r"C:\Program Files"), "Clash Verge", "mihomo.exe"),
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return candidates[0]


PROXY_ENABLED = _env_bool("PROXY_ENABLED", True)
PROXY_REQUIRED = _env_bool("PROXY_REQUIRED", True)
ALLOW_DIRECT_FALLBACK = _env_bool("ALLOW_DIRECT_FALLBACK", False)
PROXY_VERIFY = _env_bool("PROXY_VERIFY", False)

PROXY_POOL_FILE = os.path.join(DATA_DIR, "proxy_pool.json")
PROXY_POOL_CANDIDATE_FILE = os.path.join(DATA_DIR, "proxy_pool.candidate.json")
PROXY_POOL_LAST_GOOD_FILE = os.path.join(DATA_DIR, "proxy_pool.last_good.json")
PROXY_POOL_LAST_FAILED_FILE = os.path.join(DATA_DIR, "proxy_pool.last_failed.json")
PROXY_POOL_STATUS_FILE = os.path.join(DATA_DIR, "proxy_pool_status.json")
PROXY_PROBE_CACHE_FILE = os.path.join(DATA_DIR, "probe_results.json")
PROXY_LB_DIR = os.path.join(DATA_DIR, "lb_instance")
PROXY_PID_FILE = os.path.join(DATA_DIR, "lb_pid.json")
PROXY_LOCK_FILE = os.path.join(DATA_DIR, "proxy_pool.lock")

# 常驻验证守护进程：独立于 api_server / 抓取生命周期，一直运行、持续验证补充
PROXY_DAEMON_PID_FILE = os.path.join(DATA_DIR, "proxy_daemon_pid.json")
PROXY_DAEMON_LOG_FILE = os.path.join(DATA_DIR, "proxy_daemon.log")
PROXY_DAEMON_STATE_FILE = os.path.join(DATA_DIR, "proxy_daemon_state.json")
PROXY_DAEMON_PORT_MAP_FILE = os.path.join(DATA_DIR, "proxy_daemon_port_map.json")
# 抓取侧写入的活动心跳：守护进程据此在空闲时放缓验证节奏
PROXY_DAEMON_ACTIVITY_FILE = os.path.join(DATA_DIR, "proxy_daemon_activity.json")
# 抓取进程 → daemon 的可审计反馈队列与聚合质量指标（SQLite WAL）。
PROXY_EVENT_DB_FILE = os.path.join(DATA_DIR, "proxy_runtime_events.db")

CLASH_VERGE_HOME = _default_clash_home()
CLASH_PROFILES_META = os.path.join(CLASH_VERGE_HOME, "profiles.yaml") if CLASH_VERGE_HOME else ""
CLASH_PROFILES_DIR = os.path.join(CLASH_VERGE_HOME, "profiles") if CLASH_VERGE_HOME else ""
MIHOMO_BIN = _default_mihomo_bin()

# 可选信息源：主 Clash 控制口 / 混合口；不可用不得阻断代理池构建
CLASH_CTRL_API = os.environ.get("CLASH_CTRL_API", "http://127.0.0.1:9097").rstrip("/")
CLASH_MIXED_PORT = _env_int("CLASH_MIXED_PORT", 7897)

PROXY_BASE_PORT = _env_int("PROXY_BASE_PORT", 18001)
PROXY_CTRL_PORT = _env_int("PROXY_CTRL_PORT", 19897)
PROXY_PORT_RANGE_END = _env_int("PROXY_PORT_RANGE_END", 18100)
PROXY_MAX_NODES = _env_int("PROXY_MAX_NODES", 0)  # 0 = 不限

# 分层水位：8 启动、10 警戒、14 常态目标、16 热池上限。目标以上的
# 已验证节点进入温池，不丢弃；热池跌落时无需重新验证即可晋升补位。
PROXY_MIN_START_NODES = max(1, _env_int("PROXY_MIN_START_NODES", 8))
PROXY_POOL_LOW_WATERMARK = max(
    PROXY_MIN_START_NODES, _env_int("PROXY_POOL_LOW_WATERMARK", 10)
)
PROXY_POOL_TARGET_NODES = max(
    PROXY_POOL_LOW_WATERMARK, _env_int("PROXY_POOL_TARGET_NODES", 14)
)
PROXY_POOL_HOT_MAX_NODES = max(
    PROXY_POOL_TARGET_NODES, _env_int("PROXY_POOL_HOT_MAX_NODES", 16)
)
# 兼容旧冷启动体检调用方：硬门槛是 8；常态扩容目标单独由
# PROXY_POOL_TARGET_NODES 表达，不再用一个变量混用两种语义。
PROXY_MIN_VERIFIED_NODES = _env_int("PROXY_MIN_VERIFIED_NODES", PROXY_MIN_START_NODES)
PROXY_MIN_UNIQUE_IPS = _env_int("PROXY_MIN_UNIQUE_IPS", PROXY_MIN_START_NODES)
PROXY_MIN_AMAZON_OK = _env_int("PROXY_MIN_AMAZON_OK", PROXY_MIN_START_NODES)

PROXY_HEALTH_CONCURRENCY = _env_int("PROXY_HEALTH_CONCURRENCY", 20)
PROXY_CONNECT_TIMEOUT = _env_int("PROXY_CONNECT_TIMEOUT", 5)
PROXY_READ_TIMEOUT = _env_int("PROXY_READ_TIMEOUT", 8)
PROXY_PROBE_CACHE_TTL = _env_int("PROXY_PROBE_CACHE_TTL", 3600)
PROXY_POOL_MAX_AGE = _env_int("PROXY_POOL_MAX_AGE", 600)
PROXY_IDLE_TTL = _env_int("PROXY_IDLE_TTL", 1800)

# 抓取运行期代理调度：正常请求定期轮换，风控/网络故障立即换独立出口。
PROXY_RUNTIME_MIN_USABLE = _env_int("PROXY_RUNTIME_MIN_USABLE", PROXY_MIN_START_NODES)
# 短租约：同一 worker/IP 默认连续 3 个请求或最多 5 分钟；错误立即结束租约。
PROXY_ROTATE_REQUESTS = _env_int("PROXY_ROTATE_REQUESTS", 3)
PROXY_ROTATE_TTL = _env_int("PROXY_ROTATE_TTL", 300)
PROXY_MAX_ACTIVE_PER_PREFIX = _env_int("PROXY_MAX_ACTIVE_PER_PREFIX", 2)
PROXY_CAPTCHA_COOLDOWN = _env_int("PROXY_CAPTCHA_COOLDOWN", 600)
PROXY_RATE_LIMIT_COOLDOWN = _env_int("PROXY_RATE_LIMIT_COOLDOWN", 600)
PROXY_ERROR_COOLDOWN = _env_int("PROXY_ERROR_COOLDOWN", 120)
PROXY_REQUEST_DISTINCT_ATTEMPTS = _env_int("PROXY_REQUEST_DISTINCT_ATTEMPTS", 3)
PROXY_RUNTIME_RECOVERY_ATTEMPTS = _env_int("PROXY_RUNTIME_RECOVERY_ATTEMPTS", 1)

# ── 常驻验证守护进程（proxy_daemon.py）调度参数 ──────────────────────
# 调度器主循环节拍：每次醒来检查是否有到期任务可派发
PROXY_DAEMON_TICK_INTERVAL_SEC = _env_int("PROXY_DAEMON_TICK_INTERVAL_SEC", 3)
# 热池若没有真实抓取成功反馈，20 分钟做一次贴近目标站的完整复核；
# 温池/失败候选保证 60 分钟内至少获得一次滚动复查机会。
PROXY_DAEMON_RECHECK_INTERVAL_SEC = _env_int("PROXY_DAEMON_RECHECK_INTERVAL_SEC", 1200)
PROXY_DAEMON_WARM_RECHECK_INTERVAL_SEC = _env_int("PROXY_DAEMON_WARM_RECHECK_INTERVAL_SEC", 3600)
PROXY_DAEMON_FULL_SCAN_INTERVAL_SEC = _env_int("PROXY_DAEMON_FULL_SCAN_INTERVAL_SEC", 3600)
PROXY_DAEMON_EXPLORATION_INTERVAL_SEC = _env_int("PROXY_DAEMON_EXPLORATION_INTERVAL_SEC", 45)
# 复核间隔抖动比例（±），打散同批节点同时到期造成的请求脉冲
PROXY_DAEMON_RECHECK_JITTER_PCT = float(os.environ.get("PROXY_DAEMON_RECHECK_JITTER_PCT", "0.15") or "0.15")
# 轻量哨兵复核（仅 exit-IP）连续通过 N 次后，才做一次完整 Amazon 校验
PROXY_DAEMON_LIGHT_RECHECKS_BEFORE_FULL = _env_int("PROXY_DAEMON_LIGHT_RECHECKS_BEFORE_FULL", 1)
# 验证失败节点的重试退避（指数增长，避免反复打已知坏节点）
PROXY_DAEMON_RETRY_BACKOFF_BASE_SEC = _env_int("PROXY_DAEMON_RETRY_BACKOFF_BASE_SEC", 60)
PROXY_DAEMON_RETRY_BACKOFF_MAX_SEC = _env_int("PROXY_DAEMON_RETRY_BACKOFF_MAX_SEC", 1800)
# 验证并发度（守护进程内部线程池大小）；空闲时降到 IDLE_CONCURRENCY
PROXY_DAEMON_CONCURRENCY = _env_int("PROXY_DAEMON_CONCURRENCY", 10)
PROXY_DAEMON_IDLE_CONCURRENCY = _env_int("PROXY_DAEMON_IDLE_CONCURRENCY", 3)
# 无抓取活动超过该秒数后进入空闲模式（复核间隔 × 倍数、并发降级）
PROXY_DAEMON_IDLE_AFTER_SEC = _env_int("PROXY_DAEMON_IDLE_AFTER_SEC", 600)
PROXY_DAEMON_IDLE_RECHECK_MULTIPLIER = _env_int("PROXY_DAEMON_IDLE_RECHECK_MULTIPLIER", 3)
# 订阅指纹变化检测间隔；需连续确认次数一致才应用（防半写文件抖动）
PROXY_DAEMON_SUBSCRIPTION_POLL_SEC = _env_int("PROXY_DAEMON_SUBSCRIPTION_POLL_SEC", 300)
PROXY_DAEMON_SUBSCRIPTION_CONFIRM_POLLS = _env_int("PROXY_DAEMON_SUBSCRIPTION_CONFIRM_POLLS", 2)
# 发布/状态文件写入去抖间隔，避免状态密集变化时频繁刷盘
PROXY_DAEMON_PUBLISH_DEBOUNCE_SEC = _env_int("PROXY_DAEMON_PUBLISH_DEBOUNCE_SEC", 2)
PROXY_DAEMON_FEEDBACK_DEDUPE_SEC = _env_int("PROXY_DAEMON_FEEDBACK_DEDUPE_SEC", 60)
PROXY_DAEMON_FEEDBACK_BATCH = _env_int("PROXY_DAEMON_FEEDBACK_BATCH", 1000)
# 蓝绿切换时临时端口基址（与主端口段错开，避免监听冲突）
PROXY_DAEMON_BLUE_GREEN_BASE_PORT = _env_int("PROXY_DAEMON_BLUE_GREEN_BASE_PORT", 18201)

# ── 抓取侧消费活池的热重载/等待参数 ──────────────────────────────────
# ForcedProxyPool 后台线程重新读取活池文件的周期
# 抓取反馈 -> daemon -> 活池 -> worker 的最坏收敛时间控制在 15s 内。
PROXY_POOL_LIVE_REFRESH_SEC = _env_int("PROXY_POOL_LIVE_REFRESH_SEC", 5)
# acquire() 发现可用数低于 min_usable 时，最多等待守护进程补充这么久，
# 超时才真正报错（取代过去“立刻崩溃重启整进程”的粗粒度恢复）
PROXY_POOL_WAIT_FOR_REPLENISH_SEC = _env_int("PROXY_POOL_WAIT_FOR_REPLENISH_SEC", 300)
# 可用节点少于目标规模时，请求间隔放大系数（缓解单出口限流）
PROXY_LOW_POOL_DELAY_SCALE = float(os.environ.get("PROXY_LOW_POOL_DELAY_SCALE", "2.0") or "2.0")
# 抓取 worker 动态扩容：上限与检查间隔
PROXY_MAX_CRAWL_WORKERS = _env_int("PROXY_MAX_CRAWL_WORKERS", 20)
PROXY_WORKER_SCALE_INTERVAL_SEC = _env_int("PROXY_WORKER_SCALE_INTERVAL_SEC", 15)

PROXY_IP_ENDPOINTS = (
    "https://api.ipify.org",
    "https://checkip.amazonaws.com",
    "https://ifconfig.me/ip",
)
# 注意：不要用 robots.txt 作为可达性判据——反爬系统通常不拦截 robots.txt，
# 实测对 69 个节点验证时 0 个因 Amazon 不可达/验证码被淘汰，双层验证形同虚设。
# 首页会真正触发反爬路径判定，才能体现"能访问IP检测服务≠能访问Amazon"的差异。
PROXY_AMAZON_CHECK_URL = os.environ.get(
    "PROXY_AMAZON_CHECK_URL", "https://www.amazon.com/"
)

PROXY_SKIP_PROTOCOLS = frozenset(
    x.strip().lower()
    for x in os.environ.get("PROXY_SKIP_PROTOCOLS", "hysteria").split(",")
    if x.strip()
)
# 默认不再来源层剔除 CDN（共享出口改由健康检查按 exit_ip 去重）。
# 如需恢复旧行为：设置 PROXY_CDN_SERVERS=host1,host2
PROXY_CDN_SERVERS = frozenset(
    x.strip().lower()
    for x in os.environ.get("PROXY_CDN_SERVERS", "").split(",")
    if x.strip()
)
PROXY_IPROYAL_MARKERS = ("iproyal", "ip royal")
PROXY_GEO_DB_FILES = ("Country.mmdb", "geoip.dat", "geosite.dat")

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
