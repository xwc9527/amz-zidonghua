"""
fetch_products.py — 商品抓取脚本（独立进程）
从 categories.db 读取有效节点，抓取 3 个 SSR 榜单的商品数据
用法: python fetch_products.py
"""
import sqlite3, threading, time, sys, os, re, json, argparse, logging, traceback, random, hashlib
from concurrent.futures import ProcessPoolExecutor
from queue import PriorityQueue, Queue
from curl_cffi import requests as requests
from datetime import datetime
from bs4 import BeautifulSoup
from fba_fees_us import estimate_fba_fees
from detail_parser import (
    parse_detail_fields as _parse_detail_fields_shared,
    check_detail_filters as _check_detail_filters_shared,
    active_filter_none_flags as _active_filter_none_flags,
    attach_normalized_dims,
    extract_image_url,
)
from crawl_checkpoint import ProductsCheckpoint, canonical_signature
from crawler_hotpath_experiment import AsyncBatchWriter
from config import (
    DATA_DIR, DB_FILE, PROXY_MIN_START_NODES, PROXY_VERIFY, get_marketplace,
    assert_testing_paths_safe, use_run_cache,
)
from proxy_daemon import touch_crawl_activity
import product_run_cache as run_cache

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ── 日志 ──────────────────────────────────────────────────────────
os.makedirs(DATA_DIR, exist_ok=True)
LOG_PATH = os.path.abspath(
    os.environ.get("AMZ_FETCH_PRODUCTS_LOG")
    or os.path.join(DATA_DIR, "fetch_products.log")
)
_log = logging.getLogger("fetch_products")
_log.setLevel(logging.DEBUG)
_fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
_fh.setLevel(logging.DEBUG)
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
_sh = logging.StreamHandler(sys.stdout)
_sh.setLevel(logging.INFO)
_sh.setFormatter(logging.Formatter("%(message)s"))
_log.addHandler(_fh)
_log.addHandler(_sh)

# ── 配置 ────────────────────────────────────────────────────────────
BASE    = os.path.dirname(os.path.abspath(__file__))
# 尊重 DB_FILE / AMZ_DB_FILE（测试隔离与正式库均可覆盖）
DB_PATH = DB_FILE
DB_BACKEND = os.getenv("DB_BACKEND", "pg")

_mp     = get_marketplace("US")
_SITE   = "US"
_DOMAIN = _mp["domain"]
_LANG   = _mp["lang"]
_CURRENCY     = _mp["currency"]
_DECIMAL_SEP  = _mp["decimal_sep"]
_RATING_PAT   = _mp["rating_pattern"]
_RESULTS_PAT  = _mp["results_pattern"]
_RUN_ID = os.getenv("AMZ_RUN_ID") or datetime.now().strftime("PS-%Y%m%d-%H%M%S")
_AUDIT_PATH = os.path.abspath(
    os.environ.get("AMZ_FETCH_PRODUCTS_AUDIT_LOG")
    or os.path.join(DATA_DIR, "fetch_products_attempts.jsonl")
)

# PG support
_pg_conn = None
def _get_pg():
    global _pg_conn
    if _pg_conn is None or _pg_conn.closed:
        import psycopg2
        from pg_config import get_pg_dsn
        _pg_conn = psycopg2.connect(get_pg_dsn())
        _pg_conn.autocommit = True
    return _pg_conn

# 默认值（可被看板 API 参数覆盖）
DEFAULT_REVIEW_MAX    = 10
DEFAULT_MIN_LIST_SIZE = 100
DEFAULT_PRICE_MIN     = 0.0
DEFAULT_PRICE_MAX     = 0.0
DEFAULT_DELAY         = 2.0   # 请求间隔（秒）
DEFAULT_LISTS         = ["new-releases", "bestsellers", "most-wished-for", "most-gifted"]
PRODUCT_STREAMS_PER_PROXY = max(1, int(os.getenv("PRODUCT_STREAMS_PER_PROXY", "3")))
PRODUCT_TASK_MAX_ATTEMPTS = max(1, int(os.getenv("PRODUCT_TASK_MAX_ATTEMPTS", "3")))
PRODUCT_PARSE_WORKERS = max(1, int(os.getenv("PRODUCT_PARSE_WORKERS", "3")))
PRODUCT_PARSE_MAX_PENDING = max(
    PRODUCT_PARSE_WORKERS,
    int(os.getenv("PRODUCT_PARSE_MAX_PENDING", str(PRODUCT_PARSE_WORKERS * 4))),
)
PRODUCT_DETAIL_WRITE_BATCH_SIZE = max(
    1, int(os.getenv("PRODUCT_DETAIL_WRITE_BATCH_SIZE", "100")),
)
PRODUCT_DETAIL_WRITE_FLUSH_SEC = max(
    0.05, float(os.getenv("PRODUCT_DETAIL_WRITE_FLUSH_SEC", "0.25")),
)
PRODUCT_LANE_CAPTCHA_MIN_SAMPLES = max(
    1, int(os.getenv("PRODUCT_LANE_CAPTCHA_MIN_SAMPLES", "10")),
)
PRODUCT_LANE_CAPTCHA_PAUSE_RATE = max(
    0.0, min(1.0, float(os.getenv("PRODUCT_LANE_CAPTCHA_PAUSE_RATE", "0.5"))),
)
PRODUCT_MOPUP_MAX_ATTEMPTS = max(1, int(os.getenv("PRODUCT_MOPUP_MAX_ATTEMPTS", "3")))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
}

from proxy_session import (
    ForcedProxyPool,
    ProxyRequiredError,
    assert_session_has_proxy,
    make_forced_session,
)
from proxy_worker import (
    AttemptAuditor,
    FetchOutcome,
    WorkerProxyClient as SharedWorkerProxyClient,
    has_us_currency_mismatch,
    is_captcha_page,
    pool_aware_delay,
    raise_if_pool_below_minimum,
)


def _pool_delay(delay: float, pool=None) -> float:
    """在基础 delay 上按可用代理数放大（低可用时更保守）。"""
    usable = PROXY_MIN_START_NODES
    try:
        if pool is not None:
            usable = pool.usable_count
    except Exception:
        pass
    return pool_aware_delay(delay, delay * 1.5, usable)

LIST_LABELS = {
    "new-releases":       "新品榜",
    "bestsellers":        "畅销榜",
    "most-wished-for":    "心愿单",
    "most-gifted":        "礼品榜",
}

# 榜单名 → categories 表列名（用于写入验证结果）
LIST_COL_MAP = {
    "new-releases":       "nr_valid",
    "bestsellers":        "bs_valid",
    "most-wished-for":    "mw_valid",
}

_db_lock = threading.Lock()
_stats = {"total_nodes": 0, "done_nodes": 0, "skipped": 0,
          "products_found": 0, "products_saved": 0, "products_dup": 0, "errors": 0,
          "pool_usable": 0, "pool_cooling": 0, "pool_disabled": 0}
_stats_lock = threading.Lock()
# 字段解析探针：按维度累计"该维度筛选已启用时，字段解析为 None"的比例。
# 用于在跑完后自动识别"选择器过期导致字段恒为 None"这类静默失效（不报错、
# 只是结果异常少），而不是等人工去猜"是不是没有商品达标"。
_filter_field_probe: dict[str, dict[str, int]] = {}
_seen_asins = set()
_seen_lock = threading.Lock()
_checkpoint: ProductsCheckpoint | None = None
_detail_parse_pipeline = None
_detail_writer = None
_category_reverse_lock = threading.Lock()
_category_reverse_seen_edges: set[tuple[str, str, str]] = set()
_category_reverse_seen_db_path = ""


class BoundedDetailParsePipeline:
    """Process detail HTML with bounded submitted-but-unfinished work."""

    def __init__(self, workers: int, max_pending: int):
        if workers < 1 or max_pending < workers:
            raise ValueError("workers must be >=1 and max_pending must be >= workers")
        self._executor = ProcessPoolExecutor(max_workers=workers)
        self._slots = threading.BoundedSemaphore(max_pending)
        self._closed = False
        self._state_lock = threading.Lock()

    def submit(self, html: str, site: str):
        with self._state_lock:
            if self._closed:
                raise RuntimeError("detail parse pipeline is closed")
        self._slots.acquire()
        try:
            future = self._executor.submit(_parse_detail_fields_shared, html, site)
        except BaseException:
            self._slots.release()
            raise
        future.add_done_callback(lambda _future: self._slots.release())
        return future

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False):
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)

# ── 代理池（强制代理 + 独立出口轮换，与最新到货共用 Worker）────────


class ProxyPool(ForcedProxyPool):
    def __init__(self):
        # min_usable 用 PROXY_MIN_START_NODES（默认 8）：常驻验证守护进程持续
        # 在后台增补节点，这里不再要求启动时就凑够一大批；enable_live_reload
        # 让运行期间能持续感知守护进程新增/淘汰的节点。
        super().__init__(
            required=True, min_usable=PROXY_MIN_START_NODES, enable_live_reload=True,
        )
        _log.info(f"[pool] 强制加载 {self.size} 个代理端口（min_usable={self.min_usable}，活池热重载已开启）")


def _has_marketplace_currency_mismatch(text: str) -> bool:
    if _SITE != "US":
        return False
    return has_us_currency_mismatch(text)


def _make_session(worker_id: int, proxy_entry: dict) -> requests.Session:
    ua = USER_AGENTS[worker_id % len(USER_AGENTS)]
    hdrs = {
        **HEADERS,
        "User-Agent": ua,
        "Accept-Language": _LANG,
    }
    session = make_forced_session(proxy_entry, headers=hdrs, required=True)
    currency_code = _mp.get("currency_code")
    if currency_code:
        session.cookies.set("i18n-prefs", currency_code)
    assert_session_has_proxy(session, required=True)
    return session


def _warmup(session: requests.Session):
    assert_session_has_proxy(session, required=True)
    proxy = (getattr(session, "proxies", None) or {}).get("https") or ""
    try:
        session.get(f"{_DOMAIN}/", timeout=10)
        _log.info(f"[session] warmup ok proxy={proxy}")
        time.sleep(1 + random.uniform(0, 1))
    except Exception as e:
        _log.warning(f"[session] warmup 失败 proxy={proxy}: {e}")


class WorkerProxyClient(SharedWorkerProxyClient):
    def __init__(
        self,
        pool: ProxyPool,
        worker_id: int = 0,
        *,
        warmup: bool = False,
        verify_exit: bool = False,
    ):
        super().__init__(
            pool,
            worker_id,
            make_session=_make_session,
            warmup=_warmup if warmup else None,
            auditor=AttemptAuditor(_AUDIT_PATH, _RUN_ID),
            is_captcha=is_captcha_page,
            is_currency_mismatch=_has_marketplace_currency_mismatch,
            verify_exit=verify_exit,
        )


class FixedProxyClient:
    """One long-lived Session pinned to one proxy; never acquires a lease."""

    def __init__(
        self, pool: ProxyPool, proxy_entry: dict, worker_id: int,
        auditor: AttemptAuditor | None = None,
        on_result: "Callable[[FetchOutcome], None] | None" = None,
    ):
        self.pool = pool
        self.entry = dict(proxy_entry)
        self.worker_id = worker_id
        self.session = _make_session(worker_id, self.entry)
        self.auditor = auditor or AttemptAuditor(_AUDIT_PATH, _RUN_ID)
        self.on_result = on_result
        self.proxy_key = str(
            self.entry.get("exit_ip")
            or self.entry.get("node_key")
            or self.entry.get("proxy")
            or f"worker-{worker_id}"
        )

    def get(self, url: str, *, phase: str, item_id: str, referer: str = "") -> FetchOutcome:
        started = time.monotonic()
        status_code = None
        html = None
        reason = ""
        ok = False
        try:
            self.pool.wait_if_target_paused()
            headers = {"Referer": referer} if referer else None
            response = self.session.get(
                url, headers=headers, timeout=18, verify=PROXY_VERIFY,
            )
            status_code = int(response.status_code)
            html = response.text or ""
            if status_code != 200:
                reason = f"HTTP_{status_code}"
            elif is_captcha_page(html):
                reason = "CAPTCHA"
            elif _has_marketplace_currency_mismatch(html):
                reason = "CURRENCY_MISMATCH"
            else:
                ok = True
        except Exception as exc:
            reason = f"{type(exc).__name__}: {exc}"

        elapsed_ms = int((time.monotonic() - started) * 1000)
        result = "SUCCESS" if ok else "FAILED"
        self.auditor.write(
            phase=phase,
            item_id=item_id,
            worker=self.worker_id,
            attempt=1,
            result=result,
            reason=reason,
            status_code=status_code,
            exit_ips=[self.proxy_key],
            elapsed_ms=elapsed_ms,
            rotated=False,
            fixed_proxy=True,
        )
        outcome = FetchOutcome(
            ok=ok,
            html=html if ok else None,
            error_code="" if ok else "FIXED_PROXY_REQUEST_FAILED",
            final_reason=reason,
            status_code=status_code,
            attempts=1,
            exit_ips=[self.proxy_key],
            reasons=[] if ok else [reason],
            elapsed_ms=elapsed_ms,
        )
        if self.on_result is not None:
            self.on_result(outcome)
        return outcome

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass


def _update_pool_stats(pool: ProxyPool):
    snap = pool.health_snapshot()
    with _stats_lock:
        _stats["pool_usable"] = snap["usable"]
        _stats["pool_cooling"] = snap["cooling"]
        _stats["pool_disabled"] = snap["disabled"]


# ── DB 工具 ─────────────────────────────────────────────────────────

_DETAIL_COLS = [
    ("bsr_main_rank", "INTEGER"),
    ("bsr_main_category", "TEXT"),
    ("bsr_sub_rank", "INTEGER"),
    ("bsr_sub_category", "TEXT"),
    ("variant_option_count", "INTEGER"),
    ("other_sellers_count", "INTEGER"),
    ("social_proof", "TEXT"),
    ("social_proof_count", "INTEGER"),
    ("item_weight", "TEXT"),
    ("item_dimensions", "TEXT"),
    ("weight_lb", "REAL"),
    ("dim_l_in", "REAL"),
    ("dim_w_in", "REAL"),
    ("dim_h_in", "REAL"),
    ("date_first_available", "TEXT"),
    ("shipping_fee", "TEXT"),
    ("shipping_fee_value", "REAL"),
    ("fba_fee", "REAL"),
    ("placement_fee", "REAL"),
    ("fulfillment_type", "TEXT"),
    ("country_of_origin", "TEXT"),
    ("is_bestseller", "INTEGER DEFAULT 0"),
    ("detail_scraped", "INTEGER DEFAULT 0"),
    ("run_id", "TEXT"),
]

# 可从已成功详情行复用的字段（不含详情状态本身）。
# 不含 price / fba_fee / placement_fee：价格随榜单变化，费用需按当前价重算。
_CACHED_DETAIL_KEYS = [
    "bsr_main_rank", "bsr_main_category", "bsr_sub_rank", "bsr_sub_category",
    "variant_option_count", "other_sellers_count",
    "social_proof", "social_proof_count",
    "item_weight", "item_dimensions",
    "weight_lb", "dim_l_in", "dim_w_in", "dim_h_in",
    "date_first_available", "shipping_fee", "shipping_fee_value",
    "fulfillment_type", "country_of_origin",
    "is_bestseller", "is_amazon_choice",
]


def _attach_normalized_dims(d: dict) -> dict:
    return attach_normalized_dims(d)


def parse_detail_fields(html: str) -> dict:
    """兼容原调用：内部转发到共享模块并注入当前站点。"""
    return _parse_detail_fields_shared(html, _SITE)


def _reverse_category_url(node: dict, parent_slug: str) -> str:
    """Build a chart URL for a detail-page-discovered category without fetching it."""
    node_id = node["node_id"]
    slug = node.get("slug") or parent_slug
    if node.get("kind") == "bsr" and node.get("slug"):
        return f"{_DOMAIN}/gp/bestsellers/{node['slug']}/{node_id}/"
    if slug:
        return f"{_DOMAIN}/gp/new-releases/{slug}/{node_id}/"
    return f"{_DOMAIN}/gp/new-releases/{node_id}/"


def _discover_category_nodes_from_detail(detail: dict, site: str = None) -> int:
    """Persist previously unseen category edges inferred from one parsed detail page.

    This intentionally consumes only parser output: no request is made here.  The
    in-process edge cache avoids opening SQLite for the many products that share a
    breadcrumb, while the database unique key remains the cross-process authority.
    """
    site = (site or _SITE).upper()
    breadcrumbs = [
        crumb for crumb in (detail.get("breadcrumb_nodes") or [])
        if isinstance(crumb, dict) and crumb.get("name") and crumb.get("node_id")
    ]
    candidates = []
    previous_node_id = ""
    for crumb in breadcrumbs:
        node_id = str(crumb["node_id"])
        candidates.append({
            "name": str(crumb["name"]),
            "node_id": node_id,
            "parent_node_id": previous_node_id,
            "slug": "",
            "kind": "breadcrumb",
        })
        previous_node_id = node_id

    breadcrumb_leaf = previous_node_id
    for link in detail.get("bsr_node_links") or []:
        if not isinstance(link, dict) or not link.get("name") or not link.get("node_id"):
            continue
        node_id = str(link["node_id"])
        # A BSR link sometimes points at the breadcrumb leaf itself; that is not
        # a new parent-child relationship and must never create a self-edge.
        if breadcrumb_leaf and node_id == breadcrumb_leaf:
            continue
        candidates.append({
            "name": str(link["name"]),
            "node_id": node_id,
            "parent_node_id": breadcrumb_leaf,
            "slug": str(link.get("slug") or "").lower(),
            "kind": "bsr",
        })
    if not candidates:
        return 0

    global _category_reverse_seen_db_path
    db_path = os.path.abspath(DB_PATH)
    with _category_reverse_lock:
        # Tests and long-lived callers can switch DB_PATH; never let an old
        # in-memory dedup cache suppress writes to the new database.
        if _category_reverse_seen_db_path != db_path:
            _category_reverse_seen_edges.clear()
            _category_reverse_seen_db_path = db_path
        candidate_keys = {
            (site, node["node_id"], node["parent_node_id"] or "")
            for node in candidates
        }
        pending_keys = candidate_keys - _category_reverse_seen_edges
        if not pending_keys:
            return 0
        candidates = [
            node for node in candidates
            if (site, node["node_id"], node["parent_node_id"] or "") in pending_keys
        ]

        assert_testing_paths_safe(db_path=DB_PATH)
        conn = sqlite3.connect(DB_PATH, timeout=15)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=10000")
            conn.execute("BEGIN IMMEDIATE")

            existing_edges = set()
            for _, node_id, parent_node_id in pending_keys:
                row = conn.execute(
                    "SELECT 1 FROM categories "
                    "WHERE site=? AND node_id=? AND parent_node_id=? LIMIT 1",
                    (site, node_id, parent_node_id),
                ).fetchone()
                if row:
                    existing_edges.add((site, node_id, parent_node_id))

            # Parent metadata determines depth and, for breadcrumbs, the chart
            # slug.  Pick the shallowest known occurrence when the parent is a
            # DAG node with multiple incoming edges.
            parent_ids = {node["parent_node_id"] for node in candidates if node["parent_node_id"]}
            parent_metadata = {}
            for parent_id in parent_ids:
                row = conn.execute(
                    "SELECT depth, slug FROM categories WHERE site=? AND node_id=? "
                    "ORDER BY depth ASC, id ASC LIMIT 1",
                    (site, parent_id),
                ).fetchone()
                if row:
                    parent_metadata[parent_id] = {
                        "depth": int(row[0] or 0),
                        "slug": row[1] or "",
                    }

            added = 0
            resolved_metadata = dict(parent_metadata)
            inserted_keys = set()
            for node in candidates:
                key = (site, node["node_id"], node["parent_node_id"] or "")
                parent = resolved_metadata.get(node["parent_node_id"], {})
                depth = int(parent.get("depth", -1)) + 1
                parent_slug = parent.get("slug", "")
                slug = node["slug"] or parent_slug
                if key not in existing_edges and key not in inserted_keys:
                    cur = conn.execute(
                        "INSERT INTO categories "
                        "(name, url, node_id, depth, source, explored, parent_node_id, slug, site) "
                        "VALUES(?, ?, ?, ?, 'asin_reverse', 0, ?, ?, ?) "
                        "ON CONFLICT(site, node_id, parent_node_id) DO UPDATE SET "
                        "name=categories.name, url=categories.url",
                        (
                            node["name"], _reverse_category_url(node, parent_slug),
                            node["node_id"], depth, node["parent_node_id"] or "",
                            slug, site,
                        ),
                    )
                    added += cur.rowcount
                    inserted_keys.add(key)
                resolved_metadata.setdefault(node["node_id"], {
                    "depth": depth,
                    "slug": slug,
                })
            conn.commit()
            _category_reverse_seen_edges.update(candidate_keys)
            return added
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _check_detail_filters(detail: dict, filters: dict) -> bool:
    return _check_detail_filters_shared(detail, filters)


def _pg_fetchall(sql, params=()):
    conn = _get_pg()
    cur = conn.cursor()
    cur.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _pg_execute(sql, params=()):
    """执行 PG 写操作，返回 rowcount。"""
    conn = _get_pg()
    cur = conn.cursor()
    cur.execute(sql, params)
    return cur.rowcount


def db_conn():
    c = sqlite3.connect(DB_PATH, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    existing = {r[1] for r in c.execute("PRAGMA table_info(product_sightings)").fetchall()}
    for col, ctype in _DETAIL_COLS:
        if col not in existing:
            c.execute(f"ALTER TABLE product_sightings ADD COLUMN {col} {ctype}")
            existing.add(col)
    if "site" not in existing:
        c.execute("ALTER TABLE product_sightings ADD COLUMN site TEXT DEFAULT 'US'")
    if "run_id" not in existing:
        c.execute("ALTER TABLE product_sightings ADD COLUMN run_id TEXT")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ps_social_proof ON product_sightings(social_proof_count)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ps_run_id ON product_sightings(run_id)")
    c.commit()
    return c


def get_descendant_nodes(root_ids: list, lists: list, site: str = None,
                         include_descendants: bool = True) -> list:
    """根据选中的根节点 node_id 查抓取目标节点。

    include_descendants=True（默认）：所选节点 + 全部下级（现有行为）。
    include_descendants=False：仅所选节点本身（不展开下级）。

    node_id 仅在 (node_id, site) 组合下唯一，不同站点可能出现相同 node_id
    （例如 L1 根节点直接用 slug 字符串作 node_id）。所有查询必须显式带 site
    过滤，否则会把其它站点的同名节点及其全部后代一起选中并抓取
    （跨站点混抓）。
    """
    if not root_ids:
        return []
    if DB_BACKEND == "pg":
        return _get_descendant_nodes_pg(root_ids, include_descendants=include_descendants)
    site = (site or _SITE).upper()
    conn = db_conn()
    placeholders = ",".join("?" * len(root_ids))
    if not include_descendants:
        rows = conn.execute(
            f"""SELECT node_id, url, name, depth FROM categories
                WHERE node_id IS NOT NULL AND site = ?
                  AND node_id IN ({placeholders})
                ORDER BY depth DESC, name""",
            [site, *root_ids],
        ).fetchall()
        conn.close()
        result = _dedupe_category_nodes([dict(r) for r in rows])
        _log.info(f"[fetch_products] [{site}] 仅抓所选 {len(root_ids)} 个节点 → {len(result)} 个目标")
        return result

    # 按 parent_node_id 递归展开，不依赖 URL 前缀（不同榜单 URL 格式下前缀会漏抓）
    rows = conn.execute(
        f"""WITH RECURSIVE sub AS (
                SELECT node_id, url, name, depth FROM categories
                WHERE node_id IN ({placeholders}) AND site = ?
                  AND node_id IS NOT NULL AND node_id != ''
                UNION
                SELECT c.node_id, c.url, c.name, c.depth FROM categories c
                JOIN sub s ON c.parent_node_id = s.node_id
                WHERE c.site = ? AND c.node_id IS NOT NULL AND c.node_id != ''
            )
            SELECT node_id, url, name, depth FROM sub
            ORDER BY depth DESC, name""",
        [*root_ids, site, site],
    ).fetchall()
    conn.close()
    result = _dedupe_category_nodes([dict(r) for r in rows])
    _log.info(
        f"[fetch_products] [{site}] 选中 {len(root_ids)} 个根节点 → {len(result)} 个后代节点"
        f"（深度优先: L{result[0]['depth'] if result else '?'}→L{result[-1]['depth'] if result else '?'}）"
    )
    return result


def _get_descendant_nodes_pg(root_ids, include_descendants: bool = True):
    ph = ",".join(["%s"] * len(root_ids))
    if not include_descendants:
        result = _dedupe_category_nodes(_pg_fetchall(
            f"""SELECT node_id, url, name, depth FROM categories
                WHERE node_id IS NOT NULL AND site = %s AND node_id IN ({ph})
                ORDER BY depth DESC, name""",
            [_SITE, *root_ids],
        ))
        _log.info(f"[fetch_products] 仅抓所选 {len(root_ids)} 个节点 → {len(result)} 个目标")
        return result

    # 与 SQLite 一致：按 parent_node_id 递归，避免 path/ltree 缺失或 URL 格式差异导致漏展开
    result = _dedupe_category_nodes(_pg_fetchall(
        f"""WITH RECURSIVE sub AS (
                SELECT node_id, url, name, depth FROM categories
                WHERE node_id IN ({ph}) AND site = %s
                  AND node_id IS NOT NULL AND node_id != ''
                UNION
                SELECT c.node_id, c.url, c.name, c.depth FROM categories c
                JOIN sub s ON c.parent_node_id = s.node_id
                WHERE c.site = %s AND c.node_id IS NOT NULL AND c.node_id != ''
            )
            SELECT node_id, url, name, depth FROM sub
            ORDER BY depth DESC, name""",
        [*root_ids, _SITE, _SITE],
    ))
    _log.info(f"[fetch_products] 选中 {len(root_ids)} 个根节点 → {len(result)} 个后代节点（深度优先）")
    return result


def get_nodes_by_depth(depths: list[int], site: str = None) -> list:
    """Load every category at the requested exact levels, including L0 and leaf nodes."""
    clean_depths = sorted({int(depth) for depth in (depths or []) if 0 <= int(depth) <= 100})
    if not clean_depths:
        return []
    site = (site or _SITE).upper()
    if DB_BACKEND == "pg":
        ph = ",".join(["%s"] * len(clean_depths))
        result = _pg_fetchall(
            f"""SELECT node_id, MIN(url) AS url, MIN(name) AS name, MAX(depth) AS depth FROM categories
                WHERE node_id IS NOT NULL AND node_id != '' AND site = %s
                  AND depth IN ({ph})
                GROUP BY node_id
                ORDER BY depth DESC, name""",
            [site, *clean_depths],
        )
    else:
        conn = db_conn()
        ph = ",".join("?" * len(clean_depths))
        rows = conn.execute(
            f"""SELECT node_id, MIN(url) AS url, MIN(name) AS name, MAX(depth) AS depth FROM categories
                WHERE node_id IS NOT NULL AND node_id != '' AND site = ?
                  AND depth IN ({ph})
                GROUP BY node_id
                ORDER BY depth DESC, name""",
            [site, *clean_depths],
        ).fetchall()
        conn.close()
        result = [dict(row) for row in rows]
    _log.info(f"[fetch_products] [{site}] 按层级抓取 {clean_depths} → {len(result)} 个目标")
    return result


def _dedupe_category_nodes(nodes: list[dict]) -> list[dict]:
    """Keep tree edges in storage while executing each browse node only once."""
    seen = set()
    unique = []
    for node in nodes:
        node_id = node.get("node_id")
        if node_id in seen:
            continue
        seen.add(node_id)
        unique.append(node)
    return unique


_SLUG_RE = re.compile(r"^[a-zA-Z0-9_-]{1,80}$")


def _validate_slugs(slugs: list) -> list:
    """校验 slug 格式，拒绝注入字符。"""
    cleaned = []
    for s in slugs:
        if not isinstance(s, str) or not _SLUG_RE.match(s):
            raise ValueError(f"非法 slug: {s!r}（仅允许字母数字、_、-，最长80）")
        cleaned.append(s)
    return cleaned


def get_nodes_by_slugs(slugs: list, lists: list, site: str = None) -> list:
    """根据 L1 slug 查出所有后代节点。同一 slug 在不同站点的 URL 前缀不同，
    但仍需显式限定 site，避免历史数据里其它站点残留同名 slug 时混入。"""
    slugs = _validate_slugs(slugs)
    if DB_BACKEND == "pg":
        return _get_nodes_by_slugs_pg(slugs)
    site = (site or _SITE).upper()
    conn = db_conn()
    like_clauses = []
    params = []
    for slug in slugs:
        for pattern in (
            f"%/gp/new-releases/{slug}/%",
            f"%/gp/bestsellers/{slug}/%",
            f"%/gp/most-wished-for/{slug}/%",
        ):
            like_clauses.append("url LIKE ?")
            params.append(pattern)
    if not like_clauses:
        conn.close()
        return []
    sql = f"""
        SELECT DISTINCT node_id, url, name, depth FROM categories
        WHERE node_id IS NOT NULL AND site = ?
          AND ({" OR ".join(like_clauses)})
        ORDER BY depth DESC, name
    """
    rows = conn.execute(sql, [site, *params]).fetchall()
    conn.close()
    result = _dedupe_category_nodes([dict(r) for r in rows])
    _log.info(f"[fetch_products] 选中 {len(slugs)} 个 L1 slug → {len(result)} 个后代节点（深度优先）")
    return result


def _get_nodes_by_slugs_pg(slugs):
    slugs = _validate_slugs(slugs)
    like_clauses = []
    params = [_SITE]
    for slug in slugs:
        for pattern in (
            f"%/gp/new-releases/{slug}/%",
            f"%/gp/bestsellers/{slug}/%",
            f"%/gp/most-wished-for/{slug}/%",
        ):
            like_clauses.append("url LIKE %s")
            params.append(pattern)
    if not like_clauses:
        return []
    sql = f"""
        SELECT DISTINCT node_id, url, name, depth FROM categories
        WHERE node_id IS NOT NULL AND site = %s AND ({" OR ".join(like_clauses)})
        ORDER BY depth DESC, name
    """
    result = _dedupe_category_nodes(_pg_fetchall(sql, params))
    _log.info(f"[fetch_products] 选中 {len(slugs)} 个 L1 slug → {len(result)} 个后代节点（深度优先）")
    return result


def extract_slug(url: str) -> str:
    """从类目 URL 提取 chart slug。

    兼容两套亚马逊榜单路径：
    - 旧/通用：/gp/{list}/{slug}/...
    - 新 SEO：/zgbs/{slug}/...、/zgns/{slug}/...
    与 fetch_subtree.extract_slug 规则对齐；抽不到时返回空串。
    """
    text = str(url or "")
    m = re.search(
        r"/gp/(?:new-releases|bestsellers|movers-and-shakers|"
        r"most-wished-for|most-gifted)/([a-z][a-z0-9-]+)",
        text,
        flags=re.I,
    )
    if m:
        return m.group(1).lower()
    m = re.search(r"/zg(?:bs|ns)/([a-z][a-z0-9-]+)", text, flags=re.I)
    return m.group(1).lower() if m else ""


def _url_matches_list_type(url: str, list_type: str) -> bool:
    """判断已存 URL 是否已经是目标榜单页（可直接复用，避免错误重拼）。"""
    text = str(url or "").lower()
    list_type = str(list_type or "").lower()
    if not text or not list_type:
        return False
    if f"/gp/{list_type}/" in text:
        return True
    if list_type == "bestsellers" and "/zgbs/" in text:
        return True
    if list_type == "new-releases" and "/zgns/" in text:
        return True
    return False


def save_link_validity(node_id: str, list_type: str, is_valid: int,
                       site: str = None):
    """按站点将单个榜单的有效性写入 categories。

    旧版 SQLite ``link_cache`` 没有 site 列；继续兼容该 schema（不报错），
    但不再写入这个全局 node_id 缓存，以免同 node_id 的其它站点被污染。
    """
    col = LIST_COL_MAP.get(list_type)
    if not col:
        return
    site = (site or _SITE).upper()
    with _db_lock:
        if DB_BACKEND == "pg":
            conn = _get_pg()
            conn.cursor().execute(
                f"UPDATE categories SET {col}=%s WHERE node_id=%s AND site=%s",
                (is_valid, node_id, site),
            )
        else:
            conn = db_conn()
            try:
                conn.execute(
                    f"UPDATE categories SET {col}=? WHERE node_id=? AND site=?",
                    (is_valid, node_id, site),
                )
                try:
                    cache_cols = {
                        row[1] for row in conn.execute(
                            "PRAGMA table_info(link_cache)"
                        ).fetchall()
                    }
                    if "site" in cache_cols:
                        conn.execute(
                            f"INSERT INTO link_cache (node_id, site, {col}, checked_at) "
                            f"VALUES (?, ?, ?, datetime('now')) "
                            f"ON CONFLICT(node_id, site) DO UPDATE SET "
                            f"{col}=excluded.{col}, checked_at=datetime('now')",
                            (node_id, site, is_valid),
                        )
                except Exception:
                    pass
                conn.commit()
            finally:
                conn.close()


def save_products(products: list):
    """批量写入运行缓存（默认）或 product_sightings（legacy）。"""
    if not products:
        return 0
    if use_run_cache():
        for p in products:
            p.setdefault("site", _SITE)
        return run_cache.upsert_products(
            products, run_id=_RUN_ID, chart="products", default_site=_SITE,
        )
    if DB_BACKEND == "pg":
        return _save_products_pg(products)
    sql = """
        INSERT OR IGNORE INTO product_sightings
        (asin, name, price, price_raw, original_price, discount_pct,
         rating, review_count, rank, image_url, product_url,
         has_video, is_amazon_choice,
         node_id, category_name, category_slug, category_depth,
         list_type, list_total, site, run_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    touch_sql = """
        UPDATE product_sightings
        SET run_id=?, name=COALESCE(?, name), price=COALESCE(?, price),
            price_raw=COALESCE(?, price_raw), rating=COALESCE(?, rating),
            review_count=COALESCE(?, review_count), rank=COALESCE(?, rank),
            image_url=COALESCE(?, image_url), product_url=COALESCE(?, product_url),
            list_total=COALESCE(?, list_total)
        WHERE asin=? AND node_id=? AND list_type=? AND site=?
    """
    saved = 0
    with _db_lock:
        conn = db_conn()
        try:
            for p in products:
                site = p.get("site", _SITE)
                try:
                    cur = conn.execute(sql, (
                        p["asin"], p.get("name"), p.get("price"),
                        p.get("price_raw"), p.get("original_price"),
                        p.get("discount_pct"), p.get("rating"),
                        p.get("review_count"), p.get("rank"),
                        p.get("image_url"), p.get("product_url"),
                        p.get("has_video", 0), p.get("is_amazon_choice", 0),
                        p["node_id"], p.get("category_name"),
                        p.get("category_slug"), p.get("category_depth"),
                        p["list_type"], p.get("list_total"),
                        site, _RUN_ID,
                    ))
                    if cur.rowcount == 0:
                        # 已存在：刷新 run_id 与列表字段，保留已有详情
                        conn.execute(touch_sql, (
                            _RUN_ID, p.get("name"), p.get("price"), p.get("price_raw"),
                            p.get("rating"), p.get("review_count"), p.get("rank"),
                            p.get("image_url"), p.get("product_url"), p.get("list_total"),
                            p["asin"], p["node_id"], p["list_type"], site,
                        ))
                    saved += 1
                except sqlite3.IntegrityError:
                    pass
            conn.commit()
        finally:
            conn.close()
    return saved


def _save_products_pg(products):
    sql = """
        INSERT INTO product_sightings
        (asin, name, price, price_raw, original_price, discount_pct,
         rating, review_count, rank, image_url, product_url,
         has_video, is_amazon_choice,
         node_id, category_name, category_slug, category_depth,
         list_type, list_total, site, run_id)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT ON CONSTRAINT uq_ps_asin_node_list_site
        DO UPDATE SET
            name = EXCLUDED.name,
            price = EXCLUDED.price,
            price_raw = EXCLUDED.price_raw,
            rating = EXCLUDED.rating,
            review_count = EXCLUDED.review_count,
            rank = EXCLUDED.rank,
            image_url = EXCLUDED.image_url,
            product_url = EXCLUDED.product_url,
            list_total = EXCLUDED.list_total,
            run_id = EXCLUDED.run_id,
            scraped_at = now()
    """
    saved = 0
    with _db_lock:
        conn = _get_pg()
        cur = conn.cursor()
        for p in products:
            try:
                cur.execute(sql, (
                    p["asin"], p.get("name"), p.get("price"),
                    p.get("price_raw"), p.get("original_price"),
                    p.get("discount_pct"), p.get("rating"),
                    p.get("review_count"), p.get("rank"),
                    p.get("image_url"), p.get("product_url"),
                    p.get("has_video", 0), p.get("is_amazon_choice", 0),
                    p.get("node_id"), p.get("category_name"),
                    p.get("category_slug"), p.get("category_depth"),
                    p["list_type"], p.get("list_total"),
                    p.get("site", _SITE), _RUN_ID,
                ))
                saved += 1
            except Exception:
                _log.debug(f"save_products 写入失败 asin={p.get('asin')}: {traceback.format_exc()}")
    return saved


# ── HTML 解析 ───────────────────────────────────────────────────────

def extract_list_total(html: str) -> int:
    m = re.search(_RESULTS_PAT, html, re.IGNORECASE)
    if m:
        return int(m.group(1).replace(",", "").replace(".", "").replace(" ", ""))
    m2 = re.search(r'showing\s+\d+\s*-\s*\d+\s+of\s+([\d,]+)', html, re.IGNORECASE)
    if m2:
        return int(m2.group(1).replace(",", ""))
    soup = BeautifulSoup(html, "html.parser")
    cards = _select_product_items(soup)
    if cards:
        return len(cards)
    asins = {m.group(1) for m in re.finditer(r"/dp/([A-Z0-9]{10})", html)}
    return len(asins)


# 主选择器为线上已验证有效的两种榜单模板；下面几个是防御性兜底，
# 仅在主选择器 0 匹配时才会尝试，不影响现有已验证行为。
# 顺序按"误命中风险"从低到高排列：越靠后越宽泛（例如 [data-asin] 几乎会
# 命中页面上所有"赞助商品/其他人还买了"等不相关卡片），只在前面更精确的
# 选择器都未命中时才作为最后手段使用，避免把不相关内容当榜单商品抓入库。
_FALLBACK_ITEM_SELECTORS = [
    ".zg-item-immersion",
    "[id^='p13n-asin-index']",
    ".p13n-sc-uncoverable-faceout",
    "[data-asin]:has(a[href*='/dp/'])",
]


def _select_product_items(soup: BeautifulSoup) -> list:
    items = soup.select("[id^='gridItemRoot']")
    if items:
        return items
    items = soup.select(".zg-grid-general-faceout")
    if items:
        return items
    for sel in _FALLBACK_ITEM_SELECTORS:
        try:
            items = soup.select(sel)
        except NotImplementedError:
            # 部分 bs4/soupsieve 版本不支持 :has()，跳过该兜底选择器
            continue
        if items:
            _log.warning(f"[fetch_products] 主选择器未命中，使用兜底选择器: {sel} ({len(items)} 项)")
            return items
    return []


def _count_product_items(html: str) -> int:
    return len(_select_product_items(BeautifulSoup(html, "html.parser")))


def parse_products(html: str, node_id: str, category_name: str,
                   category_slug: str, category_depth: int,
                   list_type: str, list_total: int,
                   review_max: int,
                   price_min: float = 0.0,
                   price_max: float = 0.0,
                   review_min: int = 0,
                   rating_min: float = 0.0,
                   rating_max: float = 0.0,
                   list_limit: int = 0,
                   position_start: int = 1) -> list:
    """解析单页 HTML，提取符合条件的商品。"""
    soup = BeautifulSoup(html, "html.parser")
    items = _select_product_items(soup)

    products = []
    for idx, item in enumerate(items):
        p = {}
        list_position = position_start + idx

        # ASIN
        link = item.select_one("a[href*='/dp/']")
        if not link:
            continue
        href = link.get("href", "")
        m = re.search(r"/dp/([A-Z0-9]{10})", href)
        if not m:
            continue
        p["asin"] = m.group(1)
        p["product_url"] = (_DOMAIN + href) if href.startswith("/") else href

        # 商品名
        name_el = (item.select_one("div._cDEzb_p13n-sc-css-line-clamp-3_g3dy1")
                   or item.select_one(".p13n-sc-truncate")
                   or item.select_one("a > span > div"))
        p["name"] = name_el.get_text(strip=True) if name_el else ""

        # 图片：data-a-dynamic-image → srcset → data-src → src（跳过占位图）
        img = item.select_one("img")
        p["image_url"] = extract_image_url(img)

        # 价格
        price_el = item.select_one(".a-price .a-offscreen")
        if not price_el:
            price_el = item.select_one("._cDEzb_p13n-sc-price_3mJ9Z")
        if price_el:
            raw = price_el.get_text(strip=True)
            p["price_raw"] = raw
            m_price = re.search(r"[\d,.]+", raw)
            if m_price:
                price_str = m_price.group()
                if _DECIMAL_SEP == ",":
                    price_str = price_str.replace(".", "").replace(",", ".")
                else:
                    price_str = price_str.replace(",", "")
                try:
                    p["price"] = float(price_str)
                except ValueError:
                    pass

        # 原价
        orig_el = item.select_one(".a-text-price .a-offscreen")
        if orig_el:
            p["original_price"] = orig_el.get_text(strip=True)

        # 折扣
        disc_el = item.select_one(".a-badge-label-inner, [data-a-badge-color='sx-orange']")
        if disc_el:
            p["discount_pct"] = disc_el.get_text(strip=True)

        # 评分
        rating_el = item.select_one(".a-icon-alt")
        if rating_el:
            rt = rating_el.get_text(strip=True)
            m_rt = re.search(_RATING_PAT, rt)
            if m_rt:
                raw_rating = m_rt.group(1).replace(",", ".")
                try:
                    p["rating"] = float(raw_rating)
                except ValueError:
                    pass

        # 评论数
        review_el = item.select_one("a.a-size-small span, span.a-size-small")
        if review_el:
            rt_text = review_el.get_text(strip=True).replace(",", "")
            if rt_text.isdigit():
                p["review_count"] = int(rt_text)

        # 排名：优先读徽章；Amazon 当前 DOM 常无 .zg-badge-text，回退到分页顺序位
        rank_el = item.select_one(".zg-badge-text")
        if rank_el:
            rk = rank_el.get_text(strip=True).lstrip("#")
            if rk.isdigit():
                p["rank"] = int(rk)
                list_position = p["rank"]
        if "rank" not in p:
            p["rank"] = list_position

        if list_limit > 0 and list_position > list_limit:
            continue

        # 视频标记
        p["has_video"] = 1 if item.select_one(".vse-video-badge, .a-icon-vse") else 0

        # Amazon's Choice
        badge_text = " ".join(
            el.get_text(" ", strip=True)
            for el in item.select(".a-badge, .a-badge-label, .a-badge-label-inner, [data-a-badge-type]")
        )
        badge_type = " ".join(
            el.get("data-a-badge-type", "")
            for el in item.select("[data-a-badge-type]")
        )
        badge_blob = f"{badge_type} {badge_text}".lower()
        p["is_amazon_choice"] = 1 if (
            "amazons-choice" in badge_blob
            or "amazon's choice" in badge_blob
            or "amazon choice" in badge_blob
            or "amazon\u304a\u3059\u3059\u3081" in badge_blob
        ) else 0
        p["is_bestseller"] = 1 if re.search(
            r"best[\s-]*seller|bestseller|\u30d9\u30b9\u30c8\u30bb\u30e9\u30fc|\u58f2\u308c\u7b4b",
            badge_blob,
            re.I,
        ) else 0

        # ── 评论数筛选（闭区间；缺失值不通过）──
        if review_max > 0 or review_min > 0:
            if "review_count" not in p or p.get("review_count") is None:
                continue
            rc = p["review_count"]
            if review_max > 0 and rc > review_max:
                continue
            if review_min > 0 and rc < review_min:
                continue

        # ── 评分筛选 ──
        rt = p.get("rating")
        if (rating_min > 0 or rating_max > 0) and rt is None:
            continue
        if rt is not None:
            if rating_min > 0 and rt < rating_min:
                continue
            if rating_max > 0 and rt > rating_max:
                continue

        # ── 价格筛选 ──
        price = p.get("price")
        if (price_min > 0 or price_max > 0) and price is None:
            continue
        if price is not None:
            if price_min > 0 and price < price_min:
                continue
            if price_max > 0 and price > price_max:
                continue

        # ── ASIN去重 ──
        asin = p["asin"]
        with _seen_lock:
            if asin in _seen_asins:
                with _stats_lock:
                    _stats["products_dup"] += 1
                continue
            _seen_asins.add(asin)

        # 来源信息
        p["node_id"] = node_id
        p["category_name"] = category_name
        p["category_slug"] = category_slug
        p["category_depth"] = category_depth
        p["list_type"] = list_type
        p["list_total"] = list_total

        products.append(p)

    return products


_log.info("[fetch_products] 模块加载完成")




def _load_cached_detail(asin: str) -> dict | None:
    """加载同轮同站点已成功详情（run_cache）；legacy 仍按站点复用。"""
    if use_run_cache():
        cached = run_cache.load_cached_detail(asin, run_id=_RUN_ID, site=_SITE)
        if not cached:
            return None
        return {k: cached.get(k) for k in _CACHED_DETAIL_KEYS}
    cols = ", ".join(_CACHED_DETAIL_KEYS)
    with _db_lock:
        if DB_BACKEND == "pg":
            rows = _pg_fetchall(
                f"SELECT {cols} FROM product_sightings "
                "WHERE asin=%s AND site=%s AND detail_scraped=1 LIMIT 1",
                (asin, _SITE),
            )
            return dict(rows[0]) if rows else None
        conn = db_conn()
        try:
            row = conn.execute(
                f"SELECT {cols} FROM product_sightings "
                "WHERE asin=? AND site=? AND detail_scraped=1 LIMIT 1",
                (asin, _SITE),
            ).fetchone()
            return {k: row[k] for k in _CACHED_DETAIL_KEYS} if row else None
        finally:
            conn.close()


def _finalize_detail(product: dict, detail: dict, filters: dict) -> bool:
    """把详情写回当前 (asin,node_id,list_type,site)；筛选失败只删当前归属行。
    返回 True=保留，False=被筛选剔除。"""
    payload = dict(detail)
    # 始终优先当前榜单价；丢弃缓存/旧详情中的派生费用，按当前价重算
    current_price = product.get("price")
    if current_price is not None:
        payload["price"] = current_price
    payload.pop("fba_fee", None)
    payload.pop("placement_fee", None)
    _attach_normalized_dims(payload)
    if payload.get("item_weight") is not None or payload.get("item_dimensions") is not None:
        fees = estimate_fba_fees(
            _SITE,
            payload.get("item_weight"),
            payload.get("item_dimensions"),
            payload.get("price"),
        )
        if fees.get("fba_fee") is not None:
            payload["fba_fee"] = fees["fba_fee"]
        if fees.get("placement_fee") is not None:
            payload["placement_fee"] = fees["placement_fee"]
    payload["detail_scraped"] = 1
    payload["run_id"] = _RUN_ID

    if filters:
        none_flags = _active_filter_none_flags(payload, filters)
        if none_flags:
            with _stats_lock:
                for field, is_none in none_flags.items():
                    slot = _filter_field_probe.setdefault(field, {"none": 0, "total": 0})
                    slot["total"] += 1
                    if is_none:
                        slot["none"] += 1

    if filters and not _check_detail_filters(payload, filters):
        _delete_sighting(
            product["asin"],
            node_id=product.get("node_id"),
            list_type=product.get("list_type"),
        )
        with _stats_lock:
            _stats["products_saved"] -= 1
        return False

    _update_sighting_detail(
        product["asin"], payload,
        node_id=product.get("node_id"),
        list_type=product.get("list_type"),
    )
    product.update(payload)
    return True


def enrich_with_details(products: list, client: WorkerProxyClient,
                        delay: float, filters: dict = None,
                        terminal_asins: set | None = None) -> int:
    """对列表页抓到的商品逐个请求详情页，补全字段并 UPDATE 到数据库。
    不符合筛选条件的商品从数据库删除（仅当前榜单归属）。
    已有成功详情时复用并重跑当前筛选，避免漏填新归属行或沿用旧筛选结果。
    返回详情抓取失败（非"筛选剔除"、非HTTP 404永久不存在）的商品数，供调用方将节点标为 error。
    HTTP 404 视为商品永久不存在的终态：单独写入 terminal_asins（若提供），不计入失败数、不参与跨IP重试。"""
    if not products:
        return 0
    if filters is None:
        filters = {}
    fetch_failures = 0
    for p in products:
        asin = p["asin"]
        try:
            cached = _load_cached_detail(asin)
            if cached is not None:
                _finalize_detail(p, cached, filters)
                continue

            url = f"{_DOMAIN}/dp/{asin}"
            referer = p.get("product_url", f"{_DOMAIN}/s?k={asin}")
            outcome = client.get(url, phase="DETAIL", item_id=asin, referer=referer)
            raise_if_pool_below_minimum(outcome)
            if not outcome.ok or outcome.status_code != 200:
                if outcome.status_code == 404:
                    _mark_detail_failed(
                        asin, node_id=p.get("node_id"), list_type=p.get("list_type"),
                        status="not_found",
                    )
                    if terminal_asins is not None:
                        terminal_asins.add(asin)
                else:
                    _mark_detail_failed(
                        asin, node_id=p.get("node_id"), list_type=p.get("list_type"),
                    )
                    fetch_failures += 1
                continue
            if _detail_parse_pipeline is None:
                detail = parse_detail_fields(outcome.html or "")
            else:
                detail = _detail_parse_pipeline.submit(
                    outcome.html or "", _SITE,
                ).result()
            if not detail:
                # HTTP 200 但解析为空（验证码/结构变化）必须计入失败
                _mark_detail_failed(
                    asin, node_id=p.get("node_id"), list_type=p.get("list_type"),
                )
                fetch_failures += 1
                continue
            _discover_category_nodes_from_detail(detail, _SITE)
            # These parser-only discovery fields are not product_sightings
            # columns (and are intentionally not persisted with products).
            detail.pop("breadcrumb_nodes", None)
            detail.pop("bsr_node_links", None)
            _finalize_detail(p, detail, filters)
        except ProxyRequiredError:
            raise
        except (KeyError, TypeError, ValueError, AttributeError) as e:
            _mark_detail_failed(
                asin, node_id=p.get("node_id"), list_type=p.get("list_type"),
            )
            fetch_failures += 1
            _log.error(f"  [detail] {asin} 解析异常: {e}\n{traceback.format_exc()}")
        except Exception as e:
            _mark_detail_failed(
                asin, node_id=p.get("node_id"), list_type=p.get("list_type"),
            )
            fetch_failures += 1
            _log.error(f"  [detail] {asin} 未知异常: {e}\n{traceback.format_exc()}")
        time.sleep(_pool_delay(delay, getattr(client, "pool", None)) + random.uniform(0, delay * 0.3))
    return fetch_failures


def _delete_sighting(asin: str, node_id: str = None, list_type: str = None):
    """删除当前归属行；未传 node/list 时保持 asin+site（兼容旧调用）。"""
    if use_run_cache():
        run_cache.delete_item(
            asin, run_id=_RUN_ID, site=_SITE, node_id=node_id, list_type=list_type,
        )
        return
    with _db_lock:
        if DB_BACKEND == "pg":
            if node_id is not None and list_type is not None:
                _pg_execute(
                    "DELETE FROM product_sightings "
                    "WHERE asin=%s AND site=%s AND node_id=%s AND list_type=%s",
                    (asin, _SITE, node_id, list_type),
                )
            else:
                _pg_execute(
                    "DELETE FROM product_sightings WHERE asin=%s AND site=%s",
                    (asin, _SITE),
                )
        else:
            conn = db_conn()
            try:
                if node_id is not None and list_type is not None:
                    conn.execute(
                        "DELETE FROM product_sightings "
                        "WHERE asin=? AND site=? AND node_id=? AND list_type=?",
                        (asin, _SITE, node_id, list_type),
                    )
                else:
                    conn.execute(
                        "DELETE FROM product_sightings WHERE asin=? AND site=?",
                        (asin, _SITE),
                    )
                conn.commit()
            finally:
                conn.close()


def _mark_detail_failed(
    asin: str, node_id: str = None, list_type: str = None, status: str = "failed",
):
    """详情抓取失败：当前归属行 detail_scraped=2，并写入本次 run_id。
    status="not_found" 表示 HTTP 404（商品永久不存在），与代理/CAPTCHA导致的
    可重试失败("failed")分开记录；legacy 原始SQL分支无 detail_status 列，不做区分。"""
    if _detail_writer is not None:
        _detail_writer.submit_many([{
            "kind": status,
            "asin": asin,
            "node_id": node_id,
            "list_type": list_type,
        }])
        return
    if use_run_cache():
        run_cache.mark_detail_failed(
            asin, run_id=_RUN_ID, site=_SITE, node_id=node_id, list_type=list_type,
            status=status,
        )
        return
    with _db_lock:
        if DB_BACKEND == "pg":
            if node_id is not None and list_type is not None:
                _pg_execute(
                    "UPDATE product_sightings SET detail_scraped=2, run_id=%s "
                    "WHERE asin=%s AND site=%s AND node_id=%s AND list_type=%s",
                    (_RUN_ID, asin, _SITE, node_id, list_type),
                )
            else:
                _pg_execute(
                    "UPDATE product_sightings SET detail_scraped=2, run_id=%s "
                    "WHERE asin=%s AND site=%s",
                    (_RUN_ID, asin, _SITE),
                )
        else:
            conn = db_conn()
            try:
                if node_id is not None and list_type is not None:
                    conn.execute(
                        "UPDATE product_sightings SET detail_scraped=2, run_id=? "
                        "WHERE asin=? AND site=? AND node_id=? AND list_type=?",
                        (_RUN_ID, asin, _SITE, node_id, list_type),
                    )
                else:
                    conn.execute(
                        "UPDATE product_sightings SET detail_scraped=2, run_id=? "
                        "WHERE asin=? AND site=?",
                        (_RUN_ID, asin, _SITE),
                    )
                conn.commit()
            finally:
                conn.close()


def _update_sighting_detail(asin: str, detail: dict,
                            node_id: str = None, list_type: str = None):
    """更新详情：优先写当前归属行；否则回退 asin+site。"""
    detail = dict(detail)
    detail.setdefault("run_id", _RUN_ID)
    if _detail_writer is not None:
        _detail_writer.submit_many([{
            "kind": "success",
            "asin": asin,
            "detail": detail,
            "node_id": node_id,
            "list_type": list_type,
        }])
        return
    if use_run_cache():
        run_cache.update_detail(
            asin, detail, run_id=_RUN_ID, site=_SITE,
            node_id=node_id, list_type=list_type,
        )
        return
    with _db_lock:
        if DB_BACKEND == "pg":
            sets = ", ".join(f"{k}=%s" for k in detail)
            if node_id is not None and list_type is not None:
                vals = list(detail.values()) + [asin, _SITE, node_id, list_type]
                _pg_execute(
                    f"UPDATE product_sightings SET {sets} "
                    "WHERE asin=%s AND site=%s AND node_id=%s AND list_type=%s",
                    vals,
                )
            else:
                vals = list(detail.values()) + [asin, _SITE]
                _pg_execute(
                    f"UPDATE product_sightings SET {sets} WHERE asin=%s AND site=%s",
                    vals,
                )
        else:
            sets = ", ".join(f"{k}=?" for k in detail)
            conn = db_conn()
            try:
                if node_id is not None and list_type is not None:
                    vals = list(detail.values()) + [asin, _SITE, node_id, list_type]
                    conn.execute(
                        f"UPDATE product_sightings SET {sets} "
                        "WHERE asin=? AND site=? AND node_id=? AND list_type=?",
                        vals,
                    )
                else:
                    vals = list(detail.values()) + [asin, _SITE]
                    conn.execute(
                        f"UPDATE product_sightings SET {sets} WHERE asin=? AND site=?",
                        vals,
                    )
                conn.commit()
            finally:
                conn.close()


def _write_detail_batch(events: list[dict]) -> int:
    """Durability boundary for the single detail writer."""
    if use_run_cache():
        updates = []
        for event in events:
            detail = dict(event.get("detail") or {})
            if event["kind"] in ("failed", "not_found"):
                detail = {
                    "detail_scraped": 2,
                    "detail_status": event["kind"],
                    "run_id": _RUN_ID,
                }
            updates.append({
                "asin": event["asin"],
                "detail": detail,
                "run_id": _RUN_ID,
                "site": _SITE,
                "node_id": event.get("node_id"),
                "list_type": event.get("list_type"),
            })
        return run_cache.update_details_batch(updates)

    groups: dict[tuple, list[list]] = {}
    for event in events:
        if event["kind"] in ("failed", "not_found"):
            detail = {"detail_scraped": 2, "run_id": _RUN_ID}
        else:
            detail = dict(event.get("detail") or {})
            detail.setdefault("run_id", _RUN_ID)
        columns = tuple(detail)
        scoped = (
            event.get("node_id") is not None
            and event.get("list_type") is not None
        )
        values = list(detail.values()) + [event["asin"], _SITE]
        if scoped:
            values += [event["node_id"], event["list_type"]]
        groups.setdefault((columns, scoped), []).append(values)

    with _db_lock:
        if DB_BACKEND == "pg":
            conn = _get_pg()
            with conn.cursor() as cur:
                for (columns, scoped), rows in groups.items():
                    sets = ", ".join(f"{column}=%s" for column in columns)
                    where = "asin=%s AND site=%s"
                    if scoped:
                        where += " AND node_id=%s AND list_type=%s"
                    cur.executemany(
                        f"UPDATE product_sightings SET {sets} WHERE {where}",
                        rows,
                    )
        else:
            conn = db_conn()
            try:
                for (columns, scoped), rows in groups.items():
                    sets = ", ".join(f"{column}=?" for column in columns)
                    where = "asin=? AND site=?"
                    if scoped:
                        where += " AND node_id=? AND list_type=?"
                    conn.executemany(
                        f"UPDATE product_sightings SET {sets} WHERE {where}",
                        rows,
                    )
                conn.commit()
            finally:
                conn.close()
    return len(events)


# ── Worker 主循环 ───────────────────────────────────────────────────

def process_node(node: dict, lists: list, review_max: int,
                 min_list_size: int, client: WorkerProxyClient,
                 price_min: float = 0.0, price_max: float = 0.0,
                 review_min: int = 0,
                 rating_min: float = 0.0, rating_max: float = 0.0,
                 max_pages: int = 2, delay: float = 2.0,
                 detail_filters: dict = None,
                 list_limit: int = 0,
                 detail_submit=None,
                 count_completion: bool = True) -> tuple[str, str, int, int]:
    """处理单个节点的所有榜单。返回 (status, error_code, products_found, attempts)。"""
    node_id = node["node_id"]
    slug    = extract_slug(node["url"])
    name    = node["name"]
    depth   = node["depth"]
    try:
        list_limit = int(list_limit)
    except (TypeError, ValueError):
        list_limit = 0
    list_limit = max(0, min(list_limit, 100))
    max_pages = max(1, min(int(max_pages), 2))
    node_found = 0
    node_attempts = 0
    node_error = ""

    for list_type in lists:
        exact_url = node.get("canonical_list_url") or node.get("list_url")
        exact_type = node.get("canonical_list_type") or node.get("list_type")
        exact_matches_list = bool(
            exact_url
            and (
                exact_type == list_type
                or (
                    not exact_type
                    and _url_matches_list_type(str(exact_url), list_type)
                )
            )
        )
        stored_url = str(node.get("url") or "")
        if exact_matches_list:
            url_base = str(exact_url)
        elif _url_matches_list_type(stored_url, list_type):
            # 类目表已是该榜单的 /zgbs|/zgns|/gp/{list}/ URL 时直接复用
            url_base = stored_url
        else:
            chart_slug = slug or str(node.get("slug") or "").strip().lower()
            if not chart_slug:
                with _stats_lock:
                    _stats["errors"] += 1
                node_error = "EMPTY_CHART_SLUG"
                continue
            url_base = f"{_DOMAIN}/gp/{list_type}/{chart_slug}/{node_id}/"
        outcome = client.get(
            url_base, phase="LIST", item_id=f"{node_id}:{list_type}",
            referer=f"{_DOMAIN}/",
        )
        node_attempts += outcome.attempts
        raise_if_pool_below_minimum(outcome)
        if not outcome.ok:
            with _stats_lock:
                _stats["errors"] += 1
            node_error = outcome.final_reason or outcome.error_code
            continue

        html = outcome.html or ""
        product_items = _count_product_items(html)
        page1_valid = int(
            outcome.status_code == 200
            and not is_captcha_page(html)
            and product_items > 0
        )
        save_link_validity(node_id, list_type, page1_valid, site=_SITE)
        if outcome.status_code != 200:
            continue
        if is_captcha_page(html):
            continue

        time.sleep(_pool_delay(delay, getattr(client, "pool", None)))

        total = extract_list_total(html)
        if min_list_size > 0 and total < min_list_size:
            with _stats_lock:
                _stats["skipped"] += 1
            continue

        position_start = 1
        all_products = parse_products(
            html, node_id, name, slug, depth,
            list_type, total, review_max, price_min, price_max,
            review_min, rating_min, rating_max,
            list_limit, position_start
        )
        position_start += product_items

        for pg in range(2, max_pages + 1):
            if list_limit > 0 and position_start > list_limit:
                break
            page_outcome = client.get(
                url_base + ("&" if "?" in url_base else "?") + f"pg={pg}",
                phase="LIST",
                item_id=f"{node_id}:{list_type}:p{pg}", referer=url_base,
            )
            node_attempts += page_outcome.attempts
            raise_if_pool_below_minimum(page_outcome)
            if not page_outcome.ok or page_outcome.status_code != 200:
                if not page_outcome.ok:
                    node_error = page_outcome.final_reason or page_outcome.error_code
                break
            all_products += parse_products(
                page_outcome.html or "", node_id, name, slug, depth,
                list_type, total, review_max, price_min, price_max,
                review_min, rating_min, rating_max,
                list_limit, position_start
            )
            position_start += _count_product_items(page_outcome.html or "")
            time.sleep(_pool_delay(delay, getattr(client, "pool", None)))

        node_found += len(all_products)
        with _stats_lock:
            _stats["products_found"] += len(all_products)

        if all_products:
            saved = save_products(all_products)
            with _stats_lock:
                _stats["products_saved"] += saved
            if detail_submit is not None:
                detail_submit(all_products)
                continue
            detail_failures = enrich_with_details(all_products, client, delay, detail_filters)
            if detail_failures:
                with _stats_lock:
                    _stats["errors"] += detail_failures
                # 任意详情失败都标 error，断点可重试；已成功 ASIN 由 enrich 跳过，不会重复请求
                if detail_failures >= len(all_products):
                    node_error = f"DETAIL_FETCH_FAILED:{detail_failures}"
                else:
                    node_error = f"DETAIL_PARTIAL:{detail_failures}/{len(all_products)}"
                    _log.warning(
                        "[detail] node=%s list=%s partial_failures=%d/%d（节点记 error 以便重试失败 ASIN）",
                        node_id, list_type, detail_failures, len(all_products),
                    )

    if count_completion:
        with _stats_lock:
            _stats["done_nodes"] += 1
    if node_error:
        return ("error", node_error, node_found, node_attempts)
    return ("done", "", node_found, node_attempts)


def _run_batch_leased_legacy(root_ids: list, lists: list, review_max: int,
              min_list_size: int, delay: float = 2.0,
              price_min: float = 0.0, price_max: float = 0.0,
              review_min: int = 0,
              rating_min: float = 0.0, rating_max: float = 0.0,
              max_pages: int = 2,
              slugs: list = None,
              detail_filters: dict = None,
              list_limit: int = 0,
              include_descendants: bool = True,
              resume: bool = True,
              depths: list[int] = None):
    """主入口：单线程顺序抓取。从最深层类目开始，逐层向上。"""
    global _checkpoint
    if depths:
        nodes = get_nodes_by_depth(depths, site=_SITE)
    elif slugs:
        # --slugs 语义本身就是 L1 下全部后代；exact 模式对 slug 入口不适用，仍展开
        nodes = get_nodes_by_slugs(slugs, lists, site=_SITE)
    else:
        nodes = get_descendant_nodes(
            root_ids, lists, site=_SITE, include_descendants=include_descendants
        )
    _stats["total_nodes"] = len(nodes)
    _stats["products_dup"] = 0
    _stats["done_nodes"] = 0
    with _stats_lock:
        _filter_field_probe.clear()
    with _seen_lock:
        _seen_asins.clear()
    try:
        list_limit = int(list_limit)
    except (TypeError, ValueError):
        list_limit = 0
    list_limit = max(0, min(list_limit, 100))
    max_pages = max(1, min(int(max_pages), 2))

    if not nodes:
        _log.warning("[fetch_products] 无目标节点，退出")
        return

    try:
        pool = ProxyPool()
    except ProxyRequiredError as e:
        _log.error(f"[fetch_products] 代理强制模式失败: {e.code} {e}")
        raise SystemExit(2) from e
    if pool.usable_count < PROXY_MIN_START_NODES:
        _log.error(
            "[proxy] 启动拒绝：可用代理=%d，最低要求=%d",
            pool.usable_count, PROXY_MIN_START_NODES,
        )
        raise SystemExit(2)
    touch_crawl_activity(active=True, source="fetch_products")

    checkpoint_config = {
        "site": _SITE,
        "db_backend": DB_BACKEND,
        "roots": sorted(root_ids or []),
        "depths": sorted(depths or []),
        "slugs": sorted(slugs or []),
        "lists": sorted(lists or []),
        "exact_roots": not include_descendants,
        "max_pages": max_pages,
        "list_limit": list_limit,
        "review_max": review_max,
        "review_min": review_min,
        "min_list_size": min_list_size,
        "price_min": price_min,
        "price_max": price_max,
        "rating_min": rating_min,
        "rating_max": rating_max,
        "detail_filters": detail_filters or {},
        "node_count": len(nodes),
        "node_ids_sha256": hashlib.sha256(
            "\n".join(sorted(str(n["node_id"]) for n in nodes)).encode("utf-8")
        ).hexdigest(),
    }
    signature = canonical_signature(checkpoint_config)
    _checkpoint = ProductsCheckpoint(signature, checkpoint_config, resume=resume)
    done_nodes = _checkpoint.done_ids()
    remaining = [n for n in nodes if str(n["node_id"]) not in done_nodes]
    _stats["done_nodes"] = len(done_nodes)
    _stats["total_nodes"] = len(nodes)
    _update_pool_stats(pool)

    t0 = time.time()
    price_info = ""
    if price_min > 0 or price_max > 0:
        c = _CURRENCY
        price_info = f" 价格{c}{price_min:.0f}-{c}{price_max:.0f}" if price_max > 0 else f" 价格>{c}{price_min:.0f}"
    depth_hint = f"L{nodes[0]['depth']}→L{nodes[-1]['depth']}" if nodes else "?"
    _log.info(
        f"[fetch_products] 开始抓取: {len(nodes)} 节点 × {len(lists)} 榜单 "
        f"(待处理 {len(remaining)}, 深度优先 {depth_hint}), "
        f"评论<{review_max}, 最少{min_list_size}商品{price_info}, 延迟{delay}s"
    )
    _log.info("[fetch_products] run=%s checkpoint=%s", _RUN_ID, _checkpoint.path)

    client = WorkerProxyClient(pool, worker_id=0, warmup=False)
    try:
        _checkpoint.set_phase("LIST")
        for node in remaining:
            try:
                status, err_code, found, attempts = process_node(
                    node, lists, review_max, min_list_size, client,
                    price_min, price_max, review_min,
                    rating_min, rating_max, max_pages, delay,
                    detail_filters,
                    list_limit,
                )
                _checkpoint.save_node(
                    str(node["node_id"]), status=status,
                    error_code=err_code,
                    products_found=found, attempts=attempts,
                )
            except ProxyRequiredError as exc:
                _checkpoint.set_phase("PAUSED_PROXY")
                _log.critical(
                    "[安全暂停] run=%s code=%s error=%s checkpoint=%s pool=%s",
                    _RUN_ID, exc.code, exc, _checkpoint.path, pool.health_snapshot(),
                )
                raise SystemExit(3) from exc
            _update_pool_stats(pool)
            n = _stats["done_nodes"]
            total = _stats["total_nodes"]
            if n % 10 == 0 or n == total:
                elapsed = time.time() - t0
                rate = n / elapsed if elapsed > 0 else 0
                _log.info(
                    f"  [{n}/{total}] {rate:.1f}节点/s "
                    f"找到:{_stats['products_found']} "
                    f"录入:{_stats['products_saved']} "
                    f"跳过:{_stats['skipped']} "
                    f"pool={_stats['pool_usable']}可用/"
                    f"{_stats['pool_cooling']}冷却/{_stats['pool_disabled']}禁用"
                )

        pending = [
            str(n["node_id"]) for n in nodes
            if str(n["node_id"]) not in _checkpoint.done_ids()
        ]
        if pending:
            _checkpoint.set_phase("RETRY_PENDING")
            _log.error(
                "[未完成] 仍有%d个节点失败，断点已保留；下次只重试失败节点",
                len(pending),
            )
            raise SystemExit(4)

        _checkpoint.complete()
        elapsed = time.time() - t0
        _log.info(f"\n[fetch_products] 完成！"
                  f"\n  耗时: {elapsed:.0f}s"
                  f"\n  节点: {_stats['done_nodes']}/{_stats['total_nodes']}"
                  f"\n  找到: {_stats['products_found']} 个符合条件商品"
                  f"\n  录入: {_stats['products_saved']} 条（去重后）"
                  f"\n  跳过: {_stats['skipped']} 个冷门榜单"
                  f"\n  去重: {_stats['products_dup']} 个重复ASIN已跳过"
                  f"\n  错误: {_stats['errors']}")
        export_excel()
    finally:
        client.close()
        if _checkpoint is not None:
            _checkpoint.close()
            _checkpoint = None


def run_batch(root_ids: list, lists: list, review_max: int,
              min_list_size: int, delay: float = 2.0,
              price_min: float = 0.0, price_max: float = 0.0,
              review_min: int = 0,
              rating_min: float = 0.0, rating_max: float = 0.0,
              max_pages: int = 2,
              slugs: list = None,
              detail_filters: dict = None,
              list_limit: int = 0,
              include_descendants: bool = True,
              resume: bool = True,
              depths: list[int] = None):
    """Fixed-proxy producer/consumer scheduler with bounded cross-IP retries."""
    global _checkpoint, _detail_parse_pipeline, _detail_writer
    if depths:
        nodes = get_nodes_by_depth(depths, site=_SITE)
    elif slugs:
        nodes = get_nodes_by_slugs(slugs, lists, site=_SITE)
    else:
        nodes = get_descendant_nodes(
            root_ids, lists, site=_SITE, include_descendants=include_descendants
        )

    for key in _stats:
        if key not in ("pool_usable", "pool_cooling", "pool_disabled"):
            _stats[key] = 0
    _stats["total_nodes"] = len(nodes)
    with _seen_lock:
        _seen_asins.clear()
    with _stats_lock:
        _filter_field_probe.clear()
    try:
        list_limit = int(list_limit)
    except (TypeError, ValueError):
        list_limit = 0
    list_limit = max(0, min(list_limit, 100))
    max_pages = max(1, min(int(max_pages), 2))
    if not nodes:
        _log.warning("[fetch_products] no target nodes")
        return

    try:
        pool = ProxyPool()
    except ProxyRequiredError as exc:
        _log.error("[fetch_products] proxy pool unavailable: %s %s", exc.code, exc)
        raise SystemExit(2) from exc
    proxy_entries = pool.usable_entries_snapshot()
    if len(proxy_entries) < PROXY_MIN_START_NODES:
        _log.error(
            "[proxy] start refused: usable=%d minimum=%d",
            len(proxy_entries), PROXY_MIN_START_NODES,
        )
        pool.stop_live_reload()
        raise SystemExit(2)
    touch_crawl_activity(active=True, source="fetch_products")

    checkpoint_config = {
        "site": _SITE,
        "db_backend": DB_BACKEND,
        "roots": sorted(root_ids or []),
        "depths": sorted(depths or []),
        "slugs": sorted(slugs or []),
        "lists": sorted(lists or []),
        "exact_roots": not include_descendants,
        "max_pages": max_pages,
        "list_limit": list_limit,
        "review_max": review_max,
        "review_min": review_min,
        "min_list_size": min_list_size,
        "price_min": price_min,
        "price_max": price_max,
        "rating_min": rating_min,
        "rating_max": rating_max,
        "detail_filters": detail_filters or {},
        "node_count": len(nodes),
        "node_ids_sha256": hashlib.sha256(
            "\n".join(sorted(str(n["node_id"]) for n in nodes)).encode("utf-8")
        ).hexdigest(),
        "scheduler": "fixed_proxy_queue_v1",
        "streams_per_proxy": PRODUCT_STREAMS_PER_PROXY,
    }
    signature = canonical_signature(checkpoint_config)
    _checkpoint = ProductsCheckpoint(signature, checkpoint_config, resume=resume)
    done_nodes = _checkpoint.done_ids()
    remaining = [node for node in nodes if str(node["node_id"]) not in done_nodes]
    _stats["done_nodes"] = len(done_nodes)
    _update_pool_stats(pool)

    task_q = PriorityQueue()
    state_lock = threading.RLock()
    enqueue_seq = [0]
    finalized_nodes = set(done_nodes)
    worker_failures = []
    shared_auditor = AttemptAuditor(_AUDIT_PATH, _RUN_ID)
    lane_stats: dict[str, dict] = {}
    mainphase_failed_details: list[dict] = []
    t0 = time.time()

    def entry_key(entry):
        return str(
            entry.get("exit_ip")
            or entry.get("node_key")
            or entry.get("proxy")
            or ""
        )

    def _record_lane_result(key, outcome):
        with state_lock:
            st = lane_stats.setdefault(key, {"total": 0, "captcha": 0, "paused": False})
            st["total"] += 1
            if outcome.final_reason == "CAPTCHA":
                st["captcha"] += 1
            if (
                not st["paused"]
                and st["total"] >= PRODUCT_LANE_CAPTCHA_MIN_SAMPLES
                and st["captcha"] / st["total"] >= PRODUCT_LANE_CAPTCHA_PAUSE_RATE
            ):
                st["paused"] = True
                _log.warning(
                    "[fetch_products] lane %s paused: captcha=%d/%d (rate=%.0f%%)",
                    key, st["captcha"], st["total"], 100 * st["captcha"] / st["total"],
                )

    def enqueue_task(task):
        with state_lock:
            enqueue_seq[0] += 1
            sequence = enqueue_seq[0]
        if task is None:
            priority = 2
        else:
            priority = 0 if task["kind"] == "detail" else 1
        task_q.put((priority, sequence, task))

    for node in remaining:
        enqueue_task({
            "kind": "node",
            "node": node,
            "attempt": 1,
            "used_proxies": set(),
            "http_attempts": 0,
        })

    def finalize_group(group):
        with state_lock:
            if group["dispatch_open"] or group["pending_details"] > 0:
                return
            node = group["node"]
            node_id = str(node["node_id"])
            if node_id in finalized_nodes:
                return

            if group["detail_errors"]:
                status = "error"
                error_code = f"DETAIL_FETCH_FAILED:{group['detail_errors']}"
            elif group["list_status"] != "done":
                if group["node_attempt"] < PRODUCT_TASK_MAX_ATTEMPTS:
                    enqueue_task({
                        "kind": "node",
                        "node": node,
                        "attempt": group["node_attempt"] + 1,
                        "used_proxies": set(group["used_proxies"]),
                        "http_attempts": group["http_attempts"],
                    })
                    return
                status = "error"
                error_code = group["list_error"] or "LIST_FETCH_FAILED"
            else:
                status = "done"
                error_code = ""

            finalized_nodes.add(node_id)
            _checkpoint.save_node(
                node_id,
                status=status,
                error_code=error_code,
                products_found=group["products_found"],
                attempts=group["http_attempts"] + group["detail_attempts"],
            )
            with _stats_lock:
                _stats["done_nodes"] += 1
                completed = _stats["done_nodes"]
                total = _stats["total_nodes"]
            if completed % 10 == 0 or completed == total:
                elapsed_now = time.time() - t0
                rate = completed / elapsed_now if elapsed_now > 0 else 0
                _log.info(
                    "  [%d/%d] %.1f nodes/s found:%d saved:%d skipped:%d",
                    completed, total, rate, _stats["products_found"],
                    _stats["products_saved"], _stats["skipped"],
                )

    def worker_main(entry, worker_id):
        key = entry_key(entry)
        client = FixedProxyClient(
            pool, entry, worker_id, auditor=shared_auditor,
            on_result=lambda outcome: _record_lane_result(key, outcome),
        )
        try:
            while True:
                _priority, _sequence, task = task_q.get()
                try:
                    if task is None:
                        return
                    if lane_stats.get(key, {}).get("paused"):
                        enqueue_task(task)
                        return
                    used = set(task.get("used_proxies") or ())
                    if key in used and len(used) < len(proxy_entries):
                        enqueue_task(task)
                        time.sleep(0.001)
                        continue

                    if task["kind"] == "node":
                        group = {
                            "node": task["node"],
                            "node_attempt": task["attempt"],
                            "used_proxies": used | {key},
                            "http_attempts": task.get("http_attempts", 0),
                            "detail_attempts": 0,
                            "products_found": 0,
                            "pending_details": 0,
                            "detail_errors": 0,
                            "list_status": "error",
                            "list_error": "",
                            "dispatch_open": True,
                        }

                        def submit_details(products):
                            for product in products:
                                with state_lock:
                                    group["pending_details"] += 1
                                enqueue_task({
                                    "kind": "detail",
                                    "product": product,
                                    "group": group,
                                    "attempt": 1,
                                    "used_proxies": set(),
                                })

                        try:
                            status, err_code, found, attempts = process_node(
                                task["node"], lists, review_max, min_list_size,
                                client, price_min, price_max, review_min,
                                rating_min, rating_max, max_pages, delay,
                                detail_filters, list_limit,
                                detail_submit=submit_details,
                                count_completion=False,
                            )
                            group["list_status"] = status
                            group["list_error"] = err_code
                            group["products_found"] = found
                            group["http_attempts"] += attempts
                        except BaseException as exc:
                            group["list_error"] = f"{type(exc).__name__}: {exc}"
                            with state_lock:
                                worker_failures.append(
                                    (worker_id, "node", repr(exc))
                                )
                        finally:
                            with state_lock:
                                group["dispatch_open"] = False
                            finalize_group(group)
                    else:
                        group = task["group"]
                        failure = 1
                        terminal_asins: set = set()
                        try:
                            failure = enrich_with_details(
                                [task["product"]], client, delay, detail_filters,
                                terminal_asins=terminal_asins,
                            )
                        except BaseException as exc:
                            with state_lock:
                                worker_failures.append(
                                    (worker_id, "detail", repr(exc))
                                )
                        with state_lock:
                            group["detail_attempts"] += 1
                        if task["product"]["asin"] in terminal_asins:
                            # HTTP 404：商品永久不存在，不重试、不计入失败队列
                            with state_lock:
                                group["pending_details"] -= 1
                            finalize_group(group)
                        elif failure and task["attempt"] < PRODUCT_TASK_MAX_ATTEMPTS:
                            enqueue_task({
                                "kind": "detail",
                                "product": task["product"],
                                "group": group,
                                "attempt": task["attempt"] + 1,
                                "used_proxies": used | {key},
                            })
                        else:
                            with state_lock:
                                group["pending_details"] -= 1
                                if failure:
                                    group["detail_errors"] += 1
                                    mainphase_failed_details.append({
                                        "product": task["product"], "group": group,
                                    })
                            finalize_group(group)
                finally:
                    task_q.task_done()
        finally:
            client.close()

    def _run_mopup_pass():
        """主阶段结束后，用本轮实测健康的lane对 mainphase_failed_details 做一轮独立收尾补跑。
        成功或补跑中命中404的item会把对应group的detail_errors减到0；
        减到0的node重新写checkpoint为done（save_node是upsert，覆盖主阶段写入的error安全）。
        补跑仍失败的item保持原状——node维持error，checkpoint仍会在末尾拒绝complete。"""
        if not mainphase_failed_details:
            return
        healthy_entries = [
            e for e in proxy_entries
            if not lane_stats.get(entry_key(e), {}).get("paused")
        ]
        if not healthy_entries:
            _log.error(
                "[fetch_products] mopup skipped: no healthy lane left "
                "(%d failed details remain unresolved)",
                len(mainphase_failed_details),
            )
            return
        mopup_budget = min(PRODUCT_MOPUP_MAX_ATTEMPTS, len(healthy_entries))
        mopup_q: Queue = Queue()
        final_failed_details: list[dict] = []
        recovered_node_ids: set[str] = set()

        for item in mainphase_failed_details:
            mopup_q.put({
                "product": item["product"], "group": item["group"],
                "attempt": 1, "used_proxies": set(),
            })

        def mopup_worker_main(entry, worker_id):
            key = entry_key(entry)
            client = FixedProxyClient(
                pool, entry, worker_id, auditor=shared_auditor,
                on_result=lambda outcome: _record_lane_result(key, outcome),
            )
            try:
                while True:
                    item = mopup_q.get()
                    try:
                        if item is None:
                            return
                        used = set(item.get("used_proxies") or ())
                        if key in used and len(used) < len(healthy_entries):
                            mopup_q.put(item)
                            time.sleep(0.001)
                            continue
                        group = item["group"]
                        terminal_asins: set = set()
                        failure = 1
                        try:
                            failure = enrich_with_details(
                                [item["product"]], client, delay, detail_filters,
                                terminal_asins=terminal_asins,
                            )
                        except BaseException as exc:
                            with state_lock:
                                worker_failures.append(
                                    (worker_id, "mopup", repr(exc))
                                )
                        asin = item["product"]["asin"]
                        if asin in terminal_asins or not failure:
                            with state_lock:
                                group["detail_errors"] -= 1
                                recovered_node_ids.add(str(group["node"]["node_id"]))
                        elif item["attempt"] < mopup_budget:
                            mopup_q.put({
                                "product": item["product"], "group": group,
                                "attempt": item["attempt"] + 1,
                                "used_proxies": used | {key},
                            })
                        else:
                            with state_lock:
                                final_failed_details.append(item)
                    finally:
                        mopup_q.task_done()
            finally:
                client.close()

        mopup_threads = []
        for lane_idx, entry in enumerate(healthy_entries):
            for stream_idx in range(PRODUCT_STREAMS_PER_PROXY):
                worker_id = 100000 + lane_idx * PRODUCT_STREAMS_PER_PROXY + stream_idx
                thread = threading.Thread(
                    target=mopup_worker_main, args=(entry, worker_id), daemon=True,
                )
                thread.start()
                mopup_threads.append(thread)
        mopup_q.join()
        for _ in mopup_threads:
            mopup_q.put(None)
        for thread in mopup_threads:
            thread.join()

        affected_groups = {
            str(item["group"]["node"]["node_id"]): item["group"]
            for item in mainphase_failed_details
        }
        reconciled = 0
        for node_id, group in affected_groups.items():
            if group["detail_errors"] == 0:
                _checkpoint.save_node(
                    node_id, status="done", error_code="",
                    products_found=group["products_found"],
                    attempts=group["http_attempts"] + group["detail_attempts"],
                )
                reconciled += 1
        _log.info(
            "[fetch_products] mopup done: healthy_lanes=%d budget=%d "
            "items=%d recovered=%d still_failed=%d nodes_reconciled=%d",
            len(healthy_entries), mopup_budget, len(mainphase_failed_details),
            len(mainphase_failed_details) - len(final_failed_details),
            len(final_failed_details), reconciled,
        )

    worker_count = len(proxy_entries) * PRODUCT_STREAMS_PER_PROXY
    threads = []
    _detail_parse_pipeline = BoundedDetailParsePipeline(
        PRODUCT_PARSE_WORKERS, PRODUCT_PARSE_MAX_PENDING,
    )
    _detail_writer = AsyncBatchWriter(
        _write_detail_batch,
        batch_size=PRODUCT_DETAIL_WRITE_BATCH_SIZE,
        flush_interval=PRODUCT_DETAIL_WRITE_FLUSH_SEC,
        max_pending_batches=32,
    )
    try:
        _checkpoint.set_phase("LIST_DETAIL_QUEUE")
        _log.info(
            "[fetch_products] fixed queue: %d proxies x %d streams = %d workers; "
            "detail parsers=%d pending=%d",
            len(proxy_entries), PRODUCT_STREAMS_PER_PROXY, worker_count,
            PRODUCT_PARSE_WORKERS, PRODUCT_PARSE_MAX_PENDING,
        )
        for worker_id in range(worker_count):
            entry = proxy_entries[worker_id // PRODUCT_STREAMS_PER_PROXY]
            thread = threading.Thread(
                target=worker_main, args=(entry, worker_id), daemon=True,
            )
            thread.start()
            threads.append(thread)
        task_q.join()
        for _ in threads:
            enqueue_task(None)
        for thread in threads:
            thread.join()

        _run_mopup_pass()

        _detail_writer.close()
        _detail_writer = None

        pending = [
            str(node["node_id"]) for node in nodes
            if str(node["node_id"]) not in _checkpoint.done_ids()
        ]
        if worker_failures:
            _checkpoint.set_phase("WORKER_FAILURE")
            raise RuntimeError(
                f"{len(worker_failures)} worker exception(s); checkpoint retained"
            )
        if pending:
            _checkpoint.set_phase("RETRY_PENDING")
            _log.error(
                "[fetch_products] incomplete: %d nodes remain in checkpoint",
                len(pending),
            )
            raise SystemExit(4)

        _checkpoint.complete()
        elapsed = time.time() - t0
        _log.info(
            "[fetch_products] complete elapsed=%.0fs nodes=%d/%d "
            "found=%d saved=%d duplicates=%d errors=%d",
            elapsed, _stats["done_nodes"], _stats["total_nodes"],
            _stats["products_found"], _stats["products_saved"],
            _stats["products_dup"], _stats["errors"],
        )
        _warn_if_filter_field_collapsed()
        export_excel()
    finally:
        for thread in threads:
            if thread.is_alive():
                enqueue_task(None)
        for thread in threads:
            thread.join(timeout=2)
        if _detail_parse_pipeline is not None:
            _detail_parse_pipeline.shutdown(wait=True, cancel_futures=False)
            _detail_parse_pipeline = None
        if _detail_writer is not None:
            _detail_writer.close()
            _detail_writer = None
        pool.stop_live_reload()
        if _checkpoint is not None:
            _checkpoint.close()
            _checkpoint = None


# 达到多少样本才具备统计意义；样本太少（比如只测了 3 个商品）不足以判断
# "字段解析坍缩"还是"恰好没有一个是 None"，避免小样本误报。
_FIELD_PROBE_MIN_SAMPLES = 15
# None 率超过这个比例才告警；正常数据也会有一定缺失（比如部分商品确实没有
# "其他卖家"），只有接近/等于全 None 才是选择器过期的强信号。
_FIELD_PROBE_NONE_RATE_ALARM = 0.8


def _warn_if_filter_field_collapsed():
    """探针：本轮跑完后检查是否有"启用了阈值的字段，几乎全部解析为 None"。

    check_detail_filters 对 None 值和"真实不达标"一视同仁地判不通过，所以
    一旦某个字段的选择器过期（比如 Amazon 改版 DOM），筛选结果会静默地异常
    偏少甚至清零——不会报错，只会表现成"没有符合条件的商品"。这里主动把
    每个启用维度的 None 率打出来，命中阈值就高亮成 WARNING，避免每次都要
    靠人工用抓包脚本抽样才能发现。
    """
    with _stats_lock:
        probe_snapshot = {k: dict(v) for k, v in _filter_field_probe.items()}
        found = _stats.get("products_found", 0)
        saved = _stats.get("products_saved", 0)

    suspects = []
    for field, counts in probe_snapshot.items():
        total = counts["total"]
        none_n = counts["none"]
        if total < _FIELD_PROBE_MIN_SAMPLES:
            continue
        rate = none_n / total
        if rate >= _FIELD_PROBE_NONE_RATE_ALARM:
            suspects.append((field, none_n, total, rate))

    if suspects:
        for field, none_n, total, rate in sorted(suspects, key=lambda x: -x[3]):
            _log.warning(
                "[fetch_products] 疑似字段解析失效: %s 在本轮 %d 个已启用该维度筛选的详情页里 "
                "%d 个 (%.0f%%) 解析为 None——大概率是选择器/正则过期，而不是商品真的不达标，"
                "建议核对 detail_parser.py 对应字段的抓取逻辑",
                field, total, none_n, rate * 100,
            )
    elif found > 0 and saved == 0:
        _log.warning(
            "[fetch_products] found=%d 但 saved=0，且未命中字段探针阈值（样本量不足 %d 或 None 率"
            "低于 %.0f%%）——请人工核实一次抓取到的详情字段，不要默认视为\"确实没有商品达标\"",
            found, _FIELD_PROBE_MIN_SAMPLES, _FIELD_PROBE_NONE_RATE_ALARM * 100,
        )


def export_excel():
    """导出去重后的商品到 Excel。"""
    try:
        import openpyxl
    except ImportError:
        _log.error("[fetch_products] 需要 openpyxl: pip install openpyxl")
        return

    if DB_BACKEND == "pg":
        rows = _pg_fetchall("""
            SELECT asin, name, price, rating, review_count, image_url, product_url,
                   STRING_AGG(DISTINCT list_type, ',') AS appeared_lists,
                   COUNT(DISTINCT list_type) AS list_count,
                   STRING_AGG(DISTINCT category_name, ',') AS categories,
                   MIN(rank) AS best_rank,
                   MIN(scraped_at) AS first_seen
            FROM product_sightings
            GROUP BY asin, name, price, rating, review_count, image_url, product_url
            ORDER BY list_count DESC, review_count ASC
        """)
    else:
        conn = db_conn()
        rows = conn.execute("""
            SELECT asin, name, price, price_raw, original_price, discount_pct,
                   rating, review_count, image_url, product_url,
                   has_video, is_amazon_choice,
                   GROUP_CONCAT(DISTINCT list_type) AS appeared_lists,
                   COUNT(DISTINCT list_type) AS list_count,
                   GROUP_CONCAT(DISTINCT category_name) AS categories,
                   MIN(rank) AS best_rank,
                   MIN(scraped_at) AS first_seen
            FROM product_sightings
            GROUP BY asin
            ORDER BY list_count DESC, review_count ASC
        """).fetchall()
        conn.close()

    if not rows:
        _log.warning("[fetch_products] 无数据可导出")
        return

    excel_path = os.path.join(BASE, "data", "products.xlsx")
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "筛选结果"

    headers = ["ASIN", "商品名", "价格", "价格原始", "原价", "折扣",
               "评分", "评论数", "图片URL", "商品URL",
               "有视频", "Amazon's Choice",
               "出现榜单", "榜单数", "所属类目", "最佳排名", "首次发现"]
    ws.append(headers)

    for r in rows:
        ws.append(list(r))

    wb.save(excel_path)
    _log.info(f"[fetch_products] Excel 已导出: {excel_path} ({len(rows)} 行)")


# ── CLI 入口 ────────────────────────────────────────────────────────

if __name__ == "__main__":
    assert_testing_paths_safe()
    parser = argparse.ArgumentParser(description="Amazon 榜单商品抓取")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--roots", nargs="+",
                       help="根节点 node_id 列表")
    group.add_argument("--slugs", nargs="+",
                       help="L1 类目 slug 列表 (如 automotive baby-products)")
    group.add_argument("--depth", nargs="+", type=int,
                       help="抓取指定精确层级（可传多个层级，包含 L0 和末级类目）")
    parser.add_argument("--site", default="US", help="站点代码: US, DE, JP, UK, FR")
    parser.add_argument("--review-max", type=int, default=0)
    parser.add_argument("--review-min", type=int, default=0)
    parser.add_argument("--min-list",   type=int, default=0)
    parser.add_argument("--price-min",  type=float, default=DEFAULT_PRICE_MIN)
    parser.add_argument("--price-max",  type=float, default=DEFAULT_PRICE_MAX)
    parser.add_argument("--rating-min", type=float, default=0)
    parser.add_argument("--rating-max", type=float, default=0)
    parser.add_argument("--bsr-main-min", type=int, default=0)
    parser.add_argument("--bsr-main-max", type=int, default=0)
    parser.add_argument("--bsr-sub-min",  type=int, default=0)
    parser.add_argument("--bsr-sub-max",  type=int, default=0)
    parser.add_argument("--variant-min",  type=int, default=0)
    parser.add_argument("--variant-max",  type=int, default=0)
    parser.add_argument("--sellers-min",  type=int, default=0)
    parser.add_argument("--sellers-max",  type=int, default=0)
    parser.add_argument("--social-proof-min", type=int, default=0)
    parser.add_argument("--weight-min", type=float, default=0)
    parser.add_argument("--weight-max", type=float, default=0)
    parser.add_argument("--dim-l", type=float, default=0)
    parser.add_argument("--dim-w", type=float, default=0)
    parser.add_argument("--dim-h", type=float, default=0)
    parser.add_argument("--list-limit", type=int, default=0)
    parser.add_argument("--fba-fee-min", type=float, default=0)
    parser.add_argument("--fba-fee-max", type=float, default=0)
    parser.add_argument("--fulfillment-type", default="")
    parser.add_argument("--country", default="")
    parser.add_argument("--date-range", default="")
    parser.add_argument("--date-from",  default="")
    parser.add_argument("--date-to",    default="")
    parser.add_argument("--amazons-choice", action="store_true")
    parser.add_argument("--bestseller",     action="store_true")
    parser.add_argument("--max-pages", type=int, default=2)
    parser.add_argument("--delay",      type=float, default=DEFAULT_DELAY)
    parser.add_argument("--lists", nargs="+", default=DEFAULT_LISTS)
    parser.add_argument(
        "--exact-roots", action="store_true",
        help="仅抓 --roots 所选类目本身，不展开全部下级（默认会展开）",
    )
    parser.add_argument("--no-resume", action="store_true", help="忽略同配置断点并重新开始")
    args = parser.parse_args()
    args.list_limit = max(0, min(args.list_limit, 100))
    args.max_pages = max(1, min(args.max_pages, 2))

    mp = get_marketplace(args.site)
    _mp = mp
    _SITE   = args.site.upper()
    _DOMAIN = mp["domain"]
    _LANG   = mp["lang"]
    _CURRENCY    = mp["currency"]
    _DECIMAL_SEP = mp["decimal_sep"]
    _RATING_PAT  = mp["rating_pattern"]
    _RESULTS_PAT = mp["results_pattern"]
    _log.info(f"[站点] {mp['name']} ({_SITE}) → {_DOMAIN}")

    detail_filters = {
        "bsr_main_min": args.bsr_main_min, "bsr_main_max": args.bsr_main_max,
        "bsr_sub_min": args.bsr_sub_min, "bsr_sub_max": args.bsr_sub_max,
        "variant_min": args.variant_min, "variant_max": args.variant_max,
        "sellers_min": args.sellers_min, "sellers_max": args.sellers_max,
        "social_proof_min": args.social_proof_min,
        "weight_min": args.weight_min, "weight_max": args.weight_max,
        "dim_l": args.dim_l, "dim_w": args.dim_w, "dim_h": args.dim_h,
        "fba_fee_min": args.fba_fee_min, "fba_fee_max": args.fba_fee_max,
        "fulfillment_type": args.fulfillment_type,
        "country": args.country,
        "amazons_choice": args.amazons_choice,
        "bestseller": args.bestseller,
        "date_range": args.date_range,
        "date_from": args.date_from, "date_to": args.date_to,
    }
    detail_filters = {k: v for k, v in detail_filters.items() if v}

    run_batch(
        root_ids=args.roots or [],
        lists=args.lists,
        review_max=args.review_max,
        min_list_size=args.min_list,
        delay=args.delay,
        price_min=args.price_min,
        price_max=args.price_max,
        review_min=args.review_min,
        rating_min=args.rating_min,
        rating_max=args.rating_max,
        max_pages=args.max_pages,
        slugs=args.slugs,
        detail_filters=detail_filters,
        list_limit=args.list_limit,
        include_descendants=not args.exact_roots,
        resume=not args.no_resume,
        depths=args.depth,
    )
