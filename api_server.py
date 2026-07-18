# api_server.py — FastAPI v2 API (asyncpg + PostgreSQL)
# 启动: DB_BACKEND=sqlite uvicorn api_server:app --host 127.0.0.1 --port 8081
# PG:   set PG_DSN=postgresql://user:pass@localhost:5432/amz_selection
import os, sys, subprocess, threading, logging, time, math
import asyncpg
from fastapi import FastAPI, Query, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_BACKEND = os.getenv("DB_BACKEND", "pg")

# PG config（SQLite 模式可不设置 PG_DSN）
from pg_config import PG_DSN, get_pg_dsn

# SQLite fallback
DB_PATH = os.path.join(BASE_DIR, "data", "categories.db")

_product_proc = None
_product_lock = threading.Lock()
_pool = None

def _positive_int(value, default):
    try:
        v = int(value)
    except (TypeError, ValueError):
        return default
    return v if v > 0 else default

_RANGE_CHECKS = [
    ("price_min", "price_max", "现价"),
    ("rating_min", "rating_max", "评分"),
    ("review_min", "review_max", "评论数"),
    ("bsr_main_min", "bsr_main_max", "BSR大类排名"),
    ("bsr_sub_min", "bsr_sub_max", "BSR子类排名"),
    ("variant_min", "variant_max", "变体选项数"),
    ("sellers_min", "sellers_max", "其他卖家数"),
    ("weight_min", "weight_max", "商品重量"),
    ("fba_fee_min", "fba_fee_max", "FBA运费"),
]

# 不允许为负的筛选字段（含区间两端与尺寸上限）
_NONNEG_FILTER_KEYS = [
    ("price_min", "现价最小"), ("price_max", "现价最大"),
    ("rating_min", "评分最小"), ("rating_max", "评分最大"),
    ("review_min", "评论数最小"), ("review_max", "评论数最大"),
    ("bsr_main_min", "BSR大类排名最小"), ("bsr_main_max", "BSR大类排名最大"),
    ("bsr_sub_min", "BSR子类排名最小"), ("bsr_sub_max", "BSR子类排名最大"),
    ("variant_min", "变体选项数最小"), ("variant_max", "变体选项数最大"),
    ("sellers_min", "其他卖家数最小"), ("sellers_max", "其他卖家数最大"),
    ("weight_min", "商品重量最小"), ("weight_max", "商品重量最大"),
    ("fba_fee_min", "FBA运费最小"), ("fba_fee_max", "FBA运费最大"),
    ("dim_l", "尺寸长"), ("dim_w", "尺寸宽"), ("dim_h", "尺寸高"),
]

# 与 argparse type=int 对齐：评论数 / BSR / 变体 / 卖家
_INT_FILTER_KEYS = {
    "review_min", "review_max",
    "bsr_main_min", "bsr_main_max",
    "bsr_sub_min", "bsr_sub_max",
    "variant_min", "variant_max",
    "sellers_min", "sellers_max",
}


def _parse_filter_number(raw, *, as_int: bool, label: str):
    """解析数值筛选；失败返回错误文案，成功返回 (None, number)。"""
    if isinstance(raw, float) and not math.isfinite(raw):
        return (f"{label} must be finite (received {raw!r})", None)
    if isinstance(raw, str):
        text = raw.strip()
        if not text or not text.isascii():
            return (f"{label} must be an ASCII number (received {raw!r})", None)
        try:
            if not math.isfinite(float(text)):
                return (f"{label} must be finite (received {raw!r})", None)
        except ValueError:
            pass
    if isinstance(raw, bool):
        return (f"{label}必须是{'整数' if as_int else '数字'}（收到 {raw!r}）", None)
    if as_int:
        if isinstance(raw, float):
            if not raw.is_integer():
                return (f"{label}必须是整数（收到 {raw!r}）", None)
            return (None, int(raw))
        if isinstance(raw, int):
            return (None, raw)
        try:
            return (None, int(str(raw).strip()))
        except (TypeError, ValueError):
            return (f"{label}必须是整数（收到 {raw!r}）", None)
    if isinstance(raw, (int, float)):
        return (None, float(raw))
    try:
        return (None, float(str(raw).strip()))
    except (TypeError, ValueError):
        return (f"{label}必须是数字（收到 {raw!r}）", None)


def _validate_start_filters(body: dict) -> str | None:
    """校验筛选区间合法性，返回错误信息；合法返回 None。
    与前端 validateFilterRanges 逻辑保持一致，防止绕过前端直接调用 API。
    数值转换失败必须拒绝（禁止假成功启动子进程）。"""
    for key, label in _NONNEG_FILTER_KEYS:
        raw = body.get(key)
        if raw is None or raw == "":
            continue
        err, v = _parse_filter_number(raw, as_int=(key in _INT_FILTER_KEYS), label=label)
        if err:
            return err
        if v < 0:
            return f"{label}不能为负数 ({v:g})"

    for min_key, max_key, label in _RANGE_CHECKS:
        raw_min, raw_max = body.get(min_key), body.get(max_key)
        # 空值按 0；非空已在上面校验过类型
        try:
            vmin = float(raw_min) if raw_min not in (None, "", False) else 0.0
            vmax = float(raw_max) if raw_max not in (None, "", False) else 0.0
        except (TypeError, ValueError):
            return f"{label}必须是数字"
        if vmin > 0 and vmax > 0 and vmin > vmax:
            return f"{label}：最小值 ({vmin:g}) 大于最大值 ({vmax:g})"
    rmin, rmax = body.get("rating_min"), body.get("rating_max")
    try:
        if (rmin not in (None, "", False) and float(rmin) > 5) or (
            rmax not in (None, "", False) and float(rmax) > 5
        ):
            return "评分筛选超出合理范围 (0~5)"
    except (TypeError, ValueError):
        return "评分必须是数字"
    date_range = body.get("date_range")
    if date_range == "custom":
        df, dt = body.get("date_from"), body.get("date_to")
        if df and dt and str(df) > str(dt):
            return "上架日期：起始日期晚于结束日期"
    elif date_range not in (None, ""):
        try:
            days = int(str(date_range).strip())
        except (TypeError, ValueError):
            return "上架日期天数必须是大于等于 1 的整数"
        if days < 1 or str(date_range).strip() != str(days):
            return "上架日期天数必须是大于等于 1 的整数"
    return None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    if DB_BACKEND == "pg":
        _pool = await asyncpg.create_pool(get_pg_dsn(), min_size=2, max_size=10)
    yield
    if _pool:
        await _pool.close()
    with _product_lock:
        global _product_proc
        if _product_proc and _product_proc.poll() is None:
            _product_proc.terminate()
            _product_proc = None

app = FastAPI(title="Amazon 选品看板 API", lifespan=lifespan)

# CORS：默认仅本机；局域网访问时可通过 CORS_ORIGINS 追加，例如
#   set CORS_ORIGINS=http://192.168.1.10:8081,http://localhost:8081
_DEFAULT_CORS = [
    "http://127.0.0.1:8081",
    "http://localhost:8081",
    "http://127.0.0.1:8080",
    "http://localhost:8080",
    "null",  # file:// 打开 dashboard 时 Origin 为 null
]
_extra_cors = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
_cors_origins = _DEFAULT_CORS + _extra_cors
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    if request.url.path.startswith("/api/"):
        t0 = time.time()
        response = await call_next(request)
        ms = (time.time() - t0) * 1000
        logging.info(f"{request.method} {request.url.path} {response.status_code} {ms:.0f}ms")
        return response
    return await call_next(request)

@app.get("/api/v2/health")
async def health():
    if DB_BACKEND == "pg" and _pool:
        try:
            async with _pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return {"status": "ok", "backend": "pg", "pool_size": _pool.get_size()}
        except Exception as e:
            return JSONResponse({"status": "error", "msg": str(e)}, status_code=503)
    return {"status": "ok", "backend": DB_BACKEND}


@app.get("/api/v2/fba_support")
async def fba_support(site: str = None):
    """返回各站点 FBA 运费估算支持状态；可传 site 查单站。"""
    from fba_fees_us import FBA_SUPPORTED, FBA_UNSUPPORTED, fba_support_info
    if site:
        return fba_support_info(site)
    all_sites = sorted(set(FBA_SUPPORTED) | set(FBA_UNSUPPORTED) | {
        "US", "UK", "DE", "FR", "IT", "ES", "JP", "NL", "SE", "PL", "BE",
        "CA", "AU", "IN", "MX", "BR", "SG", "SA", "AE", "TR", "EG",
    })
    return {s: fba_support_info(s) for s in all_sites}

# ── DB helpers ──

async def pg_query(sql, *args):
    async with _pool.acquire() as conn:
        rows = await conn.fetch(sql, *args)
        return [dict(r) for r in rows]

async def pg_scalar(sql, *args):
    async with _pool.acquire() as conn:
        return await conn.fetchval(sql, *args) or 0

async def pg_exec(sql, *args):
    async with _pool.acquire() as conn:
        await conn.execute(sql, *args)

# SQLite fallback
async def _sqlite_query(sql, params=()):
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cur:
            return [dict(r) for r in await cur.fetchall()]

async def _sqlite_scalar(sql, params=()):
    import aiosqlite
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, params) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

_PRODUCT_EXTRA_COLS = [
    ("site", "TEXT"),
    ("detail_scraped", "INTEGER DEFAULT 0"),
    ("bsr_main_rank", "INTEGER"),
    ("bsr_main_category", "TEXT"),
    ("bsr_sub_rank", "INTEGER"),
    ("bsr_sub_category", "TEXT"),
    ("variant_option_count", "INTEGER"),
    ("other_sellers_count", "INTEGER"),
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
    ("is_amazon_choice", "INTEGER DEFAULT 0"),
    ("is_bestseller", "INTEGER DEFAULT 0"),
]

async def _ensure_product_columns():
    """保证 product_sightings 具备结果筛选所需列，并回填规范化重量/尺寸。"""
    import aiosqlite
    from fba_fees_us import parse_weight_lb, parse_dims_inches
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("PRAGMA table_info(product_sightings)")
        existing = {r[1] for r in await cur.fetchall()}
        for col, typ in _PRODUCT_EXTRA_COLS:
            if col not in existing:
                await db.execute(f"ALTER TABLE product_sightings ADD COLUMN {col} {typ}")
        await db.commit()
        # 回填：有文本重量/尺寸但缺数值字段的历史行
        cur = await db.execute(
            """SELECT rowid, item_weight, item_dimensions FROM product_sightings
               WHERE (item_weight IS NOT NULL AND item_weight != '' AND weight_lb IS NULL)
                  OR (item_dimensions IS NOT NULL AND item_dimensions != ''
                      AND (dim_l_in IS NULL OR dim_w_in IS NULL OR dim_h_in IS NULL))"""
        )
        rows = await cur.fetchall()
        for rowid, wtxt, dtxt in rows:
            w = parse_weight_lb(wtxt)
            dims = parse_dims_inches(dtxt)
            if w is None and dims is None:
                continue
            sets, params = [], []
            if w is not None:
                sets.append("weight_lb=?")
                params.append(round(w, 4))
            if dims is not None:
                sets.extend(["dim_l_in=?", "dim_w_in=?", "dim_h_in=?"])
                params.extend([round(dims[0], 4), round(dims[1], 4), round(dims[2], 4)])
            params.append(rowid)
            await db.execute(
                f"UPDATE product_sightings SET {', '.join(sets)} WHERE rowid=?", params
            )
        if rows:
            await db.commit()

_product_cols_ready = False

async def _sqlite_query_products(sql, params=()):
    global _product_cols_ready
    if not _product_cols_ready:
        await _ensure_product_columns()
        _product_cols_ready = True
    return await _sqlite_query(sql, params)

# ── 统计 ──

@app.get("/api/v2/stats")
async def stats():
    if DB_BACKEND == "pg":
        total = await pg_scalar("SELECT COUNT(*) FROM categories")
        queue = await pg_scalar("SELECT COUNT(*) FROM categories WHERE explored=0")
        has_id = await pg_scalar("SELECT COUNT(*) FROM categories WHERE node_id IS NOT NULL")
        bc = await pg_scalar("SELECT COUNT(*) FROM categories WHERE source='breadcrumb'")
        validated = await pg_scalar("SELECT COUNT(*) FROM categories WHERE nr_valid IS NOT NULL")
        rows = await pg_query("SELECT depth, COUNT(*) as cnt FROM categories GROUP BY depth ORDER BY depth")
        depths = {f"L{r['depth']}": r['cnt'] for r in rows}
    else:
        total = await _sqlite_scalar("SELECT COUNT(*) FROM categories")
        queue = await _sqlite_scalar("SELECT COUNT(*) FROM categories WHERE explored=0")
        has_id = await _sqlite_scalar("SELECT COUNT(*) FROM categories WHERE node_id IS NOT NULL")
        bc = await _sqlite_scalar("SELECT COUNT(*) FROM categories WHERE source='breadcrumb'")
        validated = await _sqlite_scalar("SELECT COUNT(*) FROM categories WHERE nr_valid IS NOT NULL")
        rows = await _sqlite_query("SELECT depth, COUNT(*) as cnt FROM categories GROUP BY depth ORDER BY depth")
        depths = {f"L{r['depth']}": r['cnt'] for r in rows}
    return {"total": total, "queue": queue, "has_id": has_id,
            "breadcrumb": bc, "validated": validated, "depths": depths}

# ── 类目树 ──

@app.get("/api/v2/tree_children")
async def tree_children(parent: str = "", q: str = "", limit: int = 50, offset: int = 0, site: str = "US", na_only: int = 0):
    limit = min(limit, 200)
    site = site.upper()
    if DB_BACKEND == "pg":
        return await _tree_children_pg(parent, q, limit, offset, site, na_only)
    else:
        return await _tree_children_sqlite(parent, q, limit, offset, site, na_only)

async def _tree_children_pg(parent, q, limit, offset, site="US", na_only=0):
    if na_only:
        sql = "SELECT name, node_id, depth, slug, na_valid, 0 as child_count FROM categories WHERE site = $1 AND na_valid = 1"
        args = [site]
        if q:
            sql += " AND name ILIKE $2"
            args.append(f"%{q}%")
            sql += f" ORDER BY depth, name LIMIT ${len(args)+1} OFFSET ${len(args)+2}"
        else:
            sql += f" ORDER BY depth, name LIMIT $2 OFFSET $3"
        args.extend([limit, offset])
        return await pg_query(sql, *args)
    if parent == "root":
        total = await pg_scalar("SELECT COUNT(*) FROM categories WHERE site = $1", site)
        root_rows = await pg_query("SELECT node_id, name FROM categories WHERE depth = 0 AND site = $1", site)
        if root_rows:
            return [{"name": r["name"], "node_id": r["node_id"], "depth": 0, "child_count": total} for r in root_rows]
        return [{"name": "All Categories", "node_id": "_root_", "depth": 0, "child_count": total}]
    elif (await pg_scalar("SELECT COUNT(*) FROM categories WHERE node_id=$1 AND depth=0 AND site=$2", parent, site)) > 0:
        sql = """SELECT c.name, c.node_id, c.depth, c.slug, c.na_valid,
                 (SELECT COUNT(*) FROM categories c2 WHERE c2.path <@ c.path AND c2.id != c.id) as child_count
                 FROM categories c WHERE c.parent_node_id = $1 AND c.site = $2"""
        args = [parent, site]
        if q:
            sql += " AND (c.name ILIKE $" + str(len(args)+1) + " OR c.name % $" + str(len(args)+1) + ")"
            args.append(f"%{q}%")
            sql += " ORDER BY similarity(c.name, $" + str(len(args)) + ") DESC"
        else:
            sql += " ORDER BY c.name"
        sql += " LIMIT $" + str(len(args)+1) + " OFFSET $" + str(len(args)+2)
        args.extend([limit, offset])
        return await pg_query(sql, *args)
    else:
        sql = """SELECT c.name, c.node_id, c.depth, c.slug, c.na_valid,
                 (SELECT COUNT(*) FROM categories c2 WHERE c2.path <@ c.path AND c2.id != c.id) as child_count
                 FROM categories c WHERE c.parent_node_id = $1 AND c.site = $2"""
        args = [parent, site]
        if q:
            sql += " AND (c.name ILIKE $3 OR c.name % $3)"
            args.append(f"%{q}%")
            sql += " ORDER BY similarity(c.name, $3) DESC"
        else:
            sql += " ORDER BY c.name"
        sql += f" LIMIT ${len(args)+1} OFFSET ${len(args)+2}"
        args.extend([limit, offset])
        return await pg_query(sql, *args)

async def _tree_children_sqlite(parent, q, limit, offset, site="US", na_only=0):
    # na_only 模式：平铺返回所有 na_valid=1 的节点，忽略层级
    if na_only:
        sql = """SELECT name, node_id, depth, slug, na_valid, 0 as child_count
                 FROM categories WHERE site = ? AND na_valid = 1"""
        params = [site]
        if q:
            sql += " AND name LIKE ?"
            params.append(f"%{q}%")
        sql += f" ORDER BY depth, name LIMIT {limit} OFFSET {offset}"
        return await _sqlite_query(sql, params)

    if parent == "root":
        # 递归统计某根 node_id 下全部后代（通过 parent_node_id 链）
        async def _descendant_count(root_node_id):
            return await _sqlite_scalar(
                """WITH RECURSIVE sub AS (
                       SELECT node_id FROM categories WHERE parent_node_id = ? AND site = ?
                       UNION ALL
                       SELECT c.node_id FROM categories c JOIN sub s ON c.parent_node_id = s.node_id WHERE c.site = ?
                   ) SELECT COUNT(*) FROM sub""",
                (root_node_id, site, site))
        # 先查 depth=0 根节点（新版爬虫自动创建的）
        roots = await _sqlite_query(
            "SELECT name, node_id FROM categories WHERE depth = 0 AND site = ? ORDER BY name", (site,))
        if roots:
            result = []
            for r in roots:
                cc = await _descendant_count(r["node_id"])
                result.append({"name": r["name"], "node_id": r["node_id"], "depth": 0, "child_count": cc})
            return result
        # 兼容旧数据：没有 depth=0 节点时，从 depth=1 的 parent_node_id 反推出 slug 列表作为根
        slug_rows = await _sqlite_query(
            "SELECT DISTINCT parent_node_id as slug FROM categories "
            "WHERE site = ? AND depth = 1 AND parent_node_id IS NOT NULL AND parent_node_id != '' "
            "ORDER BY parent_node_id", (site,))
        if slug_rows:
            result = []
            for r in slug_rows:
                slug = r["slug"]
                cc = await _descendant_count(slug)
                display_name = slug.replace("-", " ").title()
                result.append({"name": display_name, "node_id": slug, "depth": 0, "child_count": cc})
            return result
        total = await _sqlite_scalar("SELECT COUNT(*) FROM categories WHERE site = ?", (site,))
        return [{"name": "All Categories", "node_id": "_root_", "depth": 0, "child_count": total}]
    else:
        sql = """SELECT c.name, c.node_id, c.depth, c.slug, c.na_valid,
                 (WITH RECURSIVE sub AS (
                     SELECT node_id FROM categories WHERE parent_node_id = c.node_id
                     UNION ALL
                     SELECT cat.node_id FROM categories cat JOIN sub s ON cat.parent_node_id = s.node_id
                 ) SELECT COUNT(*) FROM sub) as child_count
                 FROM categories c WHERE c.parent_node_id = ? AND c.site = ?"""
        params = [parent, site]
        if q:
            sql += " AND c.name LIKE ?"
            params.append(f"%{q}%")
        sql += f" ORDER BY c.name LIMIT {limit} OFFSET {offset}"
        return await _sqlite_query(sql, params)

# ── 商品 ──

def _parse_weight_lb(text):
    if not text:
        return None
    import re
    m = re.search(r"([\d,.]+)", str(text))
    if not m:
        return None
    v = float(m.group(1).replace(",", "."))
    low = str(text).lower()
    if "kg" in low or "kilogramm" in low:
        return v * 2.205
    if "ounce" in low or "oz" in low:
        return v / 16.0
    if "gramm" in low or (re.search(r"\bg\b", low) and "kg" not in low):
        return v * 0.0022
    return v


def _parse_dims_inches(text):
    if not text:
        return None
    import re
    nums = [float(x.replace(",", ".")) for x in re.findall(r"[\d,.]+", str(text))]
    if len(nums) < 3:
        return None  # 尺寸缺失：严格不通过（由调用方判定）
    if "cm" in str(text).lower():
        nums = [n / 2.54 for n in nums[:3]]
    else:
        nums = nums[:3]
    nums.sort(reverse=True)
    return nums


def _products_post_filter(rows, weight_min=None, weight_max=None, dim_l=None, dim_w=None, dim_h=None):
    """重量/尺寸用文本字段，在应用层做严格过滤。"""
    out = []
    need_w = (weight_min is not None and weight_min > 0) or (weight_max is not None and weight_max > 0)
    need_d = any(v is not None and v > 0 for v in (dim_l, dim_w, dim_h))
    for r in rows:
        row = dict(r) if not isinstance(r, dict) else r
        if need_w:
            wv = _parse_weight_lb(row.get("item_weight"))
            if wv is None:
                continue
            if weight_min and wv < weight_min:
                continue
            if weight_max and wv > weight_max:
                continue
        if need_d:
            dims = _parse_dims_inches(row.get("item_dimensions"))
            if dims is None:
                continue  # 尺寸缺失严格不通过
            if dim_l and dims[0] > dim_l:
                continue
            if dim_w and dims[1] > dim_w:
                continue
            if dim_h and dims[2] > dim_h:
                continue
        out.append(row)
    return out


@app.get("/api/v2/products")
async def products(
    limit: int = Query(50, le=200), offset: int = 0,
    price_min: float = None, price_max: float = None,
    rating_min: float = None, rating_max: float = None,
    review_min: int = None, review_max: int = None,
    bsr_main_min: int = None, bsr_main_max: int = None,
    bsr_sub_min: int = None, bsr_sub_max: int = None,
    variant_min: int = None, variant_max: int = None,
    sellers_min: int = None, sellers_max: int = None,
    weight_min: float = None, weight_max: float = None,
    dim_l: float = None, dim_w: float = None, dim_h: float = None,
    fba_fee_min: float = None, fba_fee_max: float = None,
    fulfillment_type: str = None, country: str = None,
    amazons_choice: bool = False, bestseller: bool = False,
    date_range: str = None, date_from: str = None, date_to: str = None,
    site: str = Query(..., description="Required marketplace site, e.g. US"),
    detail_only: bool = True,
):
    """商品结果查询。site 必填；默认只返回详情抓取成功的记录。"""
    filters = dict(
        limit=limit, offset=offset,
        price_min=price_min, price_max=price_max,
        rating_min=rating_min, rating_max=rating_max,
        review_min=review_min, review_max=review_max,
        bsr_main_min=bsr_main_min, bsr_main_max=bsr_main_max,
        bsr_sub_min=bsr_sub_min, bsr_sub_max=bsr_sub_max,
        variant_min=variant_min, variant_max=variant_max,
        sellers_min=sellers_min, sellers_max=sellers_max,
        weight_min=weight_min, weight_max=weight_max,
        dim_l=dim_l, dim_w=dim_w, dim_h=dim_h,
        fba_fee_min=fba_fee_min, fba_fee_max=fba_fee_max,
        fulfillment_type=fulfillment_type, country=country,
        amazons_choice=amazons_choice, bestseller=bestseller,
        date_range=date_range, date_from=date_from, date_to=date_to,
        site=site.upper(), detail_only=detail_only,
    )
    range_err = _validate_start_filters(filters)
    if range_err:
        return JSONResponse({"status": "error", "msg": f"筛选条件不合法: {range_err}"}, status_code=400)
    if DB_BACKEND == "pg":
        return await _products_pg(**filters)
    return await _products_sqlite(**filters)


def _build_product_where(filters, style="sqlite"):
    """构建 WHERE 子句。style=sqlite 用 ?；pg 用 $n。"""
    sql = " WHERE 1=1"
    params = []
    ph = lambda: "?" if style == "sqlite" else f"${len(params)+1}"

    sql += f" AND site = {ph()}"
    params.append(filters["site"])

    if filters.get("detail_only", True):
        sql += f" AND detail_scraped = {ph()}"
        params.append(1)

    for key, op, col in [
        ("price_min", ">=", "price"), ("price_max", "<=", "price"),
        ("rating_min", ">=", "rating"), ("rating_max", "<=", "rating"),
        ("review_min", ">=", "review_count"), ("review_max", "<=", "review_count"),
        ("bsr_main_min", ">=", "bsr_main_rank"), ("bsr_main_max", "<=", "bsr_main_rank"),
        ("bsr_sub_min", ">=", "bsr_sub_rank"), ("bsr_sub_max", "<=", "bsr_sub_rank"),
        ("variant_min", ">=", "variant_option_count"), ("variant_max", "<=", "variant_option_count"),
        ("sellers_min", ">=", "other_sellers_count"), ("sellers_max", "<=", "other_sellers_count"),
        ("fba_fee_min", ">=", "fba_fee"), ("fba_fee_max", "<=", "fba_fee"),
        ("weight_min", ">=", "weight_lb"), ("weight_max", "<=", "weight_lb"),
    ]:
        val = filters.get(key)
        if val is not None and val != 0 and val is not False:
            # 数值筛选要求字段非空（缺失值不通过）
            sql += f" AND {col} IS NOT NULL AND {col} {op} {ph()}"
            params.append(val)

    # 尺寸上限：商品长/宽/高（inch，已排序）不得超过筛选上限
    for key, col in (("dim_l", "dim_l_in"), ("dim_w", "dim_w_in"), ("dim_h", "dim_h_in")):
        val = filters.get(key)
        if val is not None and val != 0:
            sql += f" AND {col} IS NOT NULL AND {col} <= {ph()}"
            params.append(val)

    ft = filters.get("fulfillment_type") or ""
    if ft:
        sql += f" AND fulfillment_type = {ph()}"
        params.append(ft)

    country = (filters.get("country") or "").strip()
    if country:
        sql += f" AND country_of_origin IS NOT NULL AND LOWER(country_of_origin) LIKE {ph()}"
        params.append(f"%{country.lower()}%")

    if filters.get("amazons_choice"):
        sql += f" AND is_amazon_choice = {ph()}"
        params.append(1)
    if filters.get("bestseller"):
        sql += f" AND is_bestseller = {ph()}"
        params.append(1)

    date_range = filters.get("date_range") or ""
    if date_range:
        sql += " AND date_first_available IS NOT NULL"
        if date_range == "custom":
            if filters.get("date_from"):
                sql += f" AND date_first_available >= {ph()}"
                params.append(filters["date_from"])
            if filters.get("date_to"):
                sql += f" AND date_first_available <= {ph()}"
                params.append(filters["date_to"])
        else:
            try:
                days = int(date_range)
                from datetime import datetime, timedelta
                cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
                sql += f" AND date_first_available >= {ph()}"
                params.append(cutoff)
            except ValueError:
                pass

    return sql, params


async def _products_pg(**filters):
    sql = """SELECT name, asin, price, review_count, rank, rating, image_url, product_url,
             list_type, category_name, site, scraped_at,
             bsr_main_rank, bsr_main_category, bsr_sub_rank, bsr_sub_category,
             variant_option_count, other_sellers_count, item_weight, item_dimensions,
             weight_lb, dim_l_in, dim_w_in, dim_h_in,
             date_first_available, shipping_fee, shipping_fee_value, fulfillment_type,
             country_of_origin, is_amazon_choice, is_bestseller, fba_fee, placement_fee,
             detail_scraped
             FROM product_sightings"""
    where, params = _build_product_where(filters, style="pg")
    sql += where
    limit = filters["limit"]
    offset = filters["offset"]
    sql += f" ORDER BY scraped_at DESC LIMIT ${len(params)+1} OFFSET ${len(params)+2}"
    params.extend([limit, offset])
    try:
        return await pg_query(sql, *params)
    except Exception:
        return []


async def _products_sqlite(**filters):
    sql = """SELECT name, asin, price, price_raw, review_count, rank, rating,
             image_url, product_url, list_type, category_name, scraped_at, site,
             bsr_main_rank, bsr_main_category, bsr_sub_rank, bsr_sub_category,
             variant_option_count, other_sellers_count, item_weight, item_dimensions,
             weight_lb, dim_l_in, dim_w_in, dim_h_in,
             date_first_available, shipping_fee, shipping_fee_value, fulfillment_type,
             country_of_origin, is_amazon_choice, is_bestseller, fba_fee, placement_fee,
             detail_scraped
             FROM product_sightings"""
    where, params = _build_product_where(filters, style="sqlite")
    sql += where
    limit = filters["limit"]
    offset = filters["offset"]
    sql += f" ORDER BY scraped_at DESC LIMIT {int(limit)} OFFSET {int(offset)}"
    try:
        return await _sqlite_query_products(sql, params)
    except Exception as e:
        logging.warning(f"_products_sqlite query failed: {e}")
        return []

def _build_new_arrivals_where(filters: dict, style: str = "sqlite"):
    """new_arrivals 查询 WHERE：字段与 product_sightings 略有差异（price_value / listing_date）。
    style=sqlite 用 ?；pg 用 $n。"""
    sql = " WHERE 1=1"
    params = []
    ph = lambda: "?" if style == "sqlite" else f"${len(params)+1}"

    if filters.get("site"):
        sql += f" AND site = {ph()}"
        params.append(filters["site"])

    for key, op, col in [
        ("price_min", ">=", "price_value"), ("price_max", "<=", "price_value"),
        ("rating_min", ">=", "rating"), ("rating_max", "<=", "rating"),
        ("review_min", ">=", "review_count"), ("review_max", "<=", "review_count"),
        ("bsr_main_min", ">=", "bsr_main_rank"), ("bsr_main_max", "<=", "bsr_main_rank"),
        ("bsr_sub_min", ">=", "bsr_sub_rank"), ("bsr_sub_max", "<=", "bsr_sub_rank"),
        ("variant_min", ">=", "variant_option_count"), ("variant_max", "<=", "variant_option_count"),
        ("sellers_min", ">=", "other_sellers_count"), ("sellers_max", "<=", "other_sellers_count"),
        ("fba_fee_min", ">=", "fba_fee"), ("fba_fee_max", "<=", "fba_fee"),
        ("weight_min", ">=", "weight_lb"), ("weight_max", "<=", "weight_lb"),
    ]:
        val = filters.get(key)
        if val is not None and val != 0 and val is not False:
            sql += f" AND {col} IS NOT NULL AND {col} {op} {ph()}"
            params.append(val)

    for key, col in (("dim_l", "dim_l_in"), ("dim_w", "dim_w_in"), ("dim_h", "dim_h_in")):
        val = filters.get(key)
        if val is not None and val != 0:
            sql += f" AND {col} IS NOT NULL AND {col} <= {ph()}"
            params.append(val)

    ft = filters.get("fulfillment_type") or ""
    if ft:
        sql += f" AND fulfillment_type = {ph()}"
        params.append(ft)

    country = (filters.get("country") or "").strip()
    if country:
        sql += f" AND country_of_origin IS NOT NULL AND LOWER(country_of_origin) LIKE {ph()}"
        params.append(f"%{country.lower()}%")

    if filters.get("amazons_choice"):
        sql += f" AND is_amazon_choice = {ph()}"
        params.append(1)
    if filters.get("bestseller"):
        sql += f" AND is_bestseller = {ph()}"
        params.append(1)

    date_range = filters.get("date_range") or ""
    if date_range:
        sql += " AND listing_date IS NOT NULL"
        if date_range == "custom":
            if filters.get("date_from"):
                sql += f" AND listing_date >= {ph()}"
                params.append(filters["date_from"])
            if filters.get("date_to"):
                sql += f" AND listing_date <= {ph()}"
                params.append(filters["date_to"])
        else:
            try:
                days = int(date_range)
                from datetime import datetime, timedelta
                cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
                sql += f" AND listing_date >= {ph()}"
                params.append(cutoff)
            except ValueError:
                pass

    return sql, params


_NA_SELECT = """SELECT asin, title AS name, title, price, price_value, rating, review_count,
              listing_date, listing_date AS date_first_available, listing_age_days,
              bsr_main_category, bsr_main_rank, bsr_sub_rank, bsr_sub_category,
              image_url, product_url, node_id, category_name, category_depth,
              site, item_weight, item_dimensions, weight_lb, dim_l_in, dim_w_in, dim_h_in,
              fba_fee, placement_fee, fulfillment_type, country_of_origin,
              is_amazon_choice, is_bestseller, scraped_at
              FROM new_arrivals"""


async def _new_arrivals_sqlite(filters: dict):
    try:
        import fetch_new_arrivals as _na_mod
        _na_mod._init_db()
    except Exception as e:
        logging.warning(f"new_arrivals schema ensure failed: {e}")
    where, params = _build_new_arrivals_where(filters, style="sqlite")
    limit = int(filters.get("limit") or 50)
    offset = int(filters.get("offset") or 0)
    sql = f"{_NA_SELECT}{where} ORDER BY scraped_at DESC LIMIT {limit} OFFSET {offset}"
    try:
        return await _sqlite_query(sql, params)
    except Exception as e:
        logging.warning(f"new_arrivals sqlite query failed: {e}")
        return []


async def _new_arrivals_pg(filters: dict):
    try:
        import fetch_new_arrivals as _na_mod
        # 确保 PG 端表存在（与 sqlite 分支 _init_db 对齐）
        _na_mod._init_db()
    except Exception as e:
        logging.warning(f"new_arrivals pg schema ensure failed: {e}")
    where, params = _build_new_arrivals_where(filters, style="pg")
    limit = int(filters.get("limit") or 50)
    offset = int(filters.get("offset") or 0)
    sql = (
        f"{_NA_SELECT}{where} ORDER BY scraped_at DESC "
        f"LIMIT ${len(params)+1} OFFSET ${len(params)+2}"
    )
    params.extend([limit, offset])
    try:
        return await pg_query(sql, *params)
    except Exception as e:
        logging.warning(f"new_arrivals pg query failed: {e}")
        return []


@app.get("/api/v2/new_arrivals")
async def new_arrivals_api(
    limit: int = Query(50, le=200), offset: int = 0,
    price_min: float = None, price_max: float = None,
    rating_min: float = None, rating_max: float = None,
    review_min: int = None, review_max: int = None,
    bsr_main_min: int = None, bsr_main_max: int = None,
    bsr_sub_min: int = None, bsr_sub_max: int = None,
    variant_min: int = None, variant_max: int = None,
    sellers_min: int = None, sellers_max: int = None,
    weight_min: float = None, weight_max: float = None,
    dim_l: float = None, dim_w: float = None, dim_h: float = None,
    fba_fee_min: float = None, fba_fee_max: float = None,
    fulfillment_type: str = None, country: str = None,
    amazons_choice: bool = False, bestseller: bool = False,
    date_range: str = None, date_from: str = None, date_to: str = None,
    site: str = None,
):
    """读取 new_arrivals；筛选参数与 /api/v2/products 对齐。"""
    filters = dict(
        limit=limit, offset=offset,
        price_min=price_min, price_max=price_max,
        rating_min=rating_min, rating_max=rating_max,
        review_min=review_min, review_max=review_max,
        bsr_main_min=bsr_main_min, bsr_main_max=bsr_main_max,
        bsr_sub_min=bsr_sub_min, bsr_sub_max=bsr_sub_max,
        variant_min=variant_min, variant_max=variant_max,
        sellers_min=sellers_min, sellers_max=sellers_max,
        weight_min=weight_min, weight_max=weight_max,
        dim_l=dim_l, dim_w=dim_w, dim_h=dim_h,
        fba_fee_min=fba_fee_min, fba_fee_max=fba_fee_max,
        fulfillment_type=fulfillment_type, country=country,
        amazons_choice=amazons_choice, bestseller=bestseller,
        date_range=date_range, date_from=date_from, date_to=date_to,
        site=site.upper() if site else None,
    )
    range_err = _validate_start_filters(filters)
    if range_err:
        return JSONResponse({"status": "error", "msg": f"筛选条件不合法: {range_err}"}, status_code=400)
    if DB_BACKEND == "pg":
        return await _new_arrivals_pg(filters)
    return await _new_arrivals_sqlite(filters)

@app.get("/api/v2/product_stats")
async def product_stats():
    running = _product_proc is not None and _product_proc.poll() is None
    total, by_list, multi, na_total = 0, [], 0, 0
    try:
        if DB_BACKEND == "pg":
            total = await pg_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
            by_list = await pg_query("SELECT list_type, COUNT(*) as cnt FROM product_sightings GROUP BY list_type")
            multi = await pg_scalar(
                "SELECT COUNT(*) FROM (SELECT asin FROM product_sightings GROUP BY asin HAVING COUNT(DISTINCT list_type)>1) t"
            )
            try:
                na_total = await pg_scalar("SELECT COUNT(DISTINCT asin) FROM new_arrivals")
            except Exception as e:
                logging.warning(f"new_arrivals pg stats failed: {e}")
        else:
            try:
                total = await _sqlite_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
                by_list = await _sqlite_query("SELECT list_type, COUNT(*) as cnt FROM product_sightings GROUP BY list_type")
                multi = await _sqlite_scalar(
                    "SELECT COUNT(*) FROM (SELECT asin FROM product_sightings GROUP BY asin HAVING COUNT(DISTINCT list_type)>1)"
                )
            except Exception as e:
                logging.warning(f"product_sightings stats failed: {e}")
            try:
                na_total = await _sqlite_scalar("SELECT COUNT(DISTINCT asin) FROM new_arrivals")
            except Exception as e:
                logging.warning(f"new_arrivals stats failed: {e}")
    except Exception as e:
        logging.warning(f"product_stats failed: {e}")
    return {"total_asins": total, "new_arrivals": na_total, "by_list": by_list or [], "multi_list": multi, "running": running}

@app.get("/api/v2/product_progress")
async def product_progress():
    running = _product_proc is not None and _product_proc.poll() is None
    ps, na = 0, 0
    try:
        if DB_BACKEND == "pg":
            try:
                ps = await pg_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
            except Exception:
                ps = 0
            try:
                na = await pg_scalar("SELECT COUNT(DISTINCT asin) FROM new_arrivals")
            except Exception:
                na = 0
        else:
            try:
                ps = await _sqlite_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
            except Exception:
                ps = 0
            try:
                na = await _sqlite_scalar("SELECT COUNT(DISTINCT asin) FROM new_arrivals")
            except Exception:
                na = 0
    except Exception:
        pass
    return {"running": running, "total_products": ps + na, "product_sightings": ps, "new_arrivals": na}

# ── 爬虫控制 ──

_FILTER_PARAM_FLAGS = [
    ("review_max", "--review-max"), ("review_min", "--review-min"),
    ("min_list", "--min-list"),
    ("price_min", "--price-min"), ("price_max", "--price-max"),
    ("rating_min", "--rating-min"), ("rating_max", "--rating-max"),
    ("bsr_main_min", "--bsr-main-min"), ("bsr_main_max", "--bsr-main-max"),
    ("bsr_sub_min", "--bsr-sub-min"), ("bsr_sub_max", "--bsr-sub-max"),
    ("variant_min", "--variant-min"), ("variant_max", "--variant-max"),
    ("sellers_min", "--sellers-min"), ("sellers_max", "--sellers-max"),
    ("weight_min", "--weight-min"), ("weight_max", "--weight-max"),
    ("dim_l", "--dim-l"), ("dim_w", "--dim-w"), ("dim_h", "--dim-h"),
    ("fba_fee_min", "--fba-fee-min"), ("fba_fee_max", "--fba-fee-max"),
    ("fulfillment_type", "--fulfillment-type"),
    ("country", "--country"),
    ("date_range", "--date-range"), ("date_from", "--date-from"), ("date_to", "--date-to"),
    ("delay", "--delay"),
]

# 最新到货脚本不接受的 CLI（榜单专用）
_LA_SKIP_FLAGS = {"min_list", "delay"}


def _append_filter_flags(cmd: list, body: dict, *, for_la: bool = False):
    if _validate_start_filters(body):
        return
    for key, flag in _FILTER_PARAM_FLAGS:
        if for_la and key in _LA_SKIP_FLAGS:
            continue
        v = body.get(key)
        if v and str(v) != "0":
            cmd += [flag, str(v)]
    if body.get("amazons_choice"):
        cmd += ["--amazons-choice"]
    if body.get("bestseller"):
        cmd += ["--bestseller"]


@app.post("/api/v2/start_products")
async def start_products(body: dict):
    global _product_proc
    with _product_lock:
        if _product_proc is not None and _product_proc.poll() is None:
            return {"status": "already_running"}
        range_err = _validate_start_filters(body)
        if range_err:
            return {"status": "error", "msg": f"筛选条件不合法: {range_err}"}
        chart = body.get("chart", "")
        # include_descendants 默认 True（兼容旧行为：所选 + 全部下级）
        include_descendants = body.get("include_descendants", True)
        if isinstance(include_descendants, str):
            include_descendants = include_descendants.strip().lower() not in ("0", "false", "no", "off")
        else:
            include_descendants = bool(include_descendants)

        if chart == "la":
            cmd = [sys.executable, "-u", os.path.join(BASE_DIR, "fetch_new_arrivals.py")]
            if body.get("roots"):
                cmd += ["--roots"] + body["roots"]
            else:
                return {"status": "error", "msg": "latest arrivals requires roots"}
            if body.get("site"):
                cmd += ["--site", body["site"]]
            page_cap = min(_positive_int(body.get("max_pages"), 2), 999)
            cmd += ["--max-pages", str(page_cap)]
            if not include_descendants:
                cmd += ["--exact-roots"]
            _append_filter_flags(cmd, body, for_la=True)
            env = os.environ.copy()
            env["DB_BACKEND"] = DB_BACKEND
            _product_proc = subprocess.Popen(cmd, cwd=BASE_DIR, env=env)
            return {"status": "started", "pid": _product_proc.pid, "backend": DB_BACKEND}

        cmd = [sys.executable, "-u", os.path.join(BASE_DIR, "fetch_products.py")]
        if body.get("slugs"):
            cmd += ["--slugs"] + body["slugs"]
        elif body.get("roots"):
            cmd += ["--roots"] + body["roots"]
        else:
            return {"status": "error", "msg": "no slugs or roots specified"}
        if body.get("lists"):
            cmd += ["--lists"] + body["lists"]
        if body.get("site"):
            cmd += ["--site", body["site"]]
        page_cap = min(_positive_int(body.get("max_pages"), 2), 2)
        cmd += ["--list-limit", "0", "--max-pages", str(page_cap)]
        if not include_descendants:
            cmd += ["--exact-roots"]
        _append_filter_flags(cmd, body, for_la=False)
        env = os.environ.copy()
        env["DB_BACKEND"] = DB_BACKEND
        _product_proc = subprocess.Popen(cmd, cwd=BASE_DIR, env=env)
    return {"status": "started", "pid": _product_proc.pid, "backend": DB_BACKEND}

@app.post("/api/v2/stop_products")
async def stop_products():
    global _product_proc
    with _product_lock:
        if _product_proc is None or _product_proc.poll() is not None:
            return {"status": "not_running"}
        _product_proc.terminate()
        pid = _product_proc.pid
        _product_proc = None
    return {"status": "stopped", "pid": pid}

@app.post("/api/v2/export_excel")
async def export_excel(body: dict = None):
    """按当前看板模式导出：chart=la → new_arrivals，否则 → product_sightings。"""
    body = body or {}
    chart = (body.get("chart") or "").strip()
    site = body.get("site")
    try:
        if chart == "la":
            import fetch_new_arrivals
            path = fetch_new_arrivals.export_excel(
                site=site,
                signal_only=bool(body.get("signal_only")),
            )
            return {"status": "ok", "file": path, "source": "new_arrivals"}
        import fetch_products
        fetch_products.export_excel()
        return {"status": "ok", "file": "data/products.xlsx", "source": "product_sightings"}
    except Exception as e:
        return {"status": "error", "msg": str(e)}

# ── 选品清单 ──

@app.get("/api/v2/watchlist")
async def get_watchlist():
    if DB_BACKEND == "pg":
        return await pg_query("SELECT id, asin, name, notes, added_at FROM watchlist ORDER BY added_at DESC")
    return []

@app.post("/api/v2/watchlist")
async def add_to_watchlist(body: dict):
    asin = body.get("asin", "").strip()
    name = body.get("name", "")
    notes = body.get("notes", "")
    if not asin:
        return {"status": "error", "msg": "asin required"}
    if DB_BACKEND == "pg":
        try:
            await pg_exec(
                "INSERT INTO watchlist (asin, name, notes) VALUES ($1, $2, $3) ON CONFLICT(asin) DO UPDATE SET name=$2, notes=$3",
                asin, name, notes
            )
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "msg": str(e)}
    return {"status": "error", "msg": "pg only"}

@app.delete("/api/v2/watchlist/{asin}")
async def remove_from_watchlist(asin: str):
    if DB_BACKEND == "pg":
        await pg_exec("DELETE FROM watchlist WHERE asin=$1", asin)
    return {"status": "ok"}

# ── 追踪数据 ──

@app.get("/api/v2/tracking/{asin}")
async def get_tracking(asin: str):
    if DB_BACKEND == "pg":
        return await pg_query(
            "SELECT price, rank, rating, review_count, snapshot_date FROM tracking WHERE asin=$1 ORDER BY snapshot_date",
            asin
        )
    return []

# ── 静态文件 ──
app.mount("/", StaticFiles(directory=os.path.join(BASE_DIR, "data"), html=True), name="static")
