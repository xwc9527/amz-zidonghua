# api_server.py — FastAPI v2 API (asyncpg + PostgreSQL)
# 启动: DB_BACKEND=sqlite uvicorn api_server:app --host 127.0.0.1 --port 8081
# PG:   set PG_DSN=postgresql://user:pass@localhost:5432/amz_selection
import os, sys, subprocess, threading, logging, time
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

@app.get("/api/v2/products")
async def products(limit: int = Query(50, le=200), offset: int = 0,
                   price_min: float = None, price_max: float = None,
                   rating_min: float = None, rating_max: float = None,
                   review_min: int = None, review_max: int = None,
                   site: str = None):
    if DB_BACKEND == "pg":
        return await _products_pg(limit, offset, price_min, price_max, rating_min, rating_max, review_min, review_max, site)
    else:
        return await _products_sqlite(limit, offset, price_min, price_max, rating_min, rating_max, review_min, review_max)

async def _products_pg(limit, offset, price_min, price_max, rating_min, rating_max, review_min, review_max, site=None):
    sql = """SELECT name, asin, price, review_count, rank, rating, image_url, product_url,
             list_type, category_name, site, scraped_at,
             bsr_main_rank, bsr_main_category, bsr_sub_rank, bsr_sub_category,
             variant_option_count, other_sellers_count, item_weight, item_dimensions,
             date_first_available, shipping_fee, shipping_fee_value, fulfillment_type,
             country_of_origin
             FROM product_sightings WHERE 1=1"""
    args = []
    idx = 1
    if site:
        sql += f" AND site = ${idx}"
        args.append(site.upper())
        idx += 1
    for val, op, col in [(price_min, ">=", "price"), (price_max, "<=", "price"),
                          (rating_min, ">=", "rating"), (rating_max, "<=", "rating"),
                          (review_min, ">=", "review_count"), (review_max, "<=", "review_count")]:
        if val is not None:
            sql += f" AND {col} {op} ${idx}"
            args.append(val)
            idx += 1
    sql += f" ORDER BY scraped_at DESC LIMIT ${idx} OFFSET ${idx+1}"
    args.extend([limit, offset])
    try:
        return await pg_query(sql, *args)
    except Exception:
        return []

async def _products_sqlite(limit, offset, price_min, price_max, rating_min, rating_max, review_min, review_max):
    sql = """SELECT name, asin, price, price_raw, review_count, rank, rating,
             image_url, product_url, list_type, category_name, scraped_at,
             bsr_main_rank, bsr_main_category, bsr_sub_rank, bsr_sub_category,
             variant_option_count, other_sellers_count, item_weight, item_dimensions,
             date_first_available, shipping_fee, shipping_fee_value, fulfillment_type,
             country_of_origin
             FROM product_sightings WHERE 1=1"""
    params = []
    for val, op, col in [(price_min, ">=", "price"), (price_max, "<=", "price"),
                          (rating_min, ">=", "rating"), (rating_max, "<=", "rating"),
                          (review_min, ">=", "review_count"), (review_max, "<=", "review_count")]:
        if val is not None:
            sql += f" AND {col} {op} ?"
            params.append(val)
    sql += f" ORDER BY scraped_at DESC LIMIT {limit} OFFSET {offset}"
    try:
        return await _sqlite_query(sql, params)
    except Exception:
        return []

@app.get("/api/v2/new_arrivals")
async def new_arrivals_api(
    limit: int = Query(50, le=200), offset: int = 0,
    site: str = None, signal_only: bool = False,
):
    """读取 fetch_new_arrivals.py 写入的 new_arrivals 表，打通看板展示链路。"""
    if DB_BACKEND == "pg":
        # PG 路径尚未建 new_arrivals 表；短期仅 SQLite 闭环
        return []
    cond = "WHERE 1=1"
    params = []
    if site:
        cond += " AND site=?"
        params.append(site.upper())
    if signal_only:
        cond += " AND is_signal=1"
    # 字段别名对齐看板 /api/v2/products 渲染（name / date_first_available）
    sql = f"""SELECT asin, title AS name, title, price, price_value, rating, review_count,
              listing_date, listing_date AS date_first_available, listing_age_days,
              bsr_main_category, bsr_main_rank, bsr_sub, bsr_sub AS bsr_sub_category,
              image_url, product_url, node_id, category_name, category_depth,
              site, is_signal, scraped_at
              FROM new_arrivals {cond}
              ORDER BY scraped_at DESC LIMIT {int(limit)} OFFSET {int(offset)}"""
    try:
        return await _sqlite_query(sql, params)
    except Exception as e:
        logging.warning(f"new_arrivals query failed: {e}")
        return []

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
            ps = await pg_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
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

@app.post("/api/v2/start_products")
async def start_products(body: dict):
    global _product_proc
    with _product_lock:
        if _product_proc is not None and _product_proc.poll() is None:
            return {"status": "already_running"}
        chart = body.get("chart", "")
        if chart == "la":
            cmd = [sys.executable, "-u", os.path.join(BASE_DIR, "fetch_new_arrivals.py")]
            if body.get("roots"):
                cmd += ["--roots"] + body["roots"]
            else:
                return {"status": "error", "msg": "latest arrivals requires roots"}
            if body.get("site"):
                cmd += ["--site", body["site"]]
            cmd += ["--max-pages", str(_positive_int(body.get("max_pages"), 10))]
            _product_proc = subprocess.Popen(cmd, cwd=BASE_DIR)
            return {"status": "started", "pid": _product_proc.pid}

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
        list_limit = min(_positive_int(body.get("list_limit"), 10), 100)
        page_cap = max(5, (list_limit + 23) // 24)
        cmd += ["--list-limit", str(list_limit), "--max-pages", str(page_cap)]
        param_flags = [
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
            ("shipping_fee", "--shipping-fee"), ("shipping_op", "--shipping-op"),
            ("shipping_val", "--shipping-val"),
            ("fulfillment_type", "--fulfillment-type"),
            ("country", "--country"),
            ("date_range", "--date-range"), ("date_from", "--date-from"), ("date_to", "--date-to"),
            ("delay", "--delay"),
        ]
        for key, flag in param_flags:
            v = body.get(key)
            if v and str(v) != "0":
                cmd += [flag, str(v)]
        if body.get("amazons_choice"):
            cmd += ["--amazons-choice"]
        if body.get("bestseller"):
            cmd += ["--bestseller"]
        _product_proc = subprocess.Popen(cmd, cwd=BASE_DIR)
    return {"status": "started", "pid": _product_proc.pid}

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
