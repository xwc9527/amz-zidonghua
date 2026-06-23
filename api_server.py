# api_server.py — FastAPI v2 API (asyncpg + PostgreSQL)
# 启动: uvicorn api_server:app --host 0.0.0.0 --port 8081
# 回退: DB_BACKEND=sqlite uvicorn api_server:app --port 8081
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

# PG config
from pg_config import PG_DSN

# SQLite fallback
DB_PATH = os.path.join(BASE_DIR, "data", "categories.db")

_product_proc = None
_product_lock = threading.Lock()
_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _pool
    if DB_BACKEND == "pg":
        _pool = await asyncpg.create_pool(PG_DSN, min_size=2, max_size=10)
    yield
    if _pool:
        await _pool.close()
    with _product_lock:
        global _product_proc
        if _product_proc and _product_proc.poll() is None:
            _product_proc.terminate()
            _product_proc = None

app = FastAPI(title="Amazon 选品看板 API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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
async def tree_children(parent: str = "", q: str = "", limit: int = 50, offset: int = 0):
    limit = min(limit, 200)
    if DB_BACKEND == "pg":
        return await _tree_children_pg(parent, q, limit, offset)
    else:
        return await _tree_children_sqlite(parent, q, limit, offset)

async def _tree_children_pg(parent, q, limit, offset):
    if parent == "root":
        total = await pg_scalar("SELECT COUNT(*) FROM categories")
        return [{"name": "All Categories", "node_id": "home-garden", "depth": 0, "child_count": total}]
    elif parent == "home-garden":
        sql = """SELECT c.name, c.node_id, c.depth, c.slug,
                 (SELECT COUNT(*) FROM categories c2 WHERE c2.path <@ c.path AND c2.id != c.id) as child_count
                 FROM categories c WHERE c.depth = 1"""
        args = []
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
        sql = """SELECT c.name, c.node_id, c.depth, c.slug,
                 (SELECT COUNT(*) FROM categories c2 WHERE c2.path <@ c.path AND c2.id != c.id) as child_count
                 FROM categories c WHERE c.parent_node_id = $1"""
        args = [parent]
        if q:
            sql += " AND (c.name ILIKE $2 OR c.name % $2)"
            args.append(f"%{q}%")
            sql += " ORDER BY similarity(c.name, $2) DESC"
        else:
            sql += " ORDER BY c.name"
        sql += f" LIMIT ${len(args)+1} OFFSET ${len(args)+2}"
        args.extend([limit, offset])
        return await pg_query(sql, *args)

async def _tree_children_sqlite(parent, q, limit, offset):
    if parent == "root":
        total = await _sqlite_scalar("SELECT COUNT(*) FROM categories")
        return [{"name": "Home & Kitchen", "node_id": "home-garden", "depth": 0, "child_count": total}]
    elif parent == "home-garden":
        sql = "SELECT name, node_id, depth, slug, child_count FROM categories WHERE depth = 1"
        params = []
        if q:
            sql += " AND name LIKE ?"
            params.append(f"%{q}%")
        sql += f" ORDER BY name LIMIT {limit} OFFSET {offset}"
        return await _sqlite_query(sql, params)
    else:
        sql = "SELECT name, node_id, depth, slug, child_count FROM categories WHERE parent_node_id = ?"
        params = [parent]
        if q:
            sql += " AND name LIKE ?"
            params.append(f"%{q}%")
        sql += f" ORDER BY name LIMIT {limit} OFFSET {offset}"
        return await _sqlite_query(sql, params)

# ── 商品 ──

@app.get("/api/v2/products")
async def products(limit: int = Query(50, le=200), offset: int = 0,
                   price_min: float = None, price_max: float = None,
                   rating_min: float = None, rating_max: float = None,
                   review_min: int = None, review_max: int = None):
    if DB_BACKEND == "pg":
        return await _products_pg(limit, offset, price_min, price_max, rating_min, rating_max, review_min, review_max)
    else:
        return await _products_sqlite(limit, offset, price_min, price_max, rating_min, rating_max, review_min, review_max)

async def _products_pg(limit, offset, price_min, price_max, rating_min, rating_max, review_min, review_max):
    sql = "SELECT name, asin, price, review_count, rank, rating, image_url, product_url, list_type, category_name, scraped_at FROM product_sightings WHERE 1=1"
    args = []
    idx = 1
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
    sql = "SELECT name, asin, price, review_count, rank, rating, image_url, product_url, list_type, category_name, scraped_at FROM product_sightings WHERE 1=1"
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

@app.get("/api/v2/product_stats")
async def product_stats():
    try:
        if DB_BACKEND == "pg":
            total = await pg_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
            by_list = await pg_query("SELECT list_type, COUNT(*) as cnt FROM product_sightings GROUP BY list_type")
            multi = await pg_scalar(
                "SELECT COUNT(*) FROM (SELECT asin FROM product_sightings GROUP BY asin HAVING COUNT(DISTINCT list_type)>1) t"
            )
        else:
            total = await _sqlite_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
            by_list = await _sqlite_query("SELECT list_type, COUNT(*) as cnt FROM product_sightings GROUP BY list_type")
            multi = await _sqlite_scalar(
                "SELECT COUNT(*) FROM (SELECT asin FROM product_sightings GROUP BY asin HAVING COUNT(DISTINCT list_type)>1)"
            )
        running = _product_proc is not None and _product_proc.poll() is None
        return {"total_asins": total, "by_list": by_list, "multi_list": multi, "running": running}
    except Exception:
        return {"total_asins": 0, "by_list": [], "multi_list": 0, "running": False}

@app.get("/api/v2/product_progress")
async def product_progress():
    running = _product_proc is not None and _product_proc.poll() is None
    try:
        if DB_BACKEND == "pg":
            total = await pg_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
        else:
            total = await _sqlite_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
    except Exception:
        total = 0
    return {"running": running, "total_products": total}

# ── 爬虫控制 ──

@app.post("/api/v2/start_products")
async def start_products(body: dict):
    global _product_proc
    with _product_lock:
        if _product_proc is not None and _product_proc.poll() is None:
            return {"status": "already_running"}
        cmd = [sys.executable, "-u", os.path.join(BASE_DIR, "fetch_products.py")]
        if body.get("slugs"):
            cmd += ["--slugs"] + body["slugs"]
        elif body.get("roots"):
            cmd += ["--roots"] + body["roots"]
        else:
            return {"status": "error", "msg": "no slugs or roots specified"}
        if body.get("lists"):
            cmd += ["--lists"] + body["lists"]
        for key, flag in [("review_max", "--review-max"), ("min_list", "--min-list"),
                          ("price_min", "--price-min"), ("price_max", "--price-max"),
                          ("delay", "--delay")]:
            if body.get(key):
                cmd += [flag, str(body[key])]
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
async def export_excel():
    try:
        import fetch_products
        fetch_products.export_excel()
        return {"status": "ok"}
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
