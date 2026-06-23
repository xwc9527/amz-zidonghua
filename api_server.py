# api_server.py — FastAPI v2 API (阶段2: 读取同一 SQLite 库)
# 启动: uvicorn api_server:app --host 0.0.0.0 --port 8081
import os, sys, subprocess, threading, json
import aiosqlite
from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "categories.db")

_product_proc = None
_product_lock = threading.Lock()

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    with _product_lock:
        global _product_proc
        if _product_proc and _product_proc.poll() is None:
            _product_proc.terminate()
            _product_proc = None

app = FastAPI(title="Amazon 选品看板 API", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

async def db_query(sql, params=()):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cur:
            return [dict(r) for r in await cur.fetchall()]

async def db_scalar(sql, params=()):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(sql, params) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

# ── 统计 ──

@app.get("/api/v2/stats")
async def stats():
    total = await db_scalar("SELECT COUNT(*) FROM categories")
    queue = await db_scalar("SELECT COUNT(*) FROM categories WHERE explored=0")
    has_id = await db_scalar("SELECT COUNT(*) FROM categories WHERE node_id IS NOT NULL")
    bc = await db_scalar("SELECT COUNT(*) FROM categories WHERE source='breadcrumb'")
    validated = await db_scalar("SELECT COUNT(*) FROM categories WHERE nr_valid IS NOT NULL")
    depths = {}
    for r in await db_query("SELECT depth, COUNT(*) as cnt FROM categories GROUP BY depth ORDER BY depth"):
        depths[f"L{r['depth']}"] = r['cnt']
    return {"total": total, "queue": queue, "has_id": has_id,
            "breadcrumb": bc, "validated": validated, "depths": depths}

# ── 类目树 ──

@app.get("/api/v2/tree_children")
async def tree_children(parent: str = "", q: str = "", limit: int = 50, offset: int = 0):
    limit = min(limit, 200)
    if parent == "root":
        total = await db_scalar("SELECT COUNT(*) FROM categories")
        return [{"name": "Home & Kitchen", "node_id": "home-garden", "depth": 0, "child_count": total}]
    elif parent == "home-garden":
        sql = "SELECT c.name, c.node_id, c.depth, c.slug, c.child_count FROM categories c WHERE c.depth = 1"
        params = []
        if q:
            sql += " AND c.name LIKE ?"
            params.append(f"%{q}%")
        sql += f" ORDER BY c.name LIMIT {limit} OFFSET {offset}"
        return await db_query(sql, params)
    else:
        sql = "SELECT c.name, c.node_id, c.depth, c.slug, c.child_count FROM categories c WHERE c.parent_node_id = ?"
        params = [parent]
        if q:
            sql += " AND c.name LIKE ?"
            params.append(f"%{q}%")
        sql += f" ORDER BY c.name LIMIT {limit} OFFSET {offset}"
        return await db_query(sql, params)

# ── 商品 ──

@app.get("/api/v2/products")
async def products(limit: int = Query(50, le=200), offset: int = 0,
                   price_min: float = None, price_max: float = None,
                   rating_min: float = None, rating_max: float = None,
                   review_min: int = None, review_max: int = None):
    sql = "SELECT name, asin, price, review_count, rank, rating, image_url, product_url, list_type, category_name, scraped_at FROM product_sightings WHERE 1=1"
    params = []
    if price_min is not None:
        sql += " AND price >= ?"; params.append(price_min)
    if price_max is not None:
        sql += " AND price <= ?"; params.append(price_max)
    if rating_min is not None:
        sql += " AND rating >= ?"; params.append(rating_min)
    if rating_max is not None:
        sql += " AND rating <= ?"; params.append(rating_max)
    if review_min is not None:
        sql += " AND review_count >= ?"; params.append(review_min)
    if review_max is not None:
        sql += " AND review_count <= ?"; params.append(review_max)
    sql += f" ORDER BY scraped_at DESC LIMIT {limit} OFFSET {offset}"
    try:
        return await db_query(sql, params)
    except Exception:
        return []

@app.get("/api/v2/product_stats")
async def product_stats():
    try:
        total = await db_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
        by_list = await db_query("SELECT list_type, COUNT(*) as cnt FROM product_sightings GROUP BY list_type")
        multi = await db_scalar(
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
        total = await db_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
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

# ── 静态文件 ──
app.mount("/", StaticFiles(directory=os.path.join(BASE_DIR, "data"), html=True), name="static")
