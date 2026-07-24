# api_server.py — FastAPI v2 API (asyncpg + PostgreSQL)
# 启动: DB_BACKEND=sqlite uvicorn api_server:app --host 127.0.0.1 --port 8081
# PG:   set PG_DSN=postgresql://user:pass@localhost:5432/amz_selection
import os, sys, subprocess, threading, logging, time, math, asyncio
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
from config import (
    DB_FILE, PROXY_MIN_START_NODES, PROXY_RUNTIME_RECOVERY_ATTEMPTS,
    assert_testing_paths_safe, is_testing, use_run_cache,
)
import product_run_cache as run_cache
import favorite_products as fav_store

# 代理池：API 运行期间守护进程持续验证/增补/淘汰节点；点击开始时只需
# 活池达到最低启动门槛。API 正常关闭时默认连带清理，避免后台孤儿进程；
# 只有显式 PROXY_DAEMON_PERSIST=1 才允许它跨 API 生命周期常驻。
from proxy_pool_manager import (
    STATUS_IDLE,
    STATUS_PREPARING,
    STATUS_PROXY_FAILED,
    STATUS_PROXY_READY,
    STATUS_RETRY_PENDING,
    STATUS_RUNNING,
    STATUS_STARTING_CRAWLER,
    STATUS_STOPPING,
    check_pool_quality,
    daemon_alive,
    daemon_status,
    ensure_daemon_running,
    ensure_proxy_ready,
    get_status as get_proxy_status,
    set_status as set_proxy_status,
    stop_daemon,
    stop_proxy_pool,
)

# SQLite fallback（尊重 DB_FILE / AMZ_DB_FILE 隔离变量）
DB_PATH = DB_FILE

_product_proc = None
_product_lock = threading.Lock()
_pool = None
_proxy_prepare_lock = threading.Lock()

# 代际计数器：区分"看门狗线程等待的那一轮"与"当前实际在跑的那一轮"，
# 避免旧看门狗在新一轮已启动后误杀新 Mihomo / 误写 idle 状态。
_crawl_generation = 0

_ACTIVE_CRAWL_LIFECYCLES = {
    STATUS_PREPARING,
    STATUS_PROXY_READY,
    STATUS_STARTING_CRAWLER,
    STATUS_RUNNING,
    STATUS_STOPPING,
}


def _crawl_lifecycle_state():
    """Return one consistent UI state for proxy preparation and crawler execution."""
    proc_running = _product_proc is not None and _product_proc.poll() is None
    proxy_state = get_proxy_status()
    lifecycle = proxy_state.get("status") or STATUS_IDLE
    active = proc_running or lifecycle in _ACTIVE_CRAWL_LIFECYCLES
    return active, lifecycle, proxy_state.get("run_id") or ""


def _proxy_sleep(reason: str = "idle_timeout"):
    """重置抓取生命周期状态为 idle。

    新架构下常驻验证守护进程与其独立 Mihomo 一直运行（不再随每轮抓取
    启停），这里不再停止 Mihomo；stop_proxy_pool() 现在只重置状态。
    """
    try:
        stop_proxy_pool()
        logging.info("[proxy] 抓取生命周期已重置为 idle reason=%s", reason)
    except Exception as e:
        logging.warning(f"[proxy] 重置状态失败: {e}")


def _watch_and_sleep_proxy(
    proc: subprocess.Popen,
    generation: int,
    request_id: str,
    run_id: str,
    started_at: float,
    cmd: list[str] | None = None,
    env: dict | None = None,
    recovery_attempt: int = 0,
):
    """后台线程：等抓取子进程退出后停止独立代理池。

    仅当自己仍是"当前这一轮"时才执行停止/置状态，防止旧看门狗
    在用户快速点击 停止→开始 后，误杀新一轮刚启动的 Mihomo。
    """
    return_code = None
    try:
        return_code = proc.wait()
    finally:
        logging.info(
            "[crawl] process exited request_id=%s run_id=%s pid=%s return_code=%s elapsed=%.1fs",
            request_id,
            run_id,
            proc.pid,
            return_code,
            time.monotonic() - started_at,
        )
        with _product_lock:
            still_current = (generation == _crawl_generation)
        if not still_current:
            logging.info("[proxy] 检测到更新一轮已启动，跳过本轮看门狗的停止操作")
            return

        if return_code == 3:
            _recover_proxy_and_resume(
                generation=generation,
                request_id=request_id,
                previous_run_id=run_id,
                cmd=cmd,
                env=env,
                recovery_attempt=recovery_attempt,
            )
        elif return_code == 0:
            # 常驻守护进程/独立 Mihomo 一直运行（不随抓取轮次启停），
            # 这里只需把抓取生命周期状态复位为 idle。
            set_proxy_status(STATUS_IDLE, run_id=run_id, pool_ready=True)
            logging.info("[proxy] 抓取自然结束 run_id=%s", run_id)
        elif return_code == 4:
            # 业务节点未完成（非代理池故障）：独立生命周期，禁止伪装成 proxy_failed
            set_proxy_status(
                STATUS_RETRY_PENDING,
                run_id=run_id,
                pool_ready=True,
                reason="部分节点/商品请求失败，断点已保留；再次开始将只重试失败项",
                error_code="CRAWL_RETRY_PENDING",
                return_code=return_code,
            )
        else:
            _proxy_sleep("crawler_failed")
            set_proxy_status(
                STATUS_PROXY_FAILED,
                run_id=run_id,
                pool_ready=False,
                reason=f"抓取进程异常退出（exit={return_code}）",
                error_code="CRAWLER_EXIT_FAILED",
                return_code=return_code,
            )


def _recover_proxy_and_resume(
    *,
    generation: int,
    request_id: str,
    previous_run_id: str,
    cmd: list[str] | None,
    env: dict | None,
    recovery_attempt: int,
):
    """运行时池长时间跌破门槛（ForcedProxyPool 自身有界等待仍未恢复）后的
    最后一道防线：最多重建一次，并由断点自动续跑。

    正常情况下常驻守护进程会持续自愈活池，抓取进程几乎不会走到这里；
    一旦发生，说明守护进程本身可能已经失效，因此走 force=False 委托路径
    （会重新确保守护进程存活），而不是与守护进程抢占 Mihomo 的完整冷启动。
    """
    global _product_proc
    next_attempt = recovery_attempt + 1
    if not cmd or next_attempt > PROXY_RUNTIME_RECOVERY_ATTEMPTS:
        _proxy_sleep("runtime_pool_recovery_exhausted")
        set_proxy_status(
            STATUS_PROXY_FAILED,
            run_id=previous_run_id,
            pool_ready=False,
            reason=f"运行时可用代理低于 {PROXY_MIN_START_NODES}，自动重建次数已用尽；断点已保留",
            error_code="POOL_RECOVERY_EXHAUSTED",
            recovery_attempt=recovery_attempt,
        )
        return

    if not _proxy_prepare_lock.acquire(blocking=False):
        set_proxy_status(
            STATUS_PROXY_FAILED,
            run_id=previous_run_id,
            pool_ready=False,
            reason="运行时代理恢复与其他代理准备冲突；断点已保留",
            error_code="POOL_RECOVERY_LOCK_BUSY",
        )
        return
    try:
        with _product_lock:
            if generation != _crawl_generation:
                return
        set_proxy_status(
            STATUS_PREPARING,
            run_id=previous_run_id,
            request_id=request_id,
            phase="runtime_recovery",
            recovery_attempt=next_attempt,
            recovery_limit=PROXY_RUNTIME_RECOVERY_ATTEMPTS,
        )
        logging.warning(
            "[proxy] runtime pool below minimum; rebuilding request_id=%s run_id=%s attempt=%d/%d",
            request_id, previous_run_id, next_attempt, PROXY_RUNTIME_RECOVERY_ATTEMPTS,
        )
        prep = ensure_proxy_ready(force=False)
        if not prep.ok:
            # 仍保留原抓取 run_id，避免恢复失败后 product_stats 切到新代理 prepare ID
            set_proxy_status(
                STATUS_PROXY_FAILED,
                run_id=previous_run_id,
                request_id=request_id,
                proxy_prepare_run_id=prep.run_id,
                pool_ready=False,
                reason=prep.reason or prep.error_code,
                error_code=prep.error_code or "POOL_RECOVERY_FAILED",
                recovery_attempt=next_attempt,
            )
            return

        # 同一轮续跑：爬虫 AMZ_RUN_ID / 生命周期 run_id 必须保持 previous_run_id
        resume_env = dict(env or os.environ.copy())
        if previous_run_id:
            resume_env["AMZ_RUN_ID"] = previous_run_id
        if use_run_cache() and previous_run_id:
            try:
                run_cache.open_existing_generation(previous_run_id)
            except Exception as exc:
                set_proxy_status(
                    STATUS_PROXY_FAILED,
                    run_id=previous_run_id,
                    request_id=request_id,
                    pool_ready=False,
                    reason=str(exc),
                    error_code="CACHE_RESUME_MISMATCH",
                    recovery_attempt=next_attempt,
                )
                return

        with _product_lock:
            if generation != _crawl_generation:
                return
            try:
                _product_proc = subprocess.Popen(
                    cmd, cwd=BASE_DIR, env=resume_env,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0,
                )
            except Exception as exc:
                _product_proc = None
                _proxy_sleep("crawler_resume_failed")
                set_proxy_status(
                    STATUS_PROXY_FAILED,
                    run_id=previous_run_id,
                    request_id=request_id,
                    proxy_prepare_run_id=prep.run_id,
                    pool_ready=False,
                    reason=str(exc),
                    error_code="CRAWLER_RESUME_FAILED",
                    recovery_attempt=next_attempt,
                )
                return
            resumed_proc = _product_proc

        resumed_at = time.monotonic()
        set_proxy_status(
            STATUS_RUNNING,
            run_id=previous_run_id,
            request_id=request_id,
            proxy_prepare_run_id=prep.run_id,
            pid=resumed_proc.pid,
            resumed_from_checkpoint=True,
            recovery_attempt=next_attempt,
        )
        logging.info(
            "[crawl] resumed from checkpoint request_id=%s run_id=%s proxy_prepare_run_id=%s pid=%d recovery_attempt=%d",
            request_id, previous_run_id, prep.run_id, resumed_proc.pid, next_attempt,
        )
        threading.Thread(
            target=_watch_and_sleep_proxy,
            args=(
                resumed_proc, generation, request_id, previous_run_id,
                resumed_at, cmd, resume_env, next_attempt,
            ),
            daemon=True,
        ).start()
    finally:
        _proxy_prepare_lock.release()

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
    ("social_proof_min", "月销量最小"),
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
    "social_proof_min",
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
    assert_testing_paths_safe()
    if use_run_cache():
        await asyncio.to_thread(run_cache.ensure_schema)
    if DB_BACKEND == "pg":
        _pool = await asyncpg.create_pool(get_pg_dsn(), min_size=2, max_size=10)
        try:
            async with _pool.acquire() as conn:
                for stmt in fav_store.PG_SCHEMA_SQL.strip().split(";"):
                    s = stmt.strip()
                    if s:
                        await conn.execute(s)
        except Exception:
            logging.exception("[favorites] ensure pg schema failed — 拒绝启动")
            if _pool:
                await _pool.close()
                _pool = None
            raise
    else:
        await asyncio.to_thread(fav_store.ensure_sqlite_schema)
    # 只要 API 在运行，常驻验证守护进程也应在运行（"始终热"策略）：
    # 拉起不阻塞——不等待它验证出任何节点，只保证进程已存在。
    if is_testing():
        logging.info("[proxy] TESTING=1：跳过代理守护进程启动")
    else:
        spawn = await asyncio.to_thread(ensure_daemon_running)
        if spawn.get("ok"):
            logging.info(
                "[proxy] 守护进程已就位 pid=%s started=%s",
                spawn.get("pid"), spawn.get("started"),
            )
        else:
            logging.warning("[proxy] 守护进程拉起失败: %s", spawn.get("error"))
    try:
        yield
    finally:
        # lifespan 内部异常、取消和正常 Ctrl+C 都必须经过同一清理路径。
        if _pool:
            await _pool.close()
        with _product_lock:
            global _product_proc
            if _product_proc and _product_proc.poll() is None:
                _product_proc.terminate()
                _product_proc = None
        if not is_testing() and os.getenv("PROXY_DAEMON_PERSIST", "0") != "1":
            stopped = await asyncio.to_thread(stop_daemon)
            if not stopped.get("ok"):
                logging.warning("[proxy] API 关闭时代理守护清理失败: %s", stopped)

app = FastAPI(title="Amazon 选品看板 API", lifespan=lifespan)

# CORS：默认仅本机；局域网访问时可通过 CORS_ORIGINS 追加，例如
#   set CORS_ORIGINS=http://192.168.1.10:8081,http://localhost:8081
_DEFAULT_CORS = [
    "http://127.0.0.1:8081",
    "http://localhost:8081",
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
    ("is_amazon_choice", "INTEGER DEFAULT 0"),
    ("is_bestseller", "INTEGER DEFAULT 0"),
    ("run_id", "TEXT"),
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
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_ps_social_proof ON product_sightings(social_proof_count)"
        )
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


def _parse_category_depth(value):
    """Parse a non-negative integer depth without accepting bool/float coercion."""
    if isinstance(value, bool) or value is None:
        return None
    text = str(value).strip()
    if not text.isdigit():
        return None
    depth = int(text)
    return depth if 0 <= depth <= 100 else None


async def _category_depth_values(site: str, start_depth: int, include_descendants: bool) -> list[int]:
    """Return existing depths for the selected site; L0/root and leaf levels are retained."""
    site = site.upper()
    op = ">=" if include_descendants else "="
    if DB_BACKEND == "pg":
        rows = await pg_query(
            f"SELECT DISTINCT depth FROM categories WHERE site = $1 AND depth {op} $2 ORDER BY depth",
            site, start_depth,
        )
    else:
        rows = await _sqlite_query(
            f"SELECT DISTINCT depth FROM categories WHERE site = ? AND depth {op} ? ORDER BY depth",
            (site, start_depth),
        )
    return [int(row["depth"]) for row in rows if row.get("depth") is not None]


@app.get("/api/v2/category_depths")
async def category_depths(site: str = "US"):
    """List every stored category level, including roots (L0) and terminal levels."""
    site = site.upper()
    if DB_BACKEND == "pg":
        rows = await pg_query(
            """SELECT depth, COUNT(DISTINCT node_id) AS category_count,
                      COUNT(DISTINCT node_id) FILTER (WHERE na_valid = 1) AS na_count
               FROM categories
               WHERE site = $1 AND depth >= 0 AND node_id IS NOT NULL AND node_id != ''
               GROUP BY depth ORDER BY depth""",
            site,
        )
    else:
        rows = await _sqlite_query(
            """SELECT depth, COUNT(DISTINCT node_id) AS category_count,
                      COUNT(DISTINCT CASE WHEN na_valid = 1 THEN node_id END) AS na_count
               FROM categories
               WHERE site = ? AND depth >= 0 AND node_id IS NOT NULL AND node_id != ''
               GROUP BY depth ORDER BY depth""",
            (site,),
        )
    return [
        {
            "depth": int(row["depth"]),
            "count": int(row["category_count"] or 0),
            "na_count": int(row["na_count"] or 0),
        }
        for row in rows
    ]


@app.post("/api/v2/category_scope_count")
async def category_scope_count(body: dict):
    """Return the deduplicated category count the crawler will actually execute."""
    site = str(body.get("site") or "US").upper()
    roots = list(dict.fromkeys(
        str(v).strip() for v in (body.get("roots") or [])
        if str(v).strip() and str(v).strip() != "__ALL__"
    ))
    all_categories = body.get("all_categories", False)
    if isinstance(all_categories, str):
        all_categories = all_categories.strip().lower() not in ("0", "false", "no", "off", "")
    else:
        all_categories = bool(all_categories)
    include_descendants = body.get("include_descendants", True)
    if isinstance(include_descendants, str):
        include_descendants = include_descendants.strip().lower() not in ("0", "false", "no", "off")
    else:
        include_descendants = bool(include_descendants)

    chart = str(body.get("chart") or "").lower()
    # 最新到货只抓 NEW 标记（na_valid=1）；榜单仍按所选类目树统计
    na_only = chart == "la" or bool(body.get("na_only"))

    scope_mode = str(body.get("scope_mode") or "tree").strip().lower()
    if scope_mode == "depth":
        depth = _parse_category_depth(body.get("depth"))
        if depth is None:
            return {
                "count": 0, "selected_count": 0, "scope_mode": "depth",
                "error": "depth must be a non-negative integer",
            }
        op = ">=" if include_descendants else "="
        valid_clause = " AND na_valid = 1" if na_only else ""
        if DB_BACKEND == "pg":
            count = await pg_scalar(
                f"SELECT COUNT(DISTINCT node_id) FROM categories "
                f"WHERE site = $1 AND depth {op} $2 AND node_id IS NOT NULL AND node_id != ''{valid_clause}",
                site, depth,
            )
            selected_count = await pg_scalar(
                "SELECT COUNT(DISTINCT node_id) FROM categories "
                "WHERE site = $1 AND depth = $2 AND node_id IS NOT NULL AND node_id != ''",
                site, depth,
            )
        else:
            count = await _sqlite_scalar(
                f"SELECT COUNT(DISTINCT node_id) FROM categories "
                f"WHERE site = ? AND depth {op} ? AND node_id IS NOT NULL AND node_id != ''{valid_clause}",
                (site, depth),
            )
            selected_count = await _sqlite_scalar(
                "SELECT COUNT(DISTINCT node_id) FROM categories "
                "WHERE site = ? AND depth = ? AND node_id IS NOT NULL AND node_id != ''",
                (site, depth),
            )
        return {
            "count": int(count or 0),
            "selected_count": int(selected_count or 0),
            "scope_mode": "depth",
            "depth": depth,
            "na_only": na_only,
            "include_descendants": include_descendants,
        }

    # 全选：最新到货 = 全部 NEW 类目；其它模式 = depth>0 全部类目
    if all_categories:
        if DB_BACKEND == "pg":
            if na_only:
                count = await pg_scalar(
                    "SELECT COUNT(DISTINCT node_id) FROM categories WHERE site = $1 AND na_valid = 1", site,
                )
            else:
                count = await pg_scalar(
                    "SELECT COUNT(DISTINCT node_id) FROM categories WHERE site = $1 AND depth > 0", site,
                )
        else:
            if na_only:
                count = await _sqlite_scalar(
                    "SELECT COUNT(DISTINCT node_id) FROM categories WHERE site = ? AND na_valid = 1", (site,),
                )
            else:
                count = await _sqlite_scalar(
                    "SELECT COUNT(DISTINCT node_id) FROM categories WHERE site = ? AND depth > 0", (site,),
                )
        return {
            "count": int(count or 0),
            "selected_count": 0,
            "all_categories": True,
            "na_only": na_only,
            "include_descendants": include_descendants,
        }

    if not roots:
        return {"count": 0, "selected_count": 0, "include_descendants": include_descendants}

    if DB_BACKEND == "pg":
        if include_descendants:
            if na_only:
                count = await pg_scalar(
                    """WITH RECURSIVE sub(node_id) AS (
                           SELECT node_id FROM categories WHERE site = $1 AND node_id = ANY($2::text[])
                           UNION
                           SELECT c.node_id FROM categories c JOIN sub s ON c.parent_node_id = s.node_id
                           WHERE c.site = $1
                       )
                       SELECT COUNT(DISTINCT c.node_id) FROM categories c
                       JOIN sub s ON s.node_id = c.node_id
                       WHERE c.site = $1 AND c.na_valid = 1""",
                    site, roots,
                )
            else:
                count = await pg_scalar(
                    """WITH RECURSIVE sub(node_id) AS (
                           SELECT node_id FROM categories WHERE site = $1 AND node_id = ANY($2::text[])
                           UNION
                           SELECT c.node_id FROM categories c JOIN sub s ON c.parent_node_id = s.node_id
                           WHERE c.site = $1
                       ) SELECT COUNT(*) FROM sub""",
                    site, roots,
                )
        else:
            if na_only:
                count = await pg_scalar(
                    "SELECT COUNT(DISTINCT node_id) FROM categories "
                    "WHERE site = $1 AND node_id = ANY($2::text[]) AND na_valid = 1",
                    site, roots,
                )
            else:
                count = await pg_scalar(
                    "SELECT COUNT(DISTINCT node_id) FROM categories WHERE site = $1 AND node_id = ANY($2::text[])",
                    site, roots,
                )
    else:
        placeholders = ",".join("?" for _ in roots)
        if include_descendants:
            if na_only:
                count = await _sqlite_scalar(
                    f"""WITH RECURSIVE sub(node_id) AS (
                            SELECT node_id FROM categories WHERE site = ? AND node_id IN ({placeholders})
                            UNION
                            SELECT c.node_id FROM categories c INDEXED BY idx_categories_parent_site
                            JOIN sub s ON c.parent_node_id = s.node_id
                            WHERE c.site = ?
                        )
                        SELECT COUNT(DISTINCT c.node_id) FROM categories c
                        JOIN sub s ON s.node_id = c.node_id
                        WHERE c.site = ? AND c.na_valid = 1""",
                    (site, *roots, site, site),
                )
            else:
                count = await _sqlite_scalar(
                    f"""WITH RECURSIVE sub(node_id) AS (
                            SELECT node_id FROM categories WHERE site = ? AND node_id IN ({placeholders})
                            UNION
                            SELECT c.node_id FROM categories c INDEXED BY idx_categories_parent_site
                            JOIN sub s ON c.parent_node_id = s.node_id
                            WHERE c.site = ?
                        ) SELECT COUNT(*) FROM sub""",
                    (site, *roots, site),
                )
        else:
            if na_only:
                count = await _sqlite_scalar(
                    f"SELECT COUNT(DISTINCT node_id) FROM categories "
                    f"WHERE site = ? AND node_id IN ({placeholders}) AND na_valid = 1",
                    (site, *roots),
                )
            else:
                count = await _sqlite_scalar(
                    f"SELECT COUNT(DISTINCT node_id) FROM categories WHERE site = ? AND node_id IN ({placeholders})",
                    (site, *roots),
                )

    return {
        "count": int(count or 0),
        "selected_count": len(roots),
        "na_only": na_only,
        "include_descendants": include_descendants,
    }

async def _tree_children_pg(parent, q, limit, offset, site="US", na_only=0):
    q = (q or "").strip()
    if na_only:
        sql = """SELECT c.name, c.node_id, c.depth, c.slug, c.na_valid,
                 (SELECT COUNT(DISTINCT d.node_id) FROM categories d
                  WHERE d.site = c.site AND d.na_valid = 1
                    AND d.path <@ c.path AND d.id != c.id) AS child_count
                 FROM categories c
                 WHERE c.site = $1
                   AND (c.na_valid = 1 OR EXISTS (
                       SELECT 1 FROM categories d
                       WHERE d.site = c.site AND d.na_valid = 1
                         AND d.path <@ c.path AND d.id != c.id
                   ))"""
        args = [site]
        if parent == "root":
            if q:
                # 根层带搜索：跨层按名称匹配 NEW 导航链上的节点
                sql += f" AND c.name ILIKE ${len(args)+1}"
                args.append(f"%{q}%")
            else:
                sql += " AND c.depth = 0"
        else:
            sql += " AND c.parent_node_id = $2"
            args.append(parent)
            if q:
                sql += f" AND c.name ILIKE ${len(args)+1}"
                args.append(f"%{q}%")
        sql += f" ORDER BY c.name LIMIT ${len(args)+1} OFFSET ${len(args)+2}"
        args.extend([limit, offset])
        return await pg_query(sql, *args)
    if parent == "root":
        if q:
            sql = """SELECT c.name, c.node_id, c.depth, c.slug, c.na_valid,
                     (SELECT COUNT(DISTINCT c2.node_id) FROM categories c2
                      WHERE c2.path <@ c.path AND c2.id != c.id) AS child_count
                     FROM categories c
                     WHERE c.site = $1 AND c.node_id IS NOT NULL AND c.node_id != ''
                       AND c.name ILIKE $2
                     ORDER BY c.depth, c.name
                     LIMIT $3 OFFSET $4"""
            return await pg_query(sql, site, f"%{q}%", limit, offset)
        total = await pg_scalar("SELECT COUNT(DISTINCT node_id) FROM categories WHERE site = $1", site)
        root_rows = await pg_query("SELECT node_id, name FROM categories WHERE depth = 0 AND site = $1", site)
        if root_rows:
            return [{"name": r["name"], "node_id": r["node_id"], "depth": 0, "child_count": total} for r in root_rows]
        return [{"name": "All Categories", "node_id": "_root_", "depth": 0, "child_count": total}]
    elif (await pg_scalar("SELECT COUNT(*) FROM categories WHERE node_id=$1 AND depth=0 AND site=$2", parent, site)) > 0:
        sql = """SELECT c.name, c.node_id, c.depth, c.slug, c.na_valid,
                 (SELECT COUNT(DISTINCT c2.node_id) FROM categories c2 WHERE c2.path <@ c.path AND c2.id != c.id) as child_count
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
                 (SELECT COUNT(DISTINCT c2.node_id) FROM categories c2 WHERE c2.path <@ c.path AND c2.id != c.id) as child_count
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
    # 最新到货模式：保留 NEW 节点及其祖先导航链，仍按 parent_node_id 逐层展示。
    q = (q or "").strip()
    if na_only:
        sql = """WITH RECURSIVE ancestry(new_node_id, ancestor_id) AS (
                     SELECT node_id, parent_node_id
                     FROM categories WHERE site = ? AND na_valid = 1
                     UNION
                     SELECT a.new_node_id, c.parent_node_id
                     FROM ancestry a JOIN categories c INDEXED BY idx_categories_node_site
                       ON c.node_id = a.ancestor_id
                     WHERE c.site = ? AND a.ancestor_id IS NOT NULL
                 ),
                 relevant(node_id) AS (
                     SELECT node_id FROM categories WHERE site = ? AND na_valid = 1
                     UNION
                     SELECT ancestor_id FROM ancestry WHERE ancestor_id IS NOT NULL
                 ),
                 new_counts(node_id, child_count) AS (
                     SELECT ancestor_id, COUNT(DISTINCT new_node_id)
                     FROM ancestry WHERE ancestor_id IS NOT NULL
                     GROUP BY ancestor_id
                 )
                 SELECT c.name, c.node_id, c.depth, c.slug, c.na_valid,
                    COALESCE(n.child_count, 0) AS child_count
                 FROM categories c JOIN relevant r ON r.node_id = c.node_id
                 LEFT JOIN new_counts n ON n.node_id = c.node_id
                 WHERE c.site = ?"""
        params = [site, site, site, site]
        if parent == "root":
            if q:
                sql += " AND c.name LIKE ?"
                params.append(f"%{q}%")
            else:
                sql += " AND c.depth = 0"
        else:
            sql += " AND c.parent_node_id = ?"
            params.append(parent)
            if q:
                sql += " AND c.name LIKE ?"
                params.append(f"%{q}%")
        sql += f" ORDER BY c.name LIMIT {limit} OFFSET {offset}"
        return await _sqlite_query(sql, params)

    if parent == "root":
        # 根层带搜索词：跨深度按名称匹配，不再忽略 q
        if q:
            sql = """SELECT c.name, c.node_id, c.depth, c.slug, c.na_valid,
                     (WITH RECURSIVE sub AS (
                         SELECT node_id FROM categories WHERE parent_node_id = c.node_id AND site = c.site
                         UNION
                         SELECT cat.node_id FROM categories cat INDEXED BY idx_categories_parent_site
                         JOIN sub s ON cat.parent_node_id = s.node_id
                         WHERE cat.site = c.site
                     ) SELECT COUNT(*) FROM sub) as child_count
                     FROM categories c
                     WHERE c.site = ? AND c.node_id IS NOT NULL AND c.node_id != ''
                       AND c.name LIKE ?
                     ORDER BY c.depth, c.name
                     LIMIT ? OFFSET ?"""
            return await _sqlite_query(sql, [site, f"%{q}%", limit, offset])
        # 递归统计某根 node_id 下全部后代（通过 parent_node_id 链）
        async def _descendant_count(root_node_id):
            return await _sqlite_scalar(
                """WITH RECURSIVE sub AS (
                       SELECT node_id FROM categories WHERE parent_node_id = ? AND site = ?
                       UNION
                       SELECT c.node_id FROM categories c INDEXED BY idx_categories_parent_site
                       JOIN sub s ON c.parent_node_id = s.node_id WHERE c.site = ?
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
        total = await _sqlite_scalar("SELECT COUNT(DISTINCT node_id) FROM categories WHERE site = ?", (site,))
        return [{"name": "All Categories", "node_id": "_root_", "depth": 0, "child_count": total}]
    else:
        sql = """SELECT c.name, c.node_id, c.depth, c.slug, c.na_valid,
                 (WITH RECURSIVE sub AS (
                     SELECT node_id FROM categories WHERE parent_node_id = c.node_id AND site = c.site
                     UNION
                     SELECT cat.node_id FROM categories cat INDEXED BY idx_categories_parent_site
                     JOIN sub s ON cat.parent_node_id = s.node_id
                     WHERE cat.site = c.site
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
    social_proof_min: int = None,
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
        social_proof_min=social_proof_min,
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
    if use_run_cache():
        rows = await asyncio.to_thread(run_cache.query_products, filters, chart="products")
        return await _attach_favorite_flags(rows, filters.get("site"))
    if DB_BACKEND == "pg":
        return await _attach_favorite_flags(await _products_pg(**filters), filters.get("site"))
    return await _attach_favorite_flags(await _products_sqlite(**filters), filters.get("site"))


async def _attach_favorite_flags(rows: list, site: str | None = None):
    """附加收藏标记；收藏读取失败不得伪装成全部未收藏。"""
    if not rows:
        return rows
    try:
        keys = await asyncio.to_thread(fav_store.favorite_key_set, DB_BACKEND, site)
    except Exception as e:
        logging.exception("favorite_key_set failed")
        return JSONResponse(
            {
                "status": "error",
                "error_code": "FAVORITE_READ_FAILED",
                "msg": f"收藏状态读取失败: {e}",
            },
            status_code=503,
        )
    for r in rows:
        s = (r.get("site") or site or "").upper()
        a = (r.get("asin") or "").upper()
        r["is_favorite"] = (s, a) in keys
        r["run_id"] = r.get("run_id") or run_cache.get_active_run_id()
    return rows


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
        ("social_proof_min", ">=", "social_proof_count"),
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
             variant_option_count, other_sellers_count, social_proof, social_proof_count,
             item_weight, item_dimensions,
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
             variant_option_count, other_sellers_count, social_proof, social_proof_count,
             item_weight, item_dimensions,
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
        ("social_proof_min", ">=", "social_proof_count"),
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
              social_proof, social_proof_count,
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
    social_proof_min: int = None,
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
        social_proof_min=social_proof_min,
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
    if use_run_cache():
        # LA 结果通常一次写全；不过滤 detail_scraped=1，避免空列表
        la_filters = {**filters, "detail_only": False}
        rows = await asyncio.to_thread(run_cache.query_products, la_filters, chart="la")
        return await _attach_favorite_flags(rows, filters.get("site"))
    if DB_BACKEND == "pg":
        return await _attach_favorite_flags(await _new_arrivals_pg(filters), filters.get("site"))
    return await _attach_favorite_flags(await _new_arrivals_sqlite(filters), filters.get("site"))

@app.get("/api/v2/product_stats")
async def product_stats(
    site: str = Query(None, description="Marketplace site, e.g. US"),
    run_id: str = Query(None, description="可选；默认用当前生命周期 run_id，隔离本次详情成败"),
):
    """大盘统计；site 隔离站点；detail_ok/detail_failed 按 run_id 隔离本次运行。"""
    running, lifecycle, lifecycle_run_id = _crawl_lifecycle_state()
    site = (site or "").strip().upper() or None
    stats_run_id = (run_id or "").strip() or (lifecycle_run_id or "").strip() or None
    total, by_list, multi, na_total = 0, [], 0, 0
    detail_ok, detail_failed = 0, 0
    try:
        if use_run_cache():
            cache_stats = await asyncio.to_thread(
                run_cache.stats, site, stats_run_id or run_cache.get_active_run_id()
            )
            return {
                "total_asins": cache_stats.get("total_asins") or 0,
                "new_arrivals": cache_stats.get("new_arrivals") or 0,
                "by_list": cache_stats.get("by_list") or [],
                "multi_list": cache_stats.get("multi_list") or 0,
                "detail_ok": cache_stats.get("detail_ok") or 0,
                "detail_failed": cache_stats.get("detail_failed") or 0,
                "site": site,
                "running": running,
                "lifecycle": lifecycle,
                "run_id": cache_stats.get("run_id") or stats_run_id or lifecycle_run_id or "",
                "result_mode": "run_cache",
            }
        if DB_BACKEND == "pg":
            if site:
                total = await pg_scalar(
                    "SELECT COUNT(DISTINCT asin) FROM product_sightings WHERE site=$1", site
                )
                by_list = await pg_query(
                    "SELECT list_type, COUNT(*) as cnt FROM product_sightings WHERE site=$1 GROUP BY list_type",
                    site,
                )
                multi = await pg_scalar(
                    "SELECT COUNT(*) FROM (SELECT asin FROM product_sightings WHERE site=$1 "
                    "GROUP BY asin HAVING COUNT(DISTINCT list_type)>1) t",
                    site,
                )
                if stats_run_id:
                    detail_ok = await pg_scalar(
                        "SELECT COUNT(DISTINCT asin) FROM product_sightings "
                        "WHERE site=$1 AND run_id=$2 AND detail_scraped=1",
                        site, stats_run_id,
                    ) or 0
                    detail_failed = await pg_scalar(
                        "SELECT COUNT(DISTINCT asin) FROM product_sightings "
                        "WHERE site=$1 AND run_id=$2 AND detail_scraped=2",
                        site, stats_run_id,
                    ) or 0
                try:
                    na_total = await pg_scalar(
                        "SELECT COUNT(DISTINCT asin) FROM new_arrivals WHERE site=$1", site
                    )
                except Exception as e:
                    logging.warning(f"new_arrivals pg stats failed: {e}")
            else:
                total = await pg_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
                by_list = await pg_query(
                    "SELECT list_type, COUNT(*) as cnt FROM product_sightings GROUP BY list_type"
                )
                multi = await pg_scalar(
                    "SELECT COUNT(*) FROM (SELECT asin FROM product_sightings "
                    "GROUP BY asin HAVING COUNT(DISTINCT list_type)>1) t"
                )
                if stats_run_id:
                    detail_ok = await pg_scalar(
                        "SELECT COUNT(DISTINCT asin) FROM product_sightings "
                        "WHERE run_id=$1 AND detail_scraped=1",
                        stats_run_id,
                    ) or 0
                    detail_failed = await pg_scalar(
                        "SELECT COUNT(DISTINCT asin) FROM product_sightings "
                        "WHERE run_id=$1 AND detail_scraped=2",
                        stats_run_id,
                    ) or 0
                try:
                    na_total = await pg_scalar("SELECT COUNT(DISTINCT asin) FROM new_arrivals")
                except Exception as e:
                    logging.warning(f"new_arrivals pg stats failed: {e}")
        else:
            try:
                if site:
                    total = await _sqlite_scalar(
                        "SELECT COUNT(DISTINCT asin) FROM product_sightings WHERE site=?", (site,)
                    )
                    by_list = await _sqlite_query(
                        "SELECT list_type, COUNT(*) as cnt FROM product_sightings "
                        "WHERE site=? GROUP BY list_type",
                        (site,),
                    )
                    multi = await _sqlite_scalar(
                        "SELECT COUNT(*) FROM (SELECT asin FROM product_sightings WHERE site=? "
                        "GROUP BY asin HAVING COUNT(DISTINCT list_type)>1)",
                        (site,),
                    )
                    if stats_run_id:
                        detail_ok = await _sqlite_scalar(
                            "SELECT COUNT(DISTINCT asin) FROM product_sightings "
                            "WHERE site=? AND run_id=? AND detail_scraped=1",
                            (site, stats_run_id),
                        ) or 0
                        detail_failed = await _sqlite_scalar(
                            "SELECT COUNT(DISTINCT asin) FROM product_sightings "
                            "WHERE site=? AND run_id=? AND detail_scraped=2",
                            (site, stats_run_id),
                        ) or 0
                    try:
                        na_total = await _sqlite_scalar(
                            "SELECT COUNT(DISTINCT asin) FROM new_arrivals WHERE site=?", (site,)
                        )
                    except Exception as e:
                        logging.warning(f"new_arrivals stats failed: {e}")
                else:
                    total = await _sqlite_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
                    by_list = await _sqlite_query(
                        "SELECT list_type, COUNT(*) as cnt FROM product_sightings GROUP BY list_type"
                    )
                    multi = await _sqlite_scalar(
                        "SELECT COUNT(*) FROM (SELECT asin FROM product_sightings "
                        "GROUP BY asin HAVING COUNT(DISTINCT list_type)>1)"
                    )
                    if stats_run_id:
                        detail_ok = await _sqlite_scalar(
                            "SELECT COUNT(DISTINCT asin) FROM product_sightings "
                            "WHERE run_id=? AND detail_scraped=1",
                            (stats_run_id,),
                        ) or 0
                        detail_failed = await _sqlite_scalar(
                            "SELECT COUNT(DISTINCT asin) FROM product_sightings "
                            "WHERE run_id=? AND detail_scraped=2",
                            (stats_run_id,),
                        ) or 0
                    try:
                        na_total = await _sqlite_scalar(
                            "SELECT COUNT(DISTINCT asin) FROM new_arrivals"
                        )
                    except Exception as e:
                        logging.warning(f"new_arrivals stats failed: {e}")
            except Exception as e:
                logging.warning(f"product_sightings stats failed: {e}")
    except Exception as e:
        logging.warning(f"product_stats failed: {e}")
    return {
        "total_asins": total,
        "new_arrivals": na_total,
        "by_list": by_list or [],
        "multi_list": multi,
        "detail_ok": detail_ok,
        "detail_failed": detail_failed,
        "site": site,
        "running": running,
        "lifecycle": lifecycle,
        "run_id": stats_run_id or lifecycle_run_id or "",
    }

@app.get("/api/v2/product_progress")
async def product_progress():
    running, lifecycle, run_id = _crawl_lifecycle_state()
    ps, na = 0, 0
    try:
        if use_run_cache():
            cache_stats = await asyncio.to_thread(run_cache.stats, None, run_id or None)
            ps = cache_stats.get("total_asins") or 0
            na = cache_stats.get("new_arrivals") or 0
            run_id = cache_stats.get("run_id") or run_id
        elif DB_BACKEND == "pg":
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
    return {
        "running": running,
        "lifecycle": lifecycle,
        "run_id": run_id,
        "total_products": ps + na,
        "product_sightings": ps,
        "new_arrivals": na,
    }


# 爬虫 FileHandler 日志（与 fetch_*.py 写入路径一致）
_CRAWL_LOG_FILES = {
    "la": os.path.join(BASE_DIR, "data", "fetch_new_arrivals.log"),
    "nr": os.path.join(BASE_DIR, "data", "fetch_products.log"),
    "bs": os.path.join(BASE_DIR, "data", "fetch_products.log"),
    "mw": os.path.join(BASE_DIR, "data", "fetch_products.log"),
    "mg": os.path.join(BASE_DIR, "data", "fetch_products.log"),
}


def _read_log_tail(path: str, *, max_lines: int = 200, since_pos: int = 0) -> dict:
    """高效读取日志尾部；since_pos>0 时做增量追加读取。"""
    max_lines = max(20, min(int(max_lines or 200), 2000))
    since_pos = max(0, int(since_pos or 0))
    if not path or not os.path.isfile(path):
        return {
            "exists": False, "lines": [], "pos": 0, "size": 0,
            "mtime": None, "truncated": False,
        }
    size = os.path.getsize(path)
    mtime = os.path.getmtime(path)
    # 文件被截断/轮转：从头重读尾部
    if since_pos > size:
        since_pos = 0

    truncated = False
    with open(path, "rb") as fh:
        if since_pos > 0:
            # since_pos 来自上一次返回的文件末尾，视为行边界；直接读新增字节
            fh.seek(since_pos)
            new_text = fh.read().decode("utf-8", errors="replace")
            lines = [ln for ln in new_text.splitlines() if ln.strip()]
            if len(lines) > max_lines:
                lines = lines[-max_lines:]
                truncated = True
            return {
                "exists": True, "lines": lines, "pos": size, "size": size,
                "mtime": mtime, "truncated": truncated, "incremental": True,
            }

        # 全量尾读：从文件末尾向前扫，最多读约 512KB
        read_bytes = min(size, 512 * 1024)
        fh.seek(max(0, size - read_bytes))
        data = fh.read().decode("utf-8", errors="replace")
        if size > read_bytes:
            # 丢掉半行
            nl = data.find("\n")
            if nl >= 0:
                data = data[nl + 1:]
            truncated = True
        lines = [ln for ln in data.splitlines() if ln.strip()]
        if len(lines) > max_lines:
            lines = lines[-max_lines:]
            truncated = True
        return {
            "exists": True, "lines": lines, "pos": size, "size": size,
            "mtime": mtime, "truncated": truncated, "incremental": False,
        }


@app.get("/api/v2/crawl_logs")
async def crawl_logs(
    chart: str = "nr",
    lines: int = 200,
    since_pos: int = 0,
):
    """读取当前模式对应的爬虫行动日志（FileHandler 文件尾部）。"""
    key = (chart or "nr").lower()
    path = _CRAWL_LOG_FILES.get(key) or _CRAWL_LOG_FILES["nr"]
    payload = await asyncio.to_thread(
        _read_log_tail, path, max_lines=lines, since_pos=since_pos,
    )
    running, lifecycle, run_id = _crawl_lifecycle_state()
    proxy = get_proxy_status()
    return {
        "chart": key,
        "source": os.path.basename(path),
        "path": path,
        "running": running,
        "lifecycle": lifecycle,
        "run_id": run_id,
        "proxy_status": proxy.get("status"),
        "proxy_phase": (proxy.get("detail") or {}).get("phase"),
        **payload,
    }

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
    ("social_proof_min", "--social-proof-min"),
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


@app.get("/api/v2/proxy_status")
async def proxy_status():
    """合并两个视角：抓取生命周期状态（get_proxy_status）与常驻验证守护进程
    的实时活池计数（daemon_status）。守护进程持续、增量地验证/增补/淘汰节点，
    前端据此展示持续变化的进度，而不是一次性的"preparing_proxy"进度条。"""
    status = get_proxy_status()
    pid, daemon = await asyncio.to_thread(lambda: (daemon_alive(), daemon_status()))
    status["daemon"] = {"alive": bool(pid), "pid": pid, **daemon}
    return status


@app.post("/api/v2/proxy_quality_check")
async def proxy_quality_check():
    """按需体检：对当前活池（默认热+温池）逐个发起一次真实 Amazon 请求，
    量化延迟/成功率/验证码率。供前端弹窗展示，不修改 daemon 的池内状态。
    这是同步阻塞的网络探测（每节点最多约 timeout 秒），放到线程里跑避免
    阻塞事件循环；节点数不多（通常 <30），整体几秒到十几秒内返回。"""
    try:
        report = await asyncio.to_thread(check_pool_quality)
        return report
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/v2/start_products")
async def start_products(body: dict):
    global _product_proc, _crawl_generation
    request_id = f"START-{time.strftime('%Y%m%d-%H%M%S')}-{threading.get_ident()}"
    chart = str(body.get("chart") or "").lower()
    scope_mode = str(body.get("scope_mode") or "tree").strip().lower()
    depth_scope = scope_mode == "depth"
    scope_depth = _parse_category_depth(body.get("depth")) if depth_scope else None
    include_descendants = body.get("include_descendants", True)
    if isinstance(include_descendants, str):
        include_descendants = include_descendants.strip().lower() not in ("0", "false", "no", "off")
    else:
        include_descendants = bool(include_descendants)
    with _product_lock:
        if _product_proc is not None and _product_proc.poll() is None:
            logging.info("[crawl] duplicate start rejected request_id=%s reason=already_running", request_id)
            return {"status": "already_running"}
        range_err = _validate_start_filters(body)
        if range_err:
            logging.warning("[crawl] start rejected request_id=%s reason=invalid_filters detail=%s", request_id, range_err)
            return {"status": "error", "msg": f"筛选条件不合法: {range_err}"}
        if depth_scope and scope_depth is None:
            logging.warning("[crawl] start rejected request_id=%s reason=invalid_depth", request_id)
            return {"status": "error", "msg": "层级必须是大于等于 0 的整数"}
        all_categories = body.get("all_categories", False)
        if isinstance(all_categories, str):
            all_categories = all_categories.strip().lower() not in ("0", "false", "no", "off", "")
        else:
            all_categories = bool(all_categories)
        roots = [
            str(v).strip() for v in (body.get("roots") or [])
            if str(v).strip() and str(v).strip() != "__ALL__"
        ]
        if chart != "la" and all_categories:
            logging.warning("[crawl] start rejected request_id=%s reason=all_categories_not_supported chart=%s", request_id, chart)
            return {"status": "error", "msg": "榜单抓取不支持全部分类，请勾选具体类目"}
        if chart == "la" and not roots and not all_categories and not depth_scope:
            logging.warning("[crawl] start rejected request_id=%s reason=no_roots chart=la", request_id)
            return {"status": "error", "msg": "latest arrivals requires roots"}
        if chart != "la" and not body.get("slugs") and not roots and not depth_scope:
            logging.warning("[crawl] start rejected request_id=%s reason=no_roots_or_slugs chart=%s", request_id, chart)
            return {"status": "error", "msg": "no slugs or roots specified"}

    depth_values = []
    if depth_scope:
        site = str(body.get("site") or "US").upper()
        depth_values = await _category_depth_values(site, scope_depth, include_descendants)
        scope_preview = await category_scope_count({
            **body,
            "site": site,
            "scope_mode": "depth",
            "depth": scope_depth,
            "include_descendants": include_descendants,
        })
        if not depth_values or int(scope_preview.get("count") or 0) <= 0:
            logging.warning(
                "[crawl] start rejected request_id=%s reason=empty_depth_scope site=%s depth=%s chart=%s",
                request_id, site, scope_depth, chart,
            )
            return {"status": "error", "msg": "该层级在当前站点和榜单模式下没有可抓取类目"}
        body = {
            **body,
            "roots": [],
            "all_categories": False,
            "scope_mode": "depth",
            "depth": scope_depth,
            "include_descendants": include_descendants,
        }

    roots = [
        str(v).strip() for v in (body.get("roots") or [])
        if str(v).strip() and str(v).strip() != "__ALL__"
    ]
    all_categories = body.get("all_categories", False)
    if isinstance(all_categories, str):
        all_categories = all_categories.strip().lower() not in ("0", "false", "no", "off", "")
    else:
        all_categories = bool(all_categories)
    # 全选时清空 roots，避免误传 __ALL__ 给爬虫
    if all_categories:
        roots = []
        body = {**body, "roots": [], "all_categories": True}
    slugs = body.get("slugs") or []
    logging.info(
        "[crawl] start requested request_id=%s chart=%s site=%s scope_mode=%s depth=%s roots=%d slugs=%d all_categories=%s include_descendants=%s max_pages=%s",
        request_id,
        chart,
        body.get("site") or "",
        scope_mode,
        scope_depth if depth_scope else "",
        len(roots),
        len(slugs),
        all_categories,
        body.get("include_descendants", True),
        body.get("max_pages"),
    )

    # 持有跨进程迁移锁直到生命周期进入 running/failed，避免“探测后释放”
    # 与清表复核之间出现 TOCTOU 窗口。
    migration_guard = None
    try:
        from migrate_clear_crawl_results import MigrationLock
        migration_guard = MigrationLock()
        if not await asyncio.to_thread(migration_guard.acquire, 0.0):
            logging.warning("[crawl] start rejected request_id=%s reason=migration_in_progress", request_id)
            return JSONResponse(
                {
                    "status": "error",
                    "error_code": "MIGRATION_IN_PROGRESS",
                    "msg": "正式库清表进行中，请稍后再启动抓取",
                },
                status_code=409,
            )
    except Exception as e:
        if migration_guard is not None:
            migration_guard.release()
        logging.exception("[crawl] migration lock acquire failed request_id=%s", request_id)
        return JSONResponse(
            {
                "status": "error",
                "error_code": "MIGRATION_LOCK_PROBE_FAILED",
                "msg": f"无法确认清表锁状态: {e}",
            },
            status_code=503,
        )

    # 代理准备：单飞锁，失败直接 proxy_failed，禁止带病启动
    if not _proxy_prepare_lock.acquire(blocking=False):
        if migration_guard is not None:
            migration_guard.release()
        logging.info("[crawl] duplicate start rejected request_id=%s reason=proxy_preparing", request_id)
        return {
            "status": STATUS_PREPARING,
            "msg": "代理池正在准备中，请勿重复点击",
        }

    # 仅成功取得单飞锁的请求可以预留新代际；重复点击不得使当前看门狗失效。
    with _product_lock:
        _crawl_generation += 1
        generation = _crawl_generation

    try:
        prepare_started_at = time.monotonic()
        set_proxy_status(STATUS_PREPARING, request_id=request_id)
        try:
            prep = await asyncio.to_thread(ensure_proxy_ready, force=False)
        except Exception as e:
            logging.exception("[crawl] proxy preparation crashed request_id=%s", request_id)
            set_proxy_status(STATUS_PROXY_FAILED, request_id=request_id, reason=str(e))
            return {
                "status": STATUS_PROXY_FAILED,
                "run_id": "",
                "candidate_nodes": 0,
                "verified_nodes": 0,
                "unique_ips": 0,
                "reason": str(e),
            }

        if not prep.ok:
            logging.warning(
                "[crawl] proxy preparation failed request_id=%s run_id=%s candidates=%d verified=%d unique_ips=%d reason=%s elapsed=%.1fs",
                request_id,
                prep.run_id,
                prep.candidate_nodes,
                prep.verified_nodes,
                prep.unique_ips,
                prep.reason or prep.error_code,
                time.monotonic() - prepare_started_at,
            )
            set_proxy_status(
                STATUS_PROXY_FAILED,
                run_id=prep.run_id,
                request_id=request_id,
                reason=prep.reason,
            )
            return {
                "status": STATUS_PROXY_FAILED,
                "run_id": prep.run_id,
                "candidate_nodes": prep.candidate_nodes,
                "verified_nodes": prep.verified_nodes,
                "unique_ips": prep.unique_ips,
                "reason": prep.reason or prep.error_code,
                "error_code": prep.error_code,
                "fail_reasons": prep.fail_reasons,
            }

        logging.info(
            "[crawl] proxy ready request_id=%s run_id=%s candidates=%d verified=%d unique_ips=%d elapsed=%.1fs",
            request_id,
            prep.run_id,
            prep.candidate_nodes,
            prep.verified_nodes,
            prep.unique_ips,
            time.monotonic() - prepare_started_at,
        )
        set_proxy_status(STATUS_PROXY_READY, run_id=prep.run_id, request_id=request_id)

        with _product_lock:
            if _product_proc is not None and _product_proc.poll() is None:
                return {"status": "already_running", "run_id": prep.run_id}

            set_proxy_status(STATUS_STARTING_CRAWLER, run_id=prep.run_id, request_id=request_id)
            if chart == "la":
                cmd = [sys.executable, "-u", os.path.join(BASE_DIR, "fetch_new_arrivals.py")]
                # all_categories / 空 roots：不传 --roots，爬虫抓站点全部类目
                la_roots = [
                    str(v).strip() for v in (body.get("roots") or [])
                    if str(v).strip() and str(v).strip() != "__ALL__"
                ]
                if depth_scope:
                    cmd += ["--depth"] + [str(value) for value in depth_values]
                elif la_roots and not body.get("all_categories"):
                    cmd += ["--roots"] + la_roots
                if body.get("site"):
                    cmd += ["--site", body["site"]]
                page_cap = min(_positive_int(body.get("max_pages"), 2), 999)
                cmd += ["--max-pages", str(page_cap)]
                if la_roots and not include_descendants and not depth_scope:
                    cmd += ["--exact-roots"]
                _append_filter_flags(cmd, body, for_la=True)
            else:
                cmd = [sys.executable, "-u", os.path.join(BASE_DIR, "fetch_products.py")]
                if depth_scope:
                    cmd += ["--depth"] + [str(value) for value in depth_values]
                elif body.get("slugs"):
                    cmd += ["--slugs"] + body["slugs"]
                else:
                    cmd += ["--roots"] + list(body.get("roots") or [])
                if body.get("lists"):
                    cmd += ["--lists"] + body["lists"]
                if body.get("site"):
                    cmd += ["--site", body["site"]]
                page_cap = min(_positive_int(body.get("max_pages"), 2), 2)
                cmd += ["--list-limit", "0", "--max-pages", str(page_cap)]
                if not include_descendants and not depth_scope:
                    cmd += ["--exact-roots"]
                _append_filter_flags(cmd, body, for_la=False)

            env = os.environ.copy()
            env["DB_BACKEND"] = DB_BACKEND
            env["PROXY_REQUIRED"] = "1"
            env["ALLOW_DIRECT_FALLBACK"] = "0"
            # 与 proxy 状态 / product_stats 共用 prep.run_id，避免 request_id 与 run_id 不一致
            env["AMZ_RUN_ID"] = prep.run_id
            from config import PRODUCT_RESULT_MODE, PRODUCT_RUN_CACHE_FILE
            env["PRODUCT_RESULT_MODE"] = PRODUCT_RESULT_MODE
            env["AMZ_RUN_CACHE_FILE"] = PRODUCT_RUN_CACHE_FILE
            cache_chart = "la" if chart == "la" else "products"
            if use_run_cache():
                # 先建 pending 代次；仅子进程创建成功后再 activate 并清旧缓存
                await asyncio.to_thread(run_cache.create_generation, prep.run_id, cache_chart)
            try:
                _product_proc = subprocess.Popen(
                    cmd, cwd=BASE_DIR, env=env,
                    creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0,
                )
            except Exception as exc:
                _product_proc = None
                logging.exception("[proxy] 抓取进程创建失败")
                # 清理 pending 代次 + 代理；子步骤失败也不丢掉稳定错误码
                abort = await _abort_crawler_start(None, prep.run_id)
                reason = str(exc)
                if abort.get("cleanup_errors"):
                    reason = f"{reason}; cleanup={';'.join(abort['cleanup_errors'])}"
                set_proxy_status(
                    STATUS_PROXY_FAILED,
                    run_id=prep.run_id,
                    request_id=request_id,
                    reason=reason,
                    error_code="CRAWLER_START_FAILED",
                    cleanup=abort.get("proxy_cleanup"),
                )
                return {
                    "status": STATUS_PROXY_FAILED,
                    "run_id": prep.run_id,
                    "candidate_nodes": prep.candidate_nodes,
                    "verified_nodes": prep.verified_nodes,
                    "unique_ips": prep.unique_ips,
                    "reason": reason,
                    "error_code": "CRAWLER_START_FAILED",
                    "cleanup_errors": abort.get("cleanup_errors") or [],
                }
            if use_run_cache():
                try:
                    await asyncio.to_thread(
                        run_cache.activate_generation, prep.run_id, cache_chart, purge_others=True
                    )
                except Exception as exc:
                    logging.exception("[cache] activate_generation failed run_id=%s", prep.run_id)
                    proc = _product_proc
                    _product_proc = None
                    abort = await _abort_crawler_start(proc, prep.run_id)
                    reason = str(exc)
                    if abort.get("cleanup_errors"):
                        reason = f"{reason}; cleanup={';'.join(abort['cleanup_errors'])}"
                    set_proxy_status(
                        STATUS_PROXY_FAILED,
                        run_id=prep.run_id,
                        request_id=request_id,
                        reason=reason,
                        error_code="CACHE_ACTIVATION_FAILED",
                        cleanup=abort.get("proxy_cleanup"),
                    )
                    return {
                        "status": STATUS_PROXY_FAILED,
                        "run_id": prep.run_id,
                        "candidate_nodes": prep.candidate_nodes,
                        "verified_nodes": prep.verified_nodes,
                        "unique_ips": prep.unique_ips,
                        "reason": reason,
                        "error_code": "CACHE_ACTIVATION_FAILED",
                        "result_mode": PRODUCT_RESULT_MODE,
                        "cleanup_errors": abort.get("cleanup_errors") or [],
                    }
            crawler_started_at = time.monotonic()
            threading.Thread(
                target=_watch_and_sleep_proxy,
                args=(_product_proc, generation, request_id, prep.run_id, crawler_started_at, cmd, env, 0),
                daemon=True,
            ).start()
            logging.info(
                "[crawl] process started request_id=%s run_id=%s pid=%d chart=%s",
                request_id,
                prep.run_id,
                _product_proc.pid,
                chart,
            )
            set_proxy_status(
                STATUS_RUNNING,
                run_id=prep.run_id,
                request_id=request_id,
                pid=_product_proc.pid,
            )
            return {
                "status": "started",
                "lifecycle": STATUS_RUNNING,
                "pid": _product_proc.pid,
                "backend": DB_BACKEND,
                "run_id": prep.run_id,
                "candidate_nodes": prep.candidate_nodes,
                "verified_nodes": prep.verified_nodes,
                "unique_ips": prep.unique_ips,
                "result_mode": PRODUCT_RESULT_MODE,
            }
    finally:
        _proxy_prepare_lock.release()
        if migration_guard is not None:
            migration_guard.release()

@app.post("/api/v2/stop_products")
async def stop_products():
    global _product_proc, _crawl_generation
    set_proxy_status(STATUS_STOPPING)
    # 等待正在进行的代理准备（ensure_proxy_ready）结束，避免和它交叉写状态
    # （阻塞操作放线程池，不卡事件循环）；常驻守护进程/独立 Mihomo 不受影响。
    acquired = await asyncio.to_thread(_proxy_prepare_lock.acquire, True, 120)
    if not acquired:
        set_proxy_status(
            STATUS_STOPPING,
            reason="等待代理准备结束超时，停止操作未执行",
            error_code="STOP_PREPARE_TIMEOUT",
        )
        return {
            "status": "stop_timeout",
            "msg": "代理池仍在准备，停止操作未执行",
            "error_code": "STOP_PREPARE_TIMEOUT",
        }
    try:
        with _product_lock:
            # 使当前世代失效：即便旧看门狗随后才唤醒，也不会误杀本次显式停止之后的新一轮
            _crawl_generation += 1
            if _product_proc is None or _product_proc.poll() is not None:
                proc_to_wait = None
            else:
                _product_proc.terminate()
                pid = _product_proc.pid
                proc_to_wait = _product_proc
                _product_proc = None

        if proc_to_wait is None:
            await asyncio.to_thread(stop_proxy_pool)
            set_proxy_status(STATUS_IDLE)
            return {"status": "not_running"}

        await asyncio.to_thread(proc_to_wait.wait)
        await asyncio.to_thread(stop_proxy_pool)
        set_proxy_status(STATUS_IDLE)
        return {"status": "stopped", "pid": pid}
    finally:
        if acquired:
            _proxy_prepare_lock.release()

@app.post("/api/v2/export_excel")
async def export_excel(body: dict = None):
    """导出：source=cache|favorites|legacy；默认 run_cache 模式导出当前缓存。"""
    body = body or {}
    chart = (body.get("chart") or "").strip()
    site = body.get("site")
    source = (body.get("source") or "").strip().lower()
    try:
        if source == "favorites":
            rows = await asyncio.to_thread(
                fav_store.list_all_favorites, DB_BACKEND,
                site=(site or "").upper() or None, q=None,
            )
            path = await asyncio.to_thread(_export_rows_xlsx, rows, "data/favorites.xlsx")
            return {"status": "ok", "file": path, "source": "favorites"}
        if use_run_cache() and source in ("", "cache"):
            cache_chart = "la" if chart == "la" else "products"
            rows = await asyncio.to_thread(
                run_cache.query_products,
                {"site": (site or "").upper() or None, "limit": 5000, "offset": 0, "detail_only": False},
                chart=cache_chart,
            )
            out = "data/new_arrivals.xlsx" if cache_chart == "la" else "data/products.xlsx"
            path = await asyncio.to_thread(_export_rows_xlsx, rows, out)
            return {"status": "ok", "file": path, "source": "run_cache"}
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
        logging.exception("export_excel failed")
        return JSONResponse(
            {"status": "error", "error_code": "EXPORT_FAILED", "msg": str(e)},
            status_code=500,
        )


async def _abort_crawler_start(proc, run_id: str) -> dict:
    """激活失败后的尽力清理；任何子步骤异常都不阻止返回 CACHE_ACTIVATION_FAILED。"""
    errors: list[str] = []
    proxy_cleanup = None
    if proc is not None:
        try:
            if proc.poll() is None:
                try:
                    proc.terminate()
                except Exception as exc:
                    errors.append(f"terminate: {exc}")
                try:
                    await asyncio.to_thread(proc.wait, 15)
                except Exception:
                    try:
                        proc.kill()
                    except Exception as exc:
                        errors.append(f"kill: {exc}")
                    try:
                        await asyncio.to_thread(proc.wait, 5)
                    except Exception as exc:
                        errors.append(f"wait_after_kill: {exc}")
        except Exception as exc:
            errors.append(f"proc_cleanup: {exc}")
    try:
        await asyncio.to_thread(run_cache.cancel_generation, run_id)
    except Exception as exc:
        errors.append(f"cancel_generation: {exc}")
    try:
        proxy_cleanup = await asyncio.to_thread(stop_proxy_pool)
    except Exception as exc:
        errors.append(f"proxy_cleanup: {exc}")
    return {"cleanup_errors": errors, "proxy_cleanup": proxy_cleanup}


def _export_rows_xlsx(rows: list, rel_path: str) -> str:
    """先写临时文件，查询/写盘全部成功后再 os.replace 原子替换。"""
    from openpyxl import Workbook
    test_export_dir = os.getenv("AMZ_TEST_EXPORT_DIR", "").strip()
    if is_testing() and test_export_dir:
        path = os.path.join(test_export_dir, os.path.basename(rel_path))
    else:
        path = os.path.join(BASE_DIR, rel_path.replace("/", os.sep))
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp_path = path + f".tmp.{os.getpid()}.{time.time_ns()}"
    try:
        wb = Workbook()
        ws = wb.active
        ws.title = "export"
        if not rows:
            ws.append(["empty"])
        else:
            keys = [k for k in rows[0].keys() if k != "snapshot_json"]
            ws.append(keys)
            for r in rows:
                ws.append([r.get(k) for k in keys])
        wb.save(tmp_path)
        os.replace(tmp_path, path)
    except Exception:
        try:
            if os.path.isfile(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        raise
    return rel_path.replace("\\", "/")

# ── 选品清单 ──

@app.get("/api/v2/favorites/count")
async def favorites_count(site: str = Query(None)):
    site = (site or "").strip().upper() or None
    try:
        n = await asyncio.to_thread(fav_store.count_favorites, DB_BACKEND, site)
    except Exception as e:
        logging.exception("favorites_count failed")
        return JSONResponse(
            {"status": "error", "error_code": "FAVORITE_READ_FAILED", "msg": str(e)},
            status_code=503,
        )
    return {"count": n, "site": site}


@app.get("/api/v2/favorites")
async def favorites_list(
    site: str = Query(None),
    q: str = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0, ge=0),
):
    site = (site or "").strip().upper() or None
    try:
        rows = await asyncio.to_thread(
            fav_store.list_favorites, DB_BACKEND, site=site, q=q, limit=limit, offset=offset,
        )
    except Exception as e:
        logging.exception("favorites_list failed")
        return JSONResponse(
            {"status": "error", "error_code": "FAVORITE_READ_FAILED", "msg": str(e)},
            status_code=503,
        )
    for r in rows:
        r["is_favorite"] = True
    return rows


def _favorite_from_cache_locked(cache_id: int, run_id: str) -> dict:
    """在缓存写锁内完成 active 校验、回读与正式库写入，杜绝换代竞态。"""
    with run_cache.locked_active_cache_item(cache_id, run_id) as row:
        return fav_store.upsert_favorite_from_cache(DB_BACKEND, row)


@app.post("/api/v2/favorites")
async def favorites_add(body: dict):
    """仅接受 run_id + cache_id；服务端从运行缓存回读快照后写入正式收藏表。"""
    if not use_run_cache():
        return JSONResponse(
            {"status": "error", "error_code": "RUN_CACHE_DISABLED", "msg": "当前未启用运行缓存模式"},
            status_code=400,
        )
    try:
        cache_id = int(body.get("cache_id"))
    except (TypeError, ValueError):
        return JSONResponse(
            {"status": "error", "error_code": "INVALID_CACHE_ID", "msg": "cache_id 必填"},
            status_code=400,
        )
    run_id = (body.get("run_id") or "").strip()
    if not run_id:
        return JSONResponse(
            {
                "status": "error",
                "error_code": "STALE_CACHE_ITEM",
                "msg": "run_id 必填",
                "active_run_id": run_cache.get_active_run_id(),
            },
            status_code=409,
        )
    try:
        saved = await asyncio.to_thread(_favorite_from_cache_locked, cache_id, run_id)
    except run_cache.StaleCacheError as e:
        return JSONResponse(
            {
                "status": "error",
                "error_code": e.error_code,
                "msg": str(e),
                "active_run_id": e.active_run_id,
            },
            status_code=409,
        )
    except Exception as e:
        logging.exception("favorites_add failed")
        return JSONResponse(
            {"status": "error", "error_code": "FAVORITE_WRITE_FAILED", "msg": str(e)},
            status_code=500,
        )
    saved["is_favorite"] = True
    return {"status": "ok", "favorite": saved}


@app.delete("/api/v2/favorites/{site}/{asin}")
async def favorites_remove(site: str, asin: str):
    site = site.strip().upper()
    asin = asin.strip().upper()
    deleted = await asyncio.to_thread(fav_store.delete_favorite, DB_BACKEND, site, asin)
    return {"status": "ok", "deleted": bool(deleted), "site": site, "asin": asin}


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
