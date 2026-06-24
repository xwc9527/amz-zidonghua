# dashboard_server.py — 独立看板服务 + 爬虫进程管理
# 用法：python dashboard_server.py [端口号]
#
# 功能：
#   1. 提供 REST API，从 SQLite 读数据供看板展示
#   2. 管理爬虫子进程（启动/暂停/恢复/停止），通过 stdin 管道发指令

import json
import os
import sys
import re
import sqlite3
import http.server
import socketserver
import functools
import urllib.parse
import subprocess
import threading

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(BASE_DIR, "data")
DB_FILE     = os.path.join(DATA_DIR, "categories.db")
SCRAPER     = os.path.join(BASE_DIR, "fetch_subtree.py")

# 爬虫子进程状态
_proc         = None          # subprocess.Popen 实例
_proc_lock    = threading.Lock()

_product_proc = None          # fetch_products.py 子进程
_product_lock = threading.Lock()


def db_query(sql, params=()):
    conn = sqlite3.connect(DB_FILE, timeout=5)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def db_scalar(sql, params=()):
    conn = sqlite3.connect(DB_FILE, timeout=5)
    try:
        return conn.execute(sql, params).fetchone()[0]
    finally:
        conn.close()


def _ensure_tables():
    """启动时自动创建缺失的表/列，避免运行时崩溃。"""
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    # product_sightings 表
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS product_sightings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        asin TEXT NOT NULL,
        name TEXT,
        price REAL,
        price_raw TEXT,
        original_price TEXT,
        discount_pct TEXT,
        rating REAL,
        review_count INTEGER,
        rank INTEGER,
        image_url TEXT,
        product_url TEXT,
        has_video INTEGER DEFAULT 0,
        is_amazon_choice INTEGER DEFAULT 0,
        node_id TEXT,
        category_name TEXT,
        category_slug TEXT,
        category_depth INTEGER,
        list_type TEXT,
        list_total INTEGER,
        scraped_at TEXT DEFAULT (datetime('now')),
        UNIQUE(asin, node_id, list_type)
    );
    CREATE INDEX IF NOT EXISTS idx_ps_asin ON product_sightings(asin);
    CREATE INDEX IF NOT EXISTS idx_ps_node ON product_sightings(node_id);
    CREATE INDEX IF NOT EXISTS idx_ps_list ON product_sightings(list_type);
    """)
    # 榜单验证列
    existing = {row[1] for row in conn.execute("PRAGMA table_info(categories)")}
    for col in ["nr_valid", "bs_valid", "ms_valid", "mw_valid"]:
        if col not in existing:
            conn.execute(f"ALTER TABLE categories ADD COLUMN {col} INTEGER")
    # link_cache 独立验证缓存表
    conn.execute("""
        CREATE TABLE IF NOT EXISTS link_cache (
            node_id  TEXT PRIMARY KEY,
            nr_valid INTEGER,
            bs_valid INTEGER,
            ms_valid INTEGER,
            mw_valid INTEGER,
            checked_at TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_lc_node ON link_cache(node_id)")
    conn.commit()
    # 启动时自动从 link_cache 恢复验证结果（防止重建数据库后丢失）
    n = conn.execute("""
        UPDATE categories SET
            nr_valid = COALESCE(nr_valid, (SELECT nr_valid FROM link_cache lc WHERE lc.node_id = categories.node_id)),
            bs_valid = COALESCE(bs_valid, (SELECT bs_valid FROM link_cache lc WHERE lc.node_id = categories.node_id)),
            ms_valid = COALESCE(ms_valid, (SELECT ms_valid FROM link_cache lc WHERE lc.node_id = categories.node_id)),
            mw_valid = COALESCE(mw_valid, (SELECT mw_valid FROM link_cache lc WHERE lc.node_id = categories.node_id))
        WHERE node_id IN (SELECT node_id FROM link_cache)
          AND (nr_valid IS NULL OR bs_valid IS NULL OR ms_valid IS NULL OR mw_valid IS NULL)
    """).rowcount
    conn.commit()
    if n > 0:
        print(f"[启动] 已从 link_cache 恢复 {n} 条验证结果", flush=True)
    conn.close()


# ── 使用 ThreadingHTTPServer 防止单请求阻塞整个服务 ─────────────────

class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """看板 API + 静态文件服务。"""

    def log_message(self, *args):
        pass  # 静默

    def handle_one_request(self):
        """防御：客户端断开连接不崩溃。"""
        try:
            super().handle_one_request()
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass  # 客户端主动断开，忽略

    def do_GET(self):
        try:
            self._dispatch_get()
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            try:
                self.send_error(500, str(e))
            except Exception:
                pass

    def _dispatch_get(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        if path == "/api/stats":
            self._json_response(self._get_stats())

        elif path == "/api/categories":
            qs = urllib.parse.parse_qs(parsed.query)
            self._json_response(self._get_categories(qs))

        elif path == "/api/status":
            self._json_response(self._get_status())

        elif path == "/api/l1_categories":
            self._json_response(self._get_l1_categories())

        elif path == "/api/slug_children":
            qs = urllib.parse.parse_qs(parsed.query)
            self._json_response(self._get_slug_children(qs))

        elif path == "/api/tree_children":
            qs = urllib.parse.parse_qs(parsed.query)
            self._json_response(self._get_tree_children(qs))

        elif path == "/api/check_progress":
            self._json_response(self._get_check_progress())

        elif path == "/api/product_stats":
            self._json_response(_get_product_stats())

        elif path == "/api/product_progress":
            self._json_response(_get_product_progress())

        elif path == "/api/products":
            self._json_response(self._get_products(qs))

        else:
            super().do_GET()

    def do_POST(self):
        try:
            self._dispatch_post()
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            try:
                self.send_error(500, str(e))
            except Exception:
                pass

    def _dispatch_post(self):
        path = urllib.parse.urlparse(self.path).path
        parsed = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(parsed.query)
        if path == "/api/start":
            self._json_response(_start_scraper())
        elif path == "/api/stop":
            self._json_response(_stop_scraper())

        elif path == "/api/start_products":
            # 从 POST body 读参数
            length = int(self.headers.get("Content-Length", 0))
            body   = json.loads(self.rfile.read(length)) if length else {}
            self._json_response(_start_product_scraper(body))
        elif path == "/api/stop_products":
            self._json_response(_stop_product_scraper())
        elif path == "/api/export_excel":
            self._json_response(_export_excel())
        else:
            self.send_error(404)

    def _get_stats(self):
        total = db_scalar("SELECT COUNT(*) FROM categories")
        queue = db_scalar("SELECT COUNT(*) FROM categories WHERE explored=0")
        has_id = db_scalar("SELECT COUNT(*) FROM categories WHERE node_id IS NOT NULL")
        bc = db_scalar("SELECT COUNT(*) FROM categories WHERE source='breadcrumb'")
        validated = db_scalar("SELECT COUNT(*) FROM categories WHERE nr_valid IS NOT NULL")
        depths = {}
        for r in db_query("SELECT depth, COUNT(*) as cnt FROM categories GROUP BY depth ORDER BY depth"):
            depths[f"L{r['depth']}"] = r['cnt']
        return {
            "total": total, "queue": queue,
            "has_id": has_id, "breadcrumb": bc,
            "validated": validated,
            "depths": depths,
        }

    def _get_categories(self, qs):
        parent = qs.get("parent", [""])[0]
        search = qs.get("q", [""])[0]
        limit = int(qs.get("limit", ["2000"])[0])
        offset = int(qs.get("offset", ["0"])[0])

        if parent == "root":
            sql = "SELECT id, name, url, depth, source, explored FROM categories WHERE depth=1 AND node_id IS NULL"
            params = []
            if search:
                sql += " AND name LIKE ?"
                params.append(f"%{search}%")
            sql += " ORDER BY name"
            rows = db_query(sql, params)
            result = []
            for r in rows:
                m = re.search(r"/gp/new-releases/([a-z][a-z0-9-]+)", r["url"])
                slug = m.group(1) if m else ""
                cc = db_scalar(
                    "SELECT SUM(child_count) + COUNT(*) FROM categories WHERE node_id IS NOT NULL AND slug = ?",
                    (slug,)
                ) if slug else 0
                result.append({
                    "id": r["id"], "name": r["name"], "url": r["url"],
                    "node_id": None, "slug": slug, "depth": r["depth"],
                    "source": r["source"], "explored": r["explored"],
                    "nr_valid": None, "bs_valid": None, "ms_valid": None, "mw_valid": None,
                    "child_count": cc or 0
                })
            return result[offset:offset+limit]

        elif parent:
            sql = ("SELECT c.id, c.name, c.url, c.node_id, c.depth, c.source, c.explored, "
                   "COALESCE(c.nr_valid, lc.nr_valid) AS nr_valid, "
                   "COALESCE(c.bs_valid, lc.bs_valid) AS bs_valid, "
                   "COALESCE(c.ms_valid, lc.ms_valid) AS ms_valid, "
                   "COALESCE(c.mw_valid, lc.mw_valid) AS mw_valid, "
                   "c.child_count "
                   "FROM categories c "
                   "LEFT JOIN link_cache lc ON lc.node_id = c.node_id "
                   "WHERE c.node_id IS NOT NULL")
            params = []
            if parent.isdigit():
                sql += " AND c.parent_node_id = ?"
                params.append(parent)
            else:
                # 兼容：如果数据库中 parent_node_id 已经建好树，则匹配之；否则回退到旧版的 url 匹配逻辑
                sql += " AND (c.parent_node_id = ? OR ((c.parent_node_id IS NULL OR c.parent_node_id = '') AND c.url LIKE ?))"
                params.extend([parent, f"%/gp/new-releases/{parent}/%"])

            if search:
                sql += " AND (c.name LIKE ? OR c.node_id LIKE ?)"
                params.extend([f"%{search}%", f"%{search}%"])

            sql += " ORDER BY c.name"
            sql += f" LIMIT {limit} OFFSET {offset}"
            return db_query(sql, params)

        else:
            sql = ("SELECT c.id, c.name, c.url, c.node_id, c.depth, c.source, c.explored, "
                   "COALESCE(c.nr_valid, lc.nr_valid) AS nr_valid, "
                   "COALESCE(c.bs_valid, lc.bs_valid) AS bs_valid, "
                   "COALESCE(c.ms_valid, lc.ms_valid) AS ms_valid, "
                   "COALESCE(c.mw_valid, lc.mw_valid) AS mw_valid, "
                   "c.child_count "
                   "FROM categories c "
                   "LEFT JOIN link_cache lc ON lc.node_id = c.node_id "
                   "WHERE c.node_id IS NOT NULL")
            params = []
            if search:
                sql += " AND (c.name LIKE ? OR c.node_id LIKE ?)"
                params.extend([f"%{search}%", f"%{search}%"])

            depth = qs.get("depth", [""])[0]
            if depth and depth != "all":
                d = int(depth)
                if d >= 5:
                    sql += " AND c.depth >= 5"
                else:
                    sql += " AND c.depth = ?"
                    params.append(d)

            sql += " ORDER BY c.depth, c.name"
            sql += f" LIMIT {limit} OFFSET {offset}"
            return db_query(sql, params)

    def _get_products(self, qs=None):
        """返回商品记录，支持分页。"""
        try:
            limit = int((qs or {}).get("limit", ["50"])[0])
            offset = int((qs or {}).get("offset", ["0"])[0])
            limit = min(limit, 200)
            return db_query(
                "SELECT name, asin, price, review_count, rank FROM product_sightings "
                f"ORDER BY scraped_at DESC LIMIT {limit} OFFSET {offset}"
            )
        except Exception:
            return []

    def _get_status(self):
        running = _proc is not None and _proc.poll() is None
        try:
            rows = db_query("SELECT phase, total, queue, updated_at FROM run_status WHERE id=1")
            st = rows[0] if rows else {"phase": "idle"}
        except Exception:
            st = {"phase": "idle"}
        # 检测外部启动的爬虫：数据库状态在 60s 内有更新且非终态
        if not running and st.get("updated_at"):
            try:
                from datetime import datetime, timezone
                last = datetime.fromisoformat(st["updated_at"]).replace(tzinfo=timezone.utc)
                if (datetime.now(timezone.utc) - last).total_seconds() < 60 \
                   and st.get("phase") not in ("idle", "done", None):
                    running = True
            except Exception:
                pass
        st["scraper_running"] = running
        if not running:
            queue = db_scalar("SELECT COUNT(*) FROM categories WHERE explored=0")
            if queue == 0 and db_scalar("SELECT COUNT(*) FROM categories") > 0:
                st["phase"] = "done"
        return st

    def _json_response(self, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        try:
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)
        except (ConnectionAbortedError, ConnectionResetError, BrokenPipeError):
            pass  # 客户端已断开

    def _get_l1_categories(self):
        """返回 L1 顶级类目列表（slug 格式），供商品抓取面板使用。"""
        rows = db_query(
            "SELECT name, url FROM categories WHERE depth=1 AND node_id IS NULL "
            "ORDER BY name"
        )
        result = []
        seen_slugs = set()
        for r in rows:
            m = re.search(r'/gp/new-releases/([a-z][a-z0-9-]+)', r['url'])
            if m:
                slug = m.group(1)
                if slug in seen_slugs:
                    continue
                seen_slugs.add(slug)
                child_count = db_scalar(
                    "SELECT COUNT(*) FROM categories "
                    "WHERE node_id IS NOT NULL AND url LIKE ?",
                    (f"%/gp/new-releases/{slug}/%",)
                )
                result.append({
                    "name": r['name'],
                    "slug": slug,
                    "child_count": child_count,
                })
        # 防御性：扫描子节点 URL 实际出现的 slug，补充 L1 行里缺失的孤儿 slug
        all_child_urls = db_query(
            "SELECT DISTINCT url FROM categories WHERE node_id IS NOT NULL"
        )
        for r in all_child_urls:
            m = re.search(r'/gp/new-releases/([a-z][a-z0-9-]+)/', r['url'])
            if m:
                slug = m.group(1)
                if slug not in seen_slugs:
                    seen_slugs.add(slug)
                    child_count = db_scalar(
                        "SELECT COUNT(*) FROM categories "
                        "WHERE node_id IS NOT NULL AND url LIKE ?",
                        (f"%/gp/new-releases/{slug}/%",)
                    )
                    result.append({
                        "name": slug.replace("-", " ").title(),
                        "slug": slug,
                        "child_count": child_count,
                    })
        result.sort(key=lambda x: x['name'])
        return result

    def _get_slug_children(self, qs):
        """返回指定 slug 下所有子类目节点，供商品面板展开树使用。"""
        slug = qs.get("slug", [""])[0]
        if not slug:
            return []
        return db_query(
            "SELECT name, url, node_id, depth, parent_node_id FROM categories "
            "WHERE node_id IS NOT NULL AND url LIKE ? "
            "ORDER BY depth, name",
            (f"%/gp/new-releases/{slug}/%",)
        )

    def _get_tree_children(self, qs):
        parent = qs.get("parent", [""])[0]
        search = qs.get("q", [""])[0]
        if parent == "root":
            total = db_scalar("SELECT COUNT(*) FROM categories")
            roots = db_query("SELECT name, node_id FROM categories WHERE depth = 0 LIMIT 10")
            if roots:
                return [{"name": r["name"], "node_id": r["node_id"], "depth": 0, "child_count": total} for r in roots]
            return [{"name": "All Categories", "node_id": "_root_", "depth": 0, "child_count": total}]
        elif db_scalar("SELECT COUNT(*) FROM categories WHERE node_id = ? AND depth = 0", (parent,)) > 0:
            sql = "SELECT c.name, c.node_id, c.depth, c.slug, c.child_count FROM categories c WHERE c.depth = 1"
            params = []
            if search:
                sql += " AND c.name LIKE ?"
                params.append(f"%{search}%")
            sql += " ORDER BY c.name"
            return db_query(sql, params)
        else:
            sql = "SELECT c.name, c.node_id, c.depth, c.slug, c.child_count FROM categories c WHERE c.parent_node_id = ?"
            params = [parent]
            if search:
                sql += " AND c.name LIKE ?"
                params.append(f"%{search}%")
            sql += " ORDER BY c.name"
            return db_query(sql, params)

    def _get_check_progress(self):
        total   = db_scalar("SELECT COUNT(*) FROM categories WHERE node_id IS NOT NULL")
        checked = db_scalar("SELECT COUNT(*) FROM categories WHERE node_id IS NOT NULL AND nr_valid IS NOT NULL")
        valid   = {}
        for col, label in [("nr_valid","新品榜"),("bs_valid","畅销榜"),("ms_valid","飙升榜"),("mw_valid","心愿单")]:
            valid[label] = db_scalar(f"SELECT COUNT(*) FROM categories WHERE {col}=1")
        running = _product_proc is not None and _product_proc.poll() is None
        return {"total": total, "checked": checked, "valid_counts": valid, "running": running}




# ── 进程管理函数 ──────────────────────────────────────────────────

def _start_scraper():
    global _proc, _is_paused
    with _proc_lock:
        if _proc is not None and _proc.poll() is None:
            return {"status": "already_running", "pid": _proc.pid}
        _proc = subprocess.Popen(
            [sys.executable, "-u", SCRAPER],
            stdin=subprocess.PIPE,
            cwd=BASE_DIR,
        )
        _is_paused = False
        print(f"[爬虫] 已启动 PID={_proc.pid}", flush=True)
    return {"status": "started", "pid": _proc.pid}


def _stop_scraper():
    global _proc
    with _proc_lock:
        if _proc is None or _proc.poll() is not None:
            return {"status": "not_running"}
        try:
            _proc.stdin.write(b"stop\n")
            _proc.stdin.flush()
        except Exception:
            pass
        try:
            _proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            _proc.terminate()
        pid = _proc.pid
        _proc = None
        print(f"[爬虫] 已停止 PID={pid}", flush=True)
    return {"status": "stopped"}


# ── 商品抓取进程管理 ──────────────────────────────────────────────────

def _get_product_stats():
    """商品数据统计。"""
    try:
        total   = db_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
        by_list = db_query(
            "SELECT list_type, COUNT(*) as cnt FROM product_sightings GROUP BY list_type"
        )
        multi   = db_scalar(
            "SELECT COUNT(*) FROM ("
            "  SELECT asin FROM product_sightings GROUP BY asin HAVING COUNT(DISTINCT list_type)>1"
            ")"
        )
        running = _product_proc is not None and _product_proc.poll() is None
        return {"total_asins": total, "by_list": by_list,
                "multi_list": multi, "running": running}
    except Exception:
        return {"total_asins": 0, "by_list": [], "multi_list": 0, "running": False}


def _get_product_progress():
    """返回抓取进度（从子进程 stdout 无法实时读，改从 DB 增量判断）。"""
    running = _product_proc is not None and _product_proc.poll() is None
    try:
        total   = db_scalar("SELECT COUNT(DISTINCT asin) FROM product_sightings")
    except Exception:
        total = 0
    return {"running": running, "total_products": total}


def _start_product_scraper(params: dict):
    """启动 fetch_products.py 子进程。params 来自前端 POST body。"""
    global _product_proc
    with _product_lock:
        if _product_proc is not None and _product_proc.poll() is None:
            return {"status": "already_running"}
        cmd = [sys.executable, "-u",
               os.path.join(BASE_DIR, "fetch_products.py")]
        # 新版用 --slugs 替代 --roots
        if params.get("slugs"):
            cmd += ["--slugs"] + params["slugs"]
        elif params.get("roots"):
            cmd += ["--roots"] + params["roots"]
        else:
            return {"status": "error", "msg": "no slugs or roots specified"}
        if params.get("site"):
            cmd += ["--site", params["site"]]
        if params.get("lists"):
            cmd += ["--lists"] + params["lists"]
        if params.get("review_max"):
            cmd += ["--review-max", str(params["review_max"])]
        if params.get("min_list"):
            cmd += ["--min-list", str(params["min_list"])]
        if params.get("price_min"):
            cmd += ["--price-min", str(params["price_min"])]
        if params.get("price_max"):
            cmd += ["--price-max", str(params["price_max"])]
        if params.get("delay"):
            cmd += ["--delay", str(params["delay"])]
        _product_proc = subprocess.Popen(cmd, cwd=BASE_DIR)
    print(f"[商品] 已启动 PID={_product_proc.pid}", flush=True)
    return {"status": "started", "pid": _product_proc.pid}


def _stop_product_scraper():
    global _product_proc
    with _product_lock:
        if _product_proc is None or _product_proc.poll() is not None:
            return {"status": "not_running"}
        _product_proc.terminate()
        pid = _product_proc.pid
        _product_proc = None
    print(f"[商品] 已停止 PID={pid}", flush=True)
    return {"status": "stopped"}


def _export_excel():
    """手动触发导出 Excel。"""
    try:
        import fetch_products
        fetch_products.export_excel()
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "msg": str(e)}


def get_py_files():
    py_files = []
    for root, dirs, files in os.walk(BASE_DIR):
        if "venv" in root or ".git" in root or "__pycache__" in root:
            continue
        for file in files:
            if file.endswith(".py"):
                py_files.append(os.path.join(root, file))
    return py_files


def get_mtimes(files):
    mtimes = {}
    for f in files:
        try:
            mtimes[f] = os.path.getmtime(f)
        except OSError:
            pass
    return mtimes


def main():
    # 启用自修复与代码热重载机制 (开发/守护模式)
    ENV_VAR = "DASHBOARD_SERVER_CHILD"
    if ENV_VAR not in os.environ:
        import time
        print("[守护进程] 自修复与代码热重载机制已启动。", flush=True)
        p = None
        try:
            while True:
                child_env = os.environ.copy()
                child_env[ENV_VAR] = "1"
                p = subprocess.Popen([sys.executable] + sys.argv, env=child_env)
                
                py_files = get_py_files()
                mtimes = get_mtimes(py_files)
                
                restarted = False
                while p.poll() is None:
                    time.sleep(1)
                    current_files = get_py_files()
                    current_mtimes = get_mtimes(current_files)
                    
                    changed = False
                    if set(current_files) != set(py_files):
                        changed = True
                    else:
                        for f in current_files:
                            if current_mtimes.get(f) != mtimes.get(f):
                                changed = True
                                break
                    if changed:
                        print("[守护进程] 检测到代码修改，正在自动重启后端服务...", flush=True)
                        p.terminate()
                        try:
                            p.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            p.kill()
                        restarted = True
                        break
                
                if not restarted:
                    code = p.returncode
                    print(f"[守护进程] 后端服务已退出 (退出码: {code})。自修复机制将在 2 秒后自动重启服务...", flush=True)
                    time.sleep(2)
        except KeyboardInterrupt:
            print("\n[守护进程] 正在停止守护与后端服务...", flush=True)
            if p and p.poll() is None:
                p.terminate()
                try:
                    p.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    p.kill()
        sys.exit(0)

    # 子进程执行的实际 HTTP 服务逻辑
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080

    if not os.path.exists(DB_FILE):
        print(f"[错误] 数据库不存在: {DB_FILE}", flush=True)
        print("请先运行 init_db.py", flush=True)
        sys.exit(1)

    _ensure_tables()

    handler = functools.partial(DashboardHandler, directory=DATA_DIR)
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    stats = db_query("SELECT COUNT(*) as total FROM categories")[0]
    print(f"[看板] http://localhost:{port}/dashboard.html", flush=True)
    print(f"[看板] 数据库: {DB_FILE}  当前 {stats['total']} 个节点", flush=True)
    print("[看板] Ctrl+C 停止", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        _stop_scraper()


if __name__ == "__main__":
    main()
