# dashboard_server.py — 独立看板服务 + 爬虫进程管理
# 用法：python dashboard_server.py [端口号]
#
# 功能：
#   1. 提供 REST API，从 SQLite 读数据供看板展示
#   2. 管理爬虫子进程（启动/暂停/恢复/停止），通过 stdin 管道发指令

import json
import os
import sys
import sqlite3
import http.server
import functools
import urllib.parse
import subprocess
import threading

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
DATA_DIR    = os.path.join(BASE_DIR, "data")
DB_FILE     = os.path.join(DATA_DIR, "categories.db")
SCRAPER     = os.path.join(BASE_DIR, "fetch_categories.py")

# 爬虫子进程状态
_proc         = None          # subprocess.Popen 实例
_proc_lock    = threading.Lock()
_check_proc   = None          # check_links.py 子进程
_check_lock   = threading.Lock()
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

        elif path == "/api/roots":
            self._json_response(self._get_roots())

        elif path == "/api/children":
            qs = urllib.parse.parse_qs(parsed.query)
            self._json_response(self._get_children(qs))

        elif path == "/api/check_progress":
            self._json_response(self._get_check_progress())

        elif path == "/api/product_stats":
            self._json_response(_get_product_stats())

        elif path == "/api/product_progress":
            self._json_response(_get_product_progress())

        elif path == "/api/l1_slugs":
            self._json_response(_get_l1_slugs())

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
        elif path == "/api/check_all":
            self._json_response(_start_check_links())
        elif path == "/api/check_node":
            node_id = qs.get("node_id", [""])[0]
            self._json_response(_check_single_node(node_id))
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
        depths = {}
        for r in db_query("SELECT depth, COUNT(*) as cnt FROM categories GROUP BY depth ORDER BY depth"):
            depths[f"L{r['depth']}"] = r['cnt']
        return {
            "total": total, "queue": queue,
            "has_id": has_id, "breadcrumb": bc,
            "depths": depths,
        }

    def _get_categories(self, qs):
        sql = ("SELECT name, url, node_id, depth, source, explored, "
               "nr_valid, bs_valid, ms_valid, mw_valid "
               "FROM categories WHERE 1=1")
        params = []

        search = qs.get("q", [""])[0]
        if search:
            sql += " AND (name LIKE ? OR node_id LIKE ?)"
            params.extend([f"%{search}%", f"%{search}%"])

        depth = qs.get("depth", [""])[0]
        if depth and depth != "all":
            d = int(depth)
            if d >= 5:
                sql += " AND depth >= 5"
            else:
                sql += " AND depth = ?"
                params.append(d)

        sql += " ORDER BY depth, name"

        limit = int(qs.get("limit", ["500"])[0])
        offset = int(qs.get("offset", ["0"])[0])
        sql += f" LIMIT {limit} OFFSET {offset}"

        return db_query(sql, params)

    def _get_status(self):
        running = _proc is not None and _proc.poll() is None
        try:
            rows = db_query("SELECT phase, total, queue, updated_at FROM run_status WHERE id=1")
            st = rows[0] if rows else {"phase": "idle"}
        except Exception:
            st = {"phase": "idle"}
        st["scraper_running"] = running
        # 爬虫已退出时，根据队列实际状态推导阶段
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

    def _get_roots(self):
        """返回 L1 顶级类目（parent_node_id IS NULL 且 true_depth=2）。"""
        rows = db_query(
            "SELECT c.name, c.url, c.node_id, c.true_depth, "
            "IFNULL(cc.cnt, 0) AS child_count "
            "FROM categories c "
            "LEFT JOIN (SELECT parent_node_id, COUNT(*) AS cnt "
            "           FROM categories GROUP BY parent_node_id) cc "
            "  ON cc.parent_node_id = c.node_id "
            "WHERE c.parent_node_id IS NULL AND c.true_depth IS NOT NULL "
            "ORDER BY c.name"
        )
        return rows

    def _get_children(self, qs):
        """返回指定 parent_node_id 的直接子节点。"""
        parent_id = qs.get("parent_id", [""])[0]
        offset    = int(qs.get("offset", ["0"])[0])
        limit     = int(qs.get("limit",  ["200"])[0])
        if not parent_id:
            return []
        rows = db_query(
            "SELECT c.name, c.url, c.node_id, c.true_depth, "
            "IFNULL(cc.cnt, 0) AS child_count "
            "FROM categories c "
            "LEFT JOIN (SELECT parent_node_id, COUNT(*) AS cnt "
            "           FROM categories GROUP BY parent_node_id) cc "
            "  ON cc.parent_node_id = c.node_id "
            "WHERE c.parent_node_id=? "
            "ORDER BY c.name LIMIT ? OFFSET ?",
            (parent_id, limit, offset)
        )
        total = db_scalar(
            "SELECT COUNT(*) FROM categories WHERE parent_node_id=?", (parent_id,)
        )
        return {"items": rows, "total": total, "offset": offset, "limit": limit}

    def _get_check_progress(self):
        total   = db_scalar("SELECT COUNT(*) FROM categories WHERE node_id IS NOT NULL")
        checked = db_scalar("SELECT COUNT(*) FROM categories WHERE node_id IS NOT NULL AND nr_valid IS NOT NULL")
        valid   = {}
        for col, label in [("nr_valid","新品榜"),("bs_valid","畅销榜"),("ms_valid","飙升榜"),("mw_valid","心愿单")]:
            valid[label] = db_scalar(f"SELECT COUNT(*) FROM categories WHERE {col}=1")
        global _check_proc
        running = _check_proc is not None and _check_proc.poll() is None
        return {"total": total, "checked": checked, "valid_counts": valid, "running": running}


# ── check_links 进程管理 ───────────────────────────────────────────

def _start_check_links():
    global _check_proc
    with _check_lock:
        if _check_proc is not None and _check_proc.poll() is None:
            return {"status": "already_running"}
        _check_proc = subprocess.Popen(
            [sys.executable, "-u", os.path.join(BASE_DIR, "check_links.py"),
             "--workers", "10"],
            cwd=BASE_DIR
        )
    print(f"[check_links] 已启动 PID={_check_proc.pid}", flush=True)
    return {"status": "started", "pid": _check_proc.pid}


def _check_single_node(node_id: str):
    """启动 check_links.py --node_id 子进程检测单节点，不阻塞服务器。"""
    if not node_id:
        return {"error": "node_id required"}
    subprocess.Popen(
        [sys.executable, "-u", os.path.join(BASE_DIR, "check_links.py"),
         "--node_id", node_id, "--workers", "1", "--delay", "0.2"],
        cwd=BASE_DIR
    )
    return {"status": "checking", "node_id": node_id}


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

def _get_l1_slugs():
    """返回 DB 中所有 L1 的 slug 列表供前端下拉。"""
    rows = db_query(
        "SELECT DISTINCT REPLACE(SUBSTR(url, INSTR(url,'new-releases/')+13), "
        "SUBSTR(REPLACE(SUBSTR(url, INSTR(url,'new-releases/')+13),'/','-'), "
        "INSTR(REPLACE(SUBSTR(url, INSTR(url,'new-releases/')+13),'/','-'),'-')), '') "
        "FROM categories WHERE depth=1 AND url LIKE '%/new-releases/%'"
    )
    # 简化：直接用正则从 url 提取 L1 slug
    import re
    all_urls = db_query("SELECT DISTINCT url FROM categories WHERE depth=1")
    slugs = set()
    for row in all_urls:
        m = re.search(r'/gp/new-releases/([^/]+)/', row["url"])
        if m:
            slugs.add(m.group(1))
    return sorted(slugs)


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
        if params.get("roots"):
            cmd += ["--roots"] + params["roots"]
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


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080

    if not os.path.exists(DB_FILE):
        print(f"[错误] 数据库不存在: {DB_FILE}")
        print("请先运行 init_db.py")
        sys.exit(1)

    handler = functools.partial(DashboardHandler, directory=DATA_DIR)
    server = http.server.HTTPServer(("localhost", port), handler)
    stats = db_query("SELECT COUNT(*) as total FROM categories")[0]
    print(f"[看板] http://localhost:{port}/dashboard.html", flush=True)
    print(f"[看板] 数据库: {DB_FILE}  当前 {stats['total']} 个节点", flush=True)
    print("[看板] Ctrl+C 停止", flush=True)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[看板] 正在停止...")
        _stop_scraper()


if __name__ == "__main__":
    main()
