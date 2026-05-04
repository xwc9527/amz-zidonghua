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
_proc      = None          # subprocess.Popen 实例
_proc_lock = threading.Lock()


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

    def do_GET(self):
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

        else:
            super().do_GET()

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        if path == "/api/start":
            self._json_response(_start_scraper())
        elif path == "/api/stop":
            self._json_response(_stop_scraper())
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
        sql = "SELECT name, url, node_id, depth, source, explored FROM categories WHERE 1=1"
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
        return st

    def _json_response(self, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _get_roots(self):
        """返回顶层类目（depth=1），作为树形视图的根节点。"""
        rows = db_query(
            "SELECT name, url, node_id, depth, source, "
            "(SELECT COUNT(*) FROM categories c2 WHERE RTRIM(c2.parent_url,'/')=RTRIM(c.url,'/')) AS child_count "
            "FROM categories c WHERE depth=1 ORDER BY name"
        )
        return rows

    def _get_children(self, qs):
        """返回指定 parent_url 的直接子节点，支持分页。"""
        parent_url = qs.get("parent_url", [""])[0].rstrip("/")
        offset     = int(qs.get("offset", ["0"])[0])
        limit      = int(qs.get("limit",  ["50"])[0])
        if not parent_url:
            return []
        rows = db_query(
            "SELECT name, url, node_id, depth, source, "
            "(SELECT COUNT(*) FROM categories c2 WHERE RTRIM(c2.parent_url,'/')=RTRIM(c.url,'/')) AS child_count "
            "FROM categories c WHERE RTRIM(c.parent_url,'/')=? "
            "ORDER BY name LIMIT ? OFFSET ?",
            (parent_url, limit, offset)
        )
        total = db_scalar(
            "SELECT COUNT(*) FROM categories WHERE RTRIM(parent_url,'/')=?", (parent_url,)
        )
        return {"items": rows, "total": total, "offset": offset, "limit": limit}


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
