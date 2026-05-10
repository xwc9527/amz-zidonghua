"""
check_links.py — 批量验证 Amazon 4 个榜单 URL 有效性
用法: python check_links.py [--workers N] [--node_id ID]

性能策略：
  - stream=True 只取状态码，不下载 body
  - timeout=(5,3): 足够宽容，不因网络抖动误判
  - 10 workers 并发（10×4=40 req 峰值，Amazon 可接受）
  - 单节点 4 URL 串行但无 sleep（每个 ~300ms，单节点 ~1.2s）
"""
import sqlite3, requests, threading, time, sys, os, argparse
from queue import Queue, Empty

BASE    = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "data", "categories.db")

LISTS = [
    ("nr_valid", "new-releases",       "新品榜"),
    ("bs_valid", "bestsellers",        "畅销榜"),
    ("ms_valid", "movers-and-shakers", "飙升榜"),
    ("mw_valid", "most-wished-for",    "心愿单"),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

_db_lock    = threading.Lock()
_done_count = [0]
_done_lock  = threading.Lock()


def db_conn():
    c = sqlite3.connect(DB_PATH, timeout=15)
    c.execute("PRAGMA journal_mode=WAL")
    return c


def ensure_cache_table():
    """创建独立的 link_cache 表——与 categories 解耦，重建数据库后仍可恢复验证结果。"""
    with _db_lock:
        conn = db_conn()
        try:
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
        finally:
            conn.close()


def restore_from_cache():
    """把 link_cache 的验证结果同步回 categories（数据库重建后调用）。"""
    with _db_lock:
        conn = db_conn()
        try:
            n = conn.execute("""
                UPDATE categories SET
                    nr_valid = (SELECT nr_valid FROM link_cache WHERE link_cache.node_id = categories.node_id),
                    bs_valid = (SELECT bs_valid FROM link_cache WHERE link_cache.node_id = categories.node_id),
                    ms_valid = (SELECT ms_valid FROM link_cache WHERE link_cache.node_id = categories.node_id),
                    mw_valid = (SELECT mw_valid FROM link_cache WHERE link_cache.node_id = categories.node_id)
                WHERE node_id IN (SELECT node_id FROM link_cache)
            """).rowcount
            conn.commit()
            print(f"[link_cache] 已从缓存恢复 {n} 条验证结果", flush=True)
            return n
        finally:
            conn.close()


def extract_slug_and_id(url: str):
    """从 new-releases URL 提取 slug 和 node_id。"""
    parts = url.rstrip("/").split("/")
    try:
        gp_idx = parts.index("gp")
        slug   = parts[gp_idx + 2] if len(parts) > gp_idx + 2 else None
        nid    = parts[gp_idx + 3] if len(parts) > gp_idx + 3 else None
        return slug, nid
    except (ValueError, IndexError):
        return None, None


def probe_url(session: requests.Session, url: str) -> int:
    """只取状态码，body 不读。"""
    try:
        with session.get(url, stream=True, timeout=(5, 3),
                         allow_redirects=True) as r:
            return r.status_code
    except Exception:
        return 0


def check_node(node_id: str, url: str, session: requests.Session) -> dict:
    """串行检测 4 个榜单（每个 ~300ms，共 ~1.2s）。"""
    slug, nid = extract_slug_and_id(url)
    if not slug or not nid:
        return {col: 0 for col, _, _ in LISTS}
    results = {}
    for col, prefix, _ in LISTS:
        test_url = f"https://www.amazon.com/gp/{prefix}/{slug}/{nid}/"
        results[col] = 1 if probe_url(session, test_url) == 200 else 0
    return results


def save_result(node_id: str, results: dict):
    """写入验证结果：同时更新 categories（快速展示）和 link_cache（持久备份）。"""
    sets = ", ".join(f"{k}=?" for k in results)
    vals = list(results.values()) + [node_id]
    cache_cols = ", ".join(results.keys())
    cache_phs  = ", ".join("?" * len(results))
    cache_upd  = ", ".join(f"{k}=excluded.{k}" for k in results)
    with _db_lock:
        conn = db_conn()
        try:
            # 1. 写 categories（即时反映到看板）
            conn.execute(f"UPDATE categories SET {sets} WHERE node_id=?", vals)
            # 2. 写 link_cache（持久备份，不受数据库重建影响）
            conn.execute(
                f"INSERT INTO link_cache (node_id, {cache_cols}, checked_at) "
                f"VALUES (?, {cache_phs}, datetime('now')) "
                f"ON CONFLICT(node_id) DO UPDATE SET {cache_upd}, checked_at=datetime('now')",
                [node_id] + list(results.values())
            )
            conn.commit()
        finally:
            conn.close()


def run_batch(workers: int = 10, target_node_id: str = None):
    ensure_cache_table()  # 确保 link_cache 表存在
    conn = db_conn()
    if target_node_id:
        rows = conn.execute(
            "SELECT node_id, url FROM categories WHERE node_id=? LIMIT 1",
            (target_node_id,)
        ).fetchall()
    else:
        # 先从 link_cache 恢复（修复因数据库重建丢失的结果）
        restored = restore_from_cache()
        # 只抓未验证的（categories.nr_valid 仍为 NULL）
        rows = conn.execute(
            "SELECT node_id, url FROM categories WHERE node_id IS NOT NULL AND nr_valid IS NULL"
        ).fetchall()
    conn.close()

    total = len(rows)
    if total == 0:
        print("[check_links] 所有节点已检测完毕，无需重复", flush=True)
        return

    q = Queue()
    for row in rows:
        q.put(row)

    t0 = time.time()
    print(f"[check_links] 待检测 {total} 节点，{workers} workers（stream 模式）", flush=True)

    def worker():
        session = requests.Session()
        session.headers.update(HEADERS)
        while True:
            try:
                node_id, url = q.get(timeout=10)
            except Empty:
                break
            try:
                results = check_node(node_id, url, session)
                save_result(node_id, results)
                with _done_lock:
                    _done_count[0] += 1
                    n = _done_count[0]
                if n % 100 == 0 or n == total:
                    elapsed = time.time() - t0
                    rate = n / elapsed if elapsed > 0 else 0
                    pct  = n * 100 // total
                    print(f"  [{n}/{total} {pct}%] {rate:.1f} 节点/s", flush=True)
            finally:
                q.task_done()

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(workers)]
    for t in threads: t.start()
    q.join()
    for t in threads: t.join()

    elapsed = time.time() - t0
    rate = total / elapsed if elapsed > 0 else 0
    print(f"[check_links] 完成！{total} 节点 / {elapsed:.1f}s = {rate:.1f} 节点/s", flush=True)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=10, help="并发 worker 数（默认10）")
    parser.add_argument("--node_id", type=str, default=None, help="只检测指定 node_id")
    args = parser.parse_args()
    run_batch(workers=args.workers, target_node_id=args.node_id)
