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
    sets = ", ".join(f"{k}=?" for k in results)
    vals = list(results.values()) + [node_id]
    with _db_lock:
        conn = db_conn()
        try:
            conn.execute(f"UPDATE categories SET {sets} WHERE node_id=?", vals)
            conn.commit()
        finally:
            conn.close()


def run_batch(workers: int = 10, target_node_id: str = None):
    conn = db_conn()
    if target_node_id:
        rows = conn.execute(
            "SELECT node_id, url FROM categories WHERE node_id=? LIMIT 1",
            (target_node_id,)
        ).fetchall()
    else:
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
