"""
probe_na_valid.py — 检测类目节点是否支持"最新到货"排序
判据：解析搜索页排序下拉，date-desc-rank 在选项里 → 支持
结果写入 categories.na_valid (1=支持, 0=不支持, NULL=未检测)

用法:
  python probe_na_valid.py --site US
  python probe_na_valid.py --site DE
  python probe_na_valid.py --site JP
  python probe_na_valid.py           # 全部顺序跑
  python probe_na_valid.py --reset   # 清空重跑
  python probe_na_valid.py --workers 5
"""

import argparse, json, os, re, sqlite3, sys, threading, time, random, logging
from queue import Queue, Empty
from curl_cffi import requests as cr

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

BASE = os.path.dirname(os.path.abspath(__file__))
DB   = os.path.join(BASE, "data", "categories.db")
POOL_FILE = os.path.join(BASE, "data", "proxy_pool.json")

logging.basicConfig(level=logging.INFO, format="%(message)s",
                    handlers=[logging.StreamHandler(sys.stdout)])
log = logging.getLogger("probe_na")

SITE_DOMAIN = {
    "US": "https://www.amazon.com",
    "DE": "https://www.amazon.de",
    "JP": "https://www.amazon.co.jp",
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

_db_lock    = threading.Lock()
_stats_lock = threading.Lock()
_stats = {"done": 0, "yes": 0, "no": 0, "err": 0, "total": 0}


def ensure_column():
    conn = sqlite3.connect(DB, timeout=15)
    conn.execute("PRAGMA journal_mode=WAL")
    existing = {r[1] for r in conn.execute("PRAGMA table_info(categories)")}
    if "na_valid" not in existing:
        conn.execute("ALTER TABLE categories ADD COLUMN na_valid INTEGER")
        conn.commit()
        log.info("[DB] 已新增 na_valid 列")
    conn.close()


def load_nodes(site: str, reset: bool) -> list:
    conn = sqlite3.connect(DB, timeout=15)
    if reset:
        conn.execute("UPDATE categories SET na_valid=NULL WHERE site=?", (site,))
        conn.commit()
        log.info(f"[{site}] 已重置 na_valid")
    rows = conn.execute(
        "SELECT node_id, name, depth FROM categories "
        "WHERE site=? AND node_id IS NOT NULL AND na_valid IS NULL "
        "ORDER BY depth, name",
        (site,)
    ).fetchall()
    conn.close()
    return [{"node_id": r[0], "name": r[1], "depth": r[2]} for r in rows]


def write_batch(updates: list):
    """updates = [(valid, site, node_id), ...]"""
    with _db_lock:
        conn = sqlite3.connect(DB, timeout=15)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executemany(
            "UPDATE categories SET na_valid=? WHERE site=? AND node_id=?", updates
        )
        conn.commit()
        conn.close()


def new_session(domain: str, proxy_entry: dict | None = None) -> cr.Session:
    ua = random.choice(USER_AGENTS)
    proxies = {}
    if proxy_entry:
        proxies = {"http": proxy_entry["proxy"], "https": proxy_entry["proxy"]}
    s = cr.Session(impersonate="chrome124", verify=False, proxies=proxies,
                   headers={"User-Agent": ua, "Accept-Language": "en-US,en;q=0.9"})
    try:
        s.get(f"{domain}/", timeout=12)
        time.sleep(random.uniform(1.0, 2.0))
    except Exception:
        pass
    return s


def check_html(html: str) -> str:
    """返回 'yes'/'no'/'no_select'/'robot'"""
    if "api-services" in html or "Something went wrong" in html:
        return "robot"
    m = re.search(r'id="s-result-sort-select".*?</select>', html, re.S)
    if not m:
        return "no_select"
    opts = re.findall(r'<option[^>]*value="([^"]*)"', m.group(0))
    return "yes" if "date-desc-rank" in opts else "no"


def worker(site: str, domain: str, q: Queue, proxy_entry: dict, mgr=None):
    port = proxy_entry.get("port") if proxy_entry else None
    s = new_session(domain, proxy_entry)
    consecutive_err = 0
    pending_writes = []

    while True:
        if mgr is not None and port is not None and mgr.should_stop(port):
            break
        try:
            node = q.get_nowait()
        except Empty:
            break

        node_id = node["node_id"]
        result  = "err"
        for attempt in range(3):
            try:
                url = f"{domain}/s?rh=n%3A{node_id}&s=date-desc-rank"
                r = s.get(url, timeout=18)
                if r.status_code == 503:
                    result = "robot"
                    break
                result = check_html(r.text)
                if result == "robot":
                    # rotate session
                    s.close()
                    time.sleep(random.uniform(15, 25))
                    s = new_session(domain, proxy_entry)
                    continue
                break
            except Exception:
                time.sleep(random.uniform(3, 6))
                s.close()
                s = new_session(domain, proxy_entry)

        valid = 1 if result == "yes" else (0 if result in ("no", "no_select") else None)
        if valid is not None:
            pending_writes.append((valid, site, node_id))

        # 被动健康上报：拿到有效判定算成功，robot/err 算失败
        if mgr is not None and port is not None:
            mgr.report(port, ok=(result in ("yes", "no", "no_select")))

        if len(pending_writes) >= 10:
            write_batch(pending_writes)
            pending_writes.clear()

        if result == "err":
            consecutive_err += 1
            if consecutive_err >= 5:
                s.close()
                s = new_session(domain, proxy_entry)
                consecutive_err = 0
        else:
            consecutive_err = 0

        with _stats_lock:
            _stats["done"] += 1
            if result == "yes":   _stats["yes"] += 1
            elif result in ("no","no_select"): _stats["no"] += 1
            else:                 _stats["err"] += 1
            done  = _stats["done"]
            total = _stats["total"]
            if done % 50 == 0 or done == total:
                pct = done / total * 100
                log.info(
                    f"  [{site}] {done}/{total} ({pct:.0f}%)  "
                    f"YES={_stats['yes']}  NO={_stats['no']}  "
                    f"ERR={_stats['err']}"
                )

        time.sleep(random.uniform(0.4, 0.8))

    if pending_writes:
        write_batch(pending_writes)
    s.close()


def probe_site(site: str, mgr, reset: bool):
    """动态 worker 版：worker 数随活跃 IP 集实时增减（L3）。"""
    domain = SITE_DOMAIN[site]
    nodes  = load_nodes(site, reset)
    if not nodes:
        log.info(f"[{site}] 无待检测节点")
        return

    q = Queue()
    for n in nodes:
        q.put(n)

    mgr.bootstrap()                 # 每站开跑前刷新活跃集，清上轮判死端口
    active = mgr.active_entries()
    log.info(f"\n=== {site}  {len(nodes)} 节点 / {len(active)} worker (动态池) ===")
    with _stats_lock:
        _stats.update({"done": 0, "yes": 0, "no": 0, "err": 0, "total": len(nodes)})

    workers = {}          # port -> Thread
    wlock = threading.Lock()

    def spawn(entry):
        port = entry["port"]
        with wlock:
            if port in workers and workers[port].is_alive():
                return
            t = threading.Thread(target=worker,
                                 args=(site, domain, q, entry, mgr), daemon=True)
            workers[port] = t
            t.start()

    # 初始：为每个活跃 IP 起一个 worker
    for e in active:
        spawn(e)
        time.sleep(0.3)

    # 动态回调：新 IP 上线 → spawn；掉线由 worker 自身 should_stop 退出
    mgr.start_monitor(on_add=lambda e: spawn(e), on_remove=None)

    # 主循环：等队列清空；若所有 worker 意外死光则等监控补线程
    idle_rounds = 0
    grace = mgr.check_interval // 5 + 6
    while not q.empty():
        time.sleep(5)
        with wlock:
            alive = sum(1 for t in workers.values() if t.is_alive())
        if alive == 0:
            idle_rounds += 1
            if idle_rounds > grace:
                log.warning(f"[{site}] 所有 worker 退出且队列未空，疑似全部 IP 失效，提前结束")
                break
        else:
            idle_rounds = 0

    # 队列已空，等剩余 worker 收尾 flush
    for t in list(workers.values()):
        t.join(timeout=30)
    mgr.stop_monitor()

    conn = sqlite3.connect(DB, timeout=15)
    yes_n = conn.execute("SELECT COUNT(*) FROM categories WHERE site=? AND na_valid=1", (site,)).fetchone()[0]
    no_n  = conn.execute("SELECT COUNT(*) FROM categories WHERE site=? AND na_valid=0", (site,)).fetchone()[0]
    null_n= conn.execute("SELECT COUNT(*) FROM categories WHERE site=? AND na_valid IS NULL AND node_id IS NOT NULL", (site,)).fetchone()[0]
    conn.close()

    log.info(f"[{site}] 完成 ✅  YES={yes_n}  NO={no_n}  未确认={null_n}\n")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", choices=["US","DE","JP"])
    parser.add_argument("--reset",   action="store_true")
    parser.add_argument("--check-interval", type=int, default=180,
                        help="动态池主动复核周期（秒，默认 180）")
    args = parser.parse_args()

    ensure_column()

    from proxy_pool_manager import PoolManager, ensure_proxy_ready
    ensure_proxy_ready()
    mgr = PoolManager(check_interval=args.check_interval)
    if not mgr.bootstrap():
        log.error("[pool] 无可用代理 IP，请先 python start_lb_proxy.py start")
        sys.exit(1)

    sites = [args.site] if args.site else ["US", "DE", "JP"]
    for site in sites:
        probe_site(site, mgr, args.reset)
    log.info("=== 全部完成 ===")


if __name__ == "__main__":
    main()
