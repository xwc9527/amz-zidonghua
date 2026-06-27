"""
test_search_api.py — 批量测试 Amazon DE 搜索接口（按最新到货排序）
对本地 DB 中所有 DE 类目节点发起请求，检查：
  1. 是否返回商品列表（非 CAPTCHA / 非空）
  2. 商品数量
  3. 翻页信息（总页数）
"""

import json, os, re, sys, time, random, sqlite3, threading
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

import requests, urllib3
urllib3.disable_warnings()
from bs4 import BeautifulSoup

from config import (
    HEADERS, DATA_DIR, DB_FILE, PROXY_POOL_FILE,
    PROXY_ENABLED, PROXY_VERIFY, get_marketplace,
)

_mp = get_marketplace("DE")
DOMAIN = _mp["domain"]
LANG = _mp["lang"]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
]

# ── 代理池 ──
class ProxyPool:
    def __init__(self):
        self._q = Queue()
        self._all = []
        if PROXY_ENABLED and os.path.exists(PROXY_POOL_FILE):
            with open(PROXY_POOL_FILE, encoding="utf-8") as f:
                entries = json.load(f)
            for p in entries:
                self._q.put(p)
                self._all.append(p)
            print(f"[pool] 加载 {len(entries)} 个代理端口")
        else:
            print("[pool] 代理未启用，使用直连")

    @property
    def size(self):
        return len(self._all)

    def acquire(self, timeout=30):
        return self._q.get(timeout=timeout)

    def release(self, entry):
        self._q.put(entry)

pool = ProxyPool()

# ── 构建搜索 URL ──
def build_search_url(node_id: str, page: int = 1) -> str:
    url = f"{DOMAIN}/s?rh=n%3A{node_id}&s=date-desc-rank&language=en"
    if page > 1:
        url += f"&page={page}"
    return url

# ── 解析搜索结果页 ──
def parse_search_page(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # 检查是否 CAPTCHA
    if "captcha" in html.lower() or "Type the characters" in html or "Klicke auf die Schaltfläche" in html:
        return {"status": "captcha", "count": 0, "has_next": False, "total_text": ""}

    # 商品卡片
    cards = soup.select('[data-component-type="s-search-result"]')
    asin_count = 0
    for card in cards:
        asin = card.get("data-asin", "").strip()
        if asin:
            asin_count += 1

    # 翻页
    has_next = bool(soup.select_one(".s-pagination-next:not(.s-pagination-disabled)"))

    # 总结果数文本
    total_text = ""
    total_el = soup.select_one(".a-section.a-spacing-small span")
    if total_el:
        total_text = total_el.get_text(strip=True)

    # 无结果检测
    no_results = soup.select_one("#search .a-spacing-large .a-text-center, .s-no-outline .a-spacing-medium")
    if no_results and "keine Ergebnisse" in no_results.get_text().lower():
        return {"status": "no_results", "count": 0, "has_next": False, "total_text": ""}

    if asin_count == 0:
        return {"status": "empty", "count": 0, "has_next": has_next, "total_text": total_text}

    return {"status": "ok", "count": asin_count, "has_next": has_next, "total_text": total_text}

# ── 发请求（带重试） ──
def fetch_search(session, url, retries=2):
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=15, verify=PROXY_VERIFY)
            if r.status_code == 200:
                return r.text
            if r.status_code == 503:
                time.sleep(10 + random.uniform(0, 5))
                continue
            if r.status_code == 429:
                time.sleep(30 + random.uniform(0, 15))
                continue
            return None
        except requests.RequestException:
            time.sleep(3 + random.uniform(0, 2))
    return None

# ── 加载所有 DE 节点 ──
def load_de_nodes():
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT node_id, name, depth FROM categories WHERE site='DE' AND depth > 0 ORDER BY depth, name"
    ).fetchall()
    conn.close()
    return [{"node_id": r["node_id"], "name": r["name"], "depth": r["depth"]} for r in rows]

# ── Worker ──
lock = threading.Lock()
results = []
stats = {"ok": 0, "captcha": 0, "empty": 0, "no_results": 0, "error": 0, "total": 0}

def worker(worker_id, task_q):
    proxy_entry = pool.acquire() if pool.size > 0 else None
    session = requests.Session()
    ua = USER_AGENTS[worker_id % len(USER_AGENTS)]
    session.headers.update({
        **HEADERS,
        "User-Agent": ua,
        "Accept-Language": LANG,
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    })
    if proxy_entry:
        session.proxies.update({"http": proxy_entry["proxy"], "https": proxy_entry["proxy"]})

    # 热身：先访问首页
    try:
        session.get(f"{DOMAIN}/", timeout=10, verify=PROXY_VERIFY)
        time.sleep(1)
    except Exception:
        pass

    while True:
        try:
            node = task_q.get(timeout=3)
        except Empty:
            break

        node_id = node["node_id"]
        name = node["name"]
        depth = node["depth"]
        url = build_search_url(node_id)

        html = fetch_search(session, url)
        time.sleep(random.uniform(1.5, 3.0))

        with lock:
            stats["total"] += 1
            if html is None:
                stats["error"] += 1
                results.append({"node_id": node_id, "name": name, "depth": depth, "status": "error"})
                if stats["total"] % 50 == 0:
                    _print_progress()
                continue

            parsed = parse_search_page(html)
            status = parsed["status"]
            stats[status] = stats.get(status, 0) + 1
            results.append({
                "node_id": node_id, "name": name, "depth": depth,
                "status": status, "count": parsed["count"],
                "has_next": parsed["has_next"], "total_text": parsed["total_text"],
            })
            if stats["total"] % 50 == 0:
                _print_progress()

    if proxy_entry:
        pool.release(proxy_entry)

def _print_progress():
    t = stats["total"]
    print(f"  [{t}/{total_nodes}] ok={stats['ok']} captcha={stats['captcha']} "
          f"empty={stats['empty']} no_results={stats.get('no_results',0)} error={stats['error']}", flush=True)

# ── Main ──
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=0, help="只测试N个节点（0=全部）")
    parser.add_argument("--workers", type=int, default=0, help="worker数（0=代理池大小）")
    parser.add_argument("--depth", type=int, default=0, help="只测试指定层级（0=全部）")
    args = parser.parse_args()

    nodes = load_de_nodes()
    if args.depth > 0:
        nodes = [n for n in nodes if n["depth"] == args.depth]
    if args.sample > 0:
        random.shuffle(nodes)
        nodes = nodes[:args.sample]

    total_nodes = len(nodes)
    num_workers = args.workers if args.workers > 0 else max(pool.size, 1)

    print(f"=== DE 搜索接口测试 ===")
    print(f"节点数: {total_nodes}, Worker数: {num_workers}")
    print(f"URL模板: {DOMAIN}/s?rh=n%3A{{node_id}}&s=date-desc-rank")
    print()

    task_q = Queue()
    for n in nodes:
        task_q.put(n)

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=num_workers) as exe:
        futs = [exe.submit(worker, i, task_q) for i in range(num_workers)]
        for f in as_completed(futs):
            f.result()

    elapsed = time.time() - t0
    print(f"\n=== 测试完成 ({elapsed:.0f}s) ===")
    print(f"总计: {stats['total']}")
    print(f"  成功(有商品): {stats['ok']}")
    print(f"  CAPTCHA:      {stats['captcha']}")
    print(f"  空页面:       {stats['empty']}")
    print(f"  无结果:       {stats.get('no_results', 0)}")
    print(f"  请求失败:     {stats['error']}")

    # 按层级统计
    print("\n--- 按层级 ---")
    for d in range(1, 8):
        d_results = [r for r in results if r["depth"] == d]
        if not d_results:
            continue
        ok = sum(1 for r in d_results if r["status"] == "ok")
        cap = sum(1 for r in d_results if r["status"] == "captcha")
        emp = sum(1 for r in d_results if r["status"] in ("empty", "no_results"))
        err = sum(1 for r in d_results if r["status"] == "error")
        print(f"  L{d}: {len(d_results)}节点 → ok={ok} captcha={cap} empty/no_results={emp} error={err}")

    # 保存详细结果
    out_file = os.path.join(DATA_DIR, "search_api_test_results.json")
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump({"stats": stats, "results": results}, f, ensure_ascii=False, indent=2)
    print(f"\n详细结果: {out_file}")

    # 显示有商品的节点中商品数分布
    ok_results = [r for r in results if r["status"] == "ok"]
    if ok_results:
        counts = [r["count"] for r in ok_results]
        print(f"\n--- 有商品节点的商品数分布 ---")
        print(f"  最少: {min(counts)}, 最多: {max(counts)}, 平均: {sum(counts)/len(counts):.1f}")
        has_next_count = sum(1 for r in ok_results if r.get("has_next"))
        print(f"  有下一页: {has_next_count}/{len(ok_results)}")
