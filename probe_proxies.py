"""
probe_proxies.py — 通过 Clash API 并发测试所有代理节点延迟
用法: python probe_proxies.py
输出: 按延迟排序的可用节点列表，并保存到 data/proxy_pool.json
"""
import json, sys, time, threading, urllib.request, urllib.parse
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

CLASH_API   = "http://127.0.0.1:9097"
TEST_URL    = "http://cp.cloudflare.com/generate_204"
TIMEOUT_MS  = 5000          # 5秒超时
MAX_WORKERS = 20            # 并发测试线程数
OUTPUT      = "data/probe_results.json"

# 跳过这些非真实节点
SKIP_TYPES  = {"Selector", "URLTest", "LoadBalance", "Fallback", "Relay"}
SKIP_NAMES  = {"DIRECT", "REJECT", "GLOBAL"}

def clash_get(path):
    req = urllib.request.Request(f"{CLASH_API}{path}",
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())

def test_node(name):
    """调用 Clash API 测单个节点延迟，返回 ms 或 None（超时/失败）"""
    encoded = urllib.parse.quote(name, safe="")
    url = f"{CLASH_API}/proxies/{encoded}/delay?timeout={TIMEOUT_MS}&url={urllib.parse.quote(TEST_URL)}"
    try:
        req = urllib.request.Request(url, headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=TIMEOUT_MS/1000 + 2) as r:
            data = json.loads(r.read())
            return data.get("delay")  # ms，0 或缺失表示失败
    except Exception:
        return None

def main():
    print(f"[probe] 连接 Clash API: {CLASH_API}", flush=True)
    try:
        all_proxies = clash_get("/proxies")["proxies"]
    except Exception as e:
        print(f"[错误] 无法连接 Clash API: {e}")
        print("请确认 Clash Verge 正在运行，且控制端口为 9097")
        sys.exit(1)

    # 筛选真实节点
    real_nodes = []
    for name, info in all_proxies.items():
        if name in SKIP_NAMES:
            continue
        if info.get("type") in SKIP_TYPES:
            continue
        real_nodes.append({"name": name, "type": info.get("type", "?")})

    print(f"[probe] 发现真实节点: {len(real_nodes)} 个，开始并发测速（{MAX_WORKERS} workers）...\n")

    results = {}
    lock = threading.Lock()
    done = [0]

    def worker(node):
        name = node["name"]
        delay = test_node(name)
        with lock:
            done[0] += 1
            results[name] = {"type": node["type"], "delay": delay}
            status = f"{delay}ms" if delay else "超时/失败"
            print(f"  [{done[0]:3d}/{len(real_nodes)}] {status:12s}  {name}", flush=True)

    # 分批并发
    threads = []
    for node in real_nodes:
        t = threading.Thread(target=worker, args=(node,), daemon=True)
        threads.append(t)
        t.start()
        if len([t for t in threads if t.is_alive()]) >= MAX_WORKERS:
            time.sleep(0.1)

    for t in threads:
        t.join()

    # 统计结果
    available = {k: v for k, v in results.items() if v["delay"]}
    failed    = {k: v for k, v in results.items() if not v["delay"]}

    print(f"\n{'='*60}")
    print(f"测速完成: {len(available)} 可用 / {len(failed)} 不可用 / {len(real_nodes)} 总计")
    print(f"{'='*60}\n")

    # 按延迟排序并展示
    sorted_ok = sorted(available.items(), key=lambda x: x[1]["delay"])

    print(f"{'排名':>4}  {'延迟':>7}  {'类型':>10}  节点名")
    print("-" * 70)
    for i, (name, info) in enumerate(sorted_ok, 1):
        print(f"{i:4d}  {info['delay']:>5}ms  {info['type']:>10}  {name}")

    if failed:
        print(f"\n不可用节点 ({len(failed)} 个):")
        for name, info in failed.items():
            print(f"  ✗  {info['type']:>10}  {name}")

    # 保存到文件
    import os
    os.makedirs("data", exist_ok=True)
    pool = [
        {"name": name, "type": info["type"], "delay": info["delay"],
         "proxy": f"http://127.0.0.1:7897"}   # 占位，多实例方案再填实际端口
        for name, info in sorted_ok
    ]
    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(pool, f, ensure_ascii=False, indent=2)
    print(f"\n[probe] 可用节点已保存: {OUTPUT} ({len(pool)} 条)")

if __name__ == "__main__":
    main()
