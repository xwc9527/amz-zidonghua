"""
start_lb_proxy.py — 启动单个 Mihomo 实例，每个节点独立端口（listeners 模式）
输出: data/proxy_pool.json（端口池，供爬虫消费者取用）
用法:
  python start_lb_proxy.py start [--max N]   启动（最多 N 个节点，默认8）
  python start_lb_proxy.py stop              停止
  python start_lb_proxy.py check             检测当前池健康状态（不重启）
  python start_lb_proxy.py refresh           重新探测 + 热更新端口池
"""
import json, os, sys, subprocess, time, shutil, threading
import urllib.request, urllib.parse, argparse
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

try:
    import yaml
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pyyaml", "-q"])
    import yaml

try:
    import requests, urllib3
    urllib3.disable_warnings()
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "-q"])
    import requests, urllib3
    urllib3.disable_warnings()

MIHOMO      = r"C:\Program Files\Clash Verge\verge-mihomo.exe"
CLASH_DATA  = r"C:\Users\47763\AppData\Roaming\io.github.clash-verge-rev.clash-verge-rev"
PROFILE     = rf"{CLASH_DATA}\profiles\RNgwOpG3Vhv0.yaml"
PROBE_FILE  = "data/probe_results.json"
LB_DIR      = "data/lb_instance"
PID_FILE    = "data/lb_pid.json"
POOL_FILE   = "data/proxy_pool.json"
CTRL_PORT   = 19897
BASE_PORT   = 18001
MAX_PROXIES = 0       # 0 = 不限，取所有可用节点
SKIP_PROTO  = {"hysteria"}   # 仅砍 hysteria v1 老协议；hy2 配置完整且 server 独立，放行（L0）
SKIP_NAMES  = {"剩余流量：866.47 GB", "套餐到期：长期有效", "PASS", "REJECT-DROP", "COMPATIBLE"}
DB_FILES    = ["Country.mmdb", "geoip.dat", "geosite.dat"]
CDN_SERVERS = {
    "downloadcfpro.xn--ghq880n3na965a.com",
    "unamecf2.xn--ghqu5fm27b67w.com",
    "unamecf.xn--ghqu5fm27b67w.com",
}
CHECK_URL   = "http://ip-api.com/json?fields=query,country"
CHECK_TIMEOUT = 12
CLASH_VERGE_API = "http://127.0.0.1:9097"


# ── 工具 ──────────────────────────────────────────────────────────────

def _get_local_active_nodes() -> set[str]:
    """查询本机 Clash Verge 当前所有规则组选中的节点名，供排除。"""
    try:
        req = urllib.request.Request(f"{CLASH_VERGE_API}/proxies",
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as r:
            data = json.loads(r.read())
        active = set()
        for name, info in data.get("proxies", {}).items():
            if info.get("type") in ("Selector", "URLTest", "Fallback") and info.get("now"):
                active.add(info["now"])
        return active
    except Exception as e:
        print(f"[warn] 无法查询 Clash Verge 当前节点: {e}")
        return set()


def _load_nodes(max_n: int = MAX_PROXIES) -> list:
    """从 probe_results.json + profile 加载可用节点，按延迟排序。max_n=0 表示不限。"""
    with open(PROBE_FILE, encoding="utf-8") as f:
        probe = json.load(f)
    with open(PROFILE, encoding="utf-8") as f:
        profile_map = {p["name"]: p for p in yaml.safe_load(f).get("proxies", [])}

    nodes = []
    for n in sorted(probe, key=lambda x: x.get("delay") or 9999):
        if n["name"] in SKIP_NAMES or n.get("type", "").lower() in SKIP_PROTO:
            continue
        cfg = profile_map.get(n["name"])
        if not cfg or cfg.get("server", "") in CDN_SERVERS:
            continue
        cfg = dict(cfg)
        cfg["_delay"] = n.get("delay", 9999)
        nodes.append(cfg)
        if max_n > 0 and len(nodes) >= max_n:
            break
    return nodes


def _verify_ports(pool: list, timeout: int = CHECK_TIMEOUT) -> list[dict]:
    """
    并发检测 pool 中每个端口的可用性，返回通过的条目（附 exit_ip/country）。
    """
    lock = threading.Lock()
    alive = []

    # 获取本机公网 IP，用于过滤直连节点
    local_ip = ""
    try:
        r = requests.get("http://ip-api.com/json?fields=query", timeout=5)
        local_ip = r.json().get("query", "")
        print(f"  [info] 本机公网IP: {local_ip}")
    except Exception:
        pass

    def _check(entry):
        px = {"http": entry["proxy"], "https": entry["proxy"]}
        try:
            r = requests.get(CHECK_URL, proxies=px, verify=False, timeout=timeout)
            d = r.json()
            ip = d.get("query", "")
            country = d.get("country", "?")
            if ip and not ip.startswith("192.") and not ip.startswith("10.") and ip != local_ip:
                with lock:
                    entry = dict(entry)
                    entry["exit_ip"] = ip
                    entry["country"] = country
                    alive.append(entry)
                print(f"  ✅ port {entry['port']}  {ip:<18} {country:<12} {entry['name']}")
            else:
                print(f"  ❌ port {entry['port']}  IP异常({ip}) {entry['name']}")
        except Exception as e:
            print(f"  ❌ port {entry['port']}  {e}  {entry['name']}")

    threads = [threading.Thread(target=_check, args=(p,)) for p in pool]
    for t in threads:
        t.start()
        time.sleep(0.2)
    for t in threads:
        t.join()

    alive.sort(key=lambda x: x["port"])

    # 按出口 IP 去重：同一出口 IP 只保留延迟最低的节点
    seen_ips = {}
    for entry in sorted(alive, key=lambda x: x.get("delay") or 9999):
        ip = entry.get("exit_ip", "")
        if ip and ip not in seen_ips:
            seen_ips[ip] = entry
    deduped = sorted(seen_ips.values(), key=lambda x: x["port"])
    if len(deduped) < len(alive):
        print(f"  [去重] {len(alive)} 个端口 → {len(deduped)} 个独立出口IP")
    return deduped


def _build_mihomo_cfg(nodes: list) -> dict:
    """生成 Mihomo listeners 配置。"""
    groups, listeners = [], []
    for i, n in enumerate(nodes):
        gname = f"node_{i}"
        port = BASE_PORT + i
        groups.append({"name": gname, "type": "select", "proxies": [n["name"]]})
        listeners.append({"name": f"http-in-{i}", "type": "http", "port": port, "proxy": gname})
    return {
        "mixed-port":          0,
        "allow-lan":           False,
        "mode":                "rule",
        "log-level":           "warning",
        "ipv6":                False,
        "external-controller": f"127.0.0.1:{CTRL_PORT}",
        "proxies":             nodes,
        "proxy-groups":        groups,
        "listeners":           listeners,
        "rules":               ["MATCH,DIRECT"],
    }


def _is_running() -> int | None:
    """返回正在运行的 PID，否则 None。"""
    if not os.path.exists(PID_FILE):
        return None
    try:
        with open(PID_FILE) as f:
            info = json.load(f)
        pid = info["pid"]
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True, text=True
        )
        return pid if str(pid) in result.stdout else None
    except Exception:
        return None


# ── 命令 ──────────────────────────────────────────────────────────────

def cmd_start(max_n: int = MAX_PROXIES):
    # 先停已有实例
    pid = _is_running()
    if pid:
        print(f"[proxy] 检测到运行中实例 PID={pid}，先停止...")
        cmd_stop()
        time.sleep(3)

    nodes = _load_nodes(max_n)

    # 排除本机 Clash Verge 正在使用的节点
    local_active = _get_local_active_nodes()
    if local_active:
        before = len(nodes)
        excluded = [n["name"] for n in nodes if n["name"] in local_active]
        nodes = [n for n in nodes if n["name"] not in local_active]
        if excluded:
            print(f"[proxy] 排除本机正在使用的 {len(excluded)} 个节点: {excluded}")
        print(f"[proxy] 排除后剩余: {len(nodes)} 个（原 {before} 个）")

    limit_str = f"上限 {max_n}" if max_n > 0 else "不限"
    print(f"[proxy] 候选节点: {len(nodes)} 个（{limit_str}）")
    if not nodes:
        print("[proxy] 无可用节点，请先运行 probe_proxies.py")
        return

    os.makedirs(LB_DIR, exist_ok=True)
    for db in DB_FILES:
        src = os.path.join(CLASH_DATA, db)
        dst = os.path.join(LB_DIR, db)
        if os.path.exists(src) and not os.path.exists(dst):
            shutil.copy2(src, dst)

    # 清理 _delay 临时字段再写配置
    clean_nodes = [{k: v for k, v in n.items() if k != "_delay"} for n in nodes]
    cfg = _build_mihomo_cfg(clean_nodes)

    cfg_path = os.path.join(LB_DIR, "config.yaml")
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)

    log_file = os.path.abspath(os.path.join(LB_DIR, "mihomo.log"))
    log_fh = open(log_file, "w")
    proc = subprocess.Popen(
        [MIHOMO, "-d", os.path.abspath(LB_DIR)],
        stdout=log_fh, stderr=log_fh,
        creationflags=0x01000000 | subprocess.CREATE_NO_WINDOW,
    )
    with open(PID_FILE, "w") as f:
        json.dump({"pid": proc.pid, "ports": [BASE_PORT + i for i in range(len(nodes))]}, f)
    print(f"[proxy] 实例已启动 PID={proc.pid}，等待端口就绪...")
    time.sleep(5)

    if proc.poll() is not None:
        log_fh.close()
        with open(log_file, encoding="utf-8", errors="replace") as f:
            print("[FATAL] 启动失败:\n" + f.read())
        return

    # 构建候选 pool 条目
    raw_pool = [
        {"name": n["name"], "port": BASE_PORT + i,
         "proxy": f"http://127.0.0.1:{BASE_PORT + i}",
         "delay": n.get("_delay")}
        for i, n in enumerate(nodes)
    ]

    # 健康验证
    print(f"\n[proxy] 健康检测 {len(raw_pool)} 个端口...")
    verified = _verify_ports(raw_pool)

    with open(POOL_FILE, "w", encoding="utf-8") as f:
        json.dump(verified, f, ensure_ascii=False, indent=2)

    unique_ips = {p["exit_ip"] for p in verified}
    print(f"\n[结果] {len(verified)}/{len(nodes)} 端口可用  {len(unique_ips)} 个独立 IP")
    print(f"[proxy] 端口池已写入: {POOL_FILE}")

    if len(verified) == 0:
        print("[WARN] 无可用端口，请检查节点状态")


def cmd_stop():
    pid = _is_running()
    if not pid:
        # 尝试读文件里的 PID 强杀
        if os.path.exists(PID_FILE):
            with open(PID_FILE) as f:
                info = json.load(f)
            subprocess.run(["taskkill", "/PID", str(info["pid"]), "/F"],
                           capture_output=True)
            os.remove(PID_FILE)
            print(f"[proxy] 已强制停止 PID={info['pid']}")
        else:
            print("[proxy] 无运行记录")
        return
    subprocess.run(["taskkill", "/PID", str(pid), "/F"], capture_output=True)
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)
    print(f"[proxy] 已停止 PID={pid}")


def cmd_check():
    """检测 proxy_pool.json 中当前端口的健康状态，更新文件（不重启 Mihomo）。"""
    if not os.path.exists(POOL_FILE):
        print("[check] proxy_pool.json 不存在，请先 start")
        return
    with open(POOL_FILE, encoding="utf-8") as f:
        pool = json.load(f)

    print(f"[check] 检测 {len(pool)} 个端口...")
    verified = _verify_ports(pool)
    dead = len(pool) - len(verified)

    with open(POOL_FILE, "w", encoding="utf-8") as f:
        json.dump(verified, f, ensure_ascii=False, indent=2)

    unique_ips = {p["exit_ip"] for p in verified}
    print(f"\n[结果] 存活 {len(verified)}/{len(pool)}  死亡 {dead}  独立IP {len(unique_ips)}")
    if dead > 0:
        print("[check] 建议运行 refresh 重新探测并同步端口")
    return verified


def cmd_refresh(max_n: int = MAX_PROXIES):
    """重新探测节点延迟 → 热更新：停旧实例 → 启新实例 → 更新 pool。"""
    print("[refresh] 重新探测节点延迟...")
    subprocess.run([sys.executable, "probe_proxies.py"], check=False)
    print("[refresh] 探测完成，重启代理实例...")
    cmd_start(max_n)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mihomo 代理池管理")
    parser.add_argument("cmd", choices=["start", "stop", "check", "refresh"],
                        nargs="?", default="start")
    parser.add_argument("--max", type=int, default=MAX_PROXIES,
                        help=f"最大代理节点数（默认 {MAX_PROXIES}）")
    args = parser.parse_args()

    if args.cmd == "start":
        cmd_start(args.max)
    elif args.cmd == "stop":
        cmd_stop()
    elif args.cmd == "check":
        cmd_check()
    elif args.cmd == "refresh":
        cmd_refresh(args.max)
