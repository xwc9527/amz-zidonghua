"""
proxy_pool_manager.py — 动态 IP 池管理器（L2 + L3）

职责:
  - 从 lb_pid.json 读取 Mihomo 常驻的全部端口（含当前去重掉的，用于检测「恢复」）
  - 定期轻量校验所有端口的出口 IP（用 ipify，避免 ip-api 对本机高频限流）
  - 按出口 IP 去重，维护「当前活跃独立 IP 集」
  - 融合被动信号：worker 上报每端口真实业务请求成败，连续失败的端口主动复核
  - 通过回调通知调用方（爬虫）动态 spawn / 退出 worker

用法:
    mgr = PoolManager()
    mgr.bootstrap()                       # 首轮校验，拿到初始活跃集
    for entry in mgr.active_entries():    # 为每个活跃 IP 起一个 worker
        spawn_worker(entry, mgr)
    mgr.start_monitor(on_add=..., on_remove=...)
    ...
    mgr.stop_monitor()

worker 内部:
    mgr.report(port, ok=True/False)       # 上报每次请求成败（被动健康）
    if mgr.should_stop(port): break       # 端口被判死，优雅退出
"""
import json, os, subprocess, sys, threading, time, logging
from curl_cffi import requests as cr

BASE = os.path.dirname(os.path.abspath(__file__))
POOL_FILE = os.path.join(BASE, "data", "proxy_pool.json")
PID_FILE  = os.path.join(BASE, "data", "lb_pid.json")
POOL_MAX_AGE = 600  # proxy_pool.json 超过 10 分钟视为过期

log = logging.getLogger("pool_mgr")

# 不限流的纯文本取 IP 端点（备选，按序尝试）
IP_ENDPOINTS = [
    "https://api.ipify.org",
    "https://checkip.amazonaws.com",
    "https://ifconfig.me/ip",
]


def _mihomo_alive() -> bool:
    """检查 lb_pid.json 中记录的 Mihomo 进程是否存活。"""
    if not os.path.exists(PID_FILE):
        return False
    try:
        with open(PID_FILE) as f:
            pid = json.load(f).get("pid")
        if not pid:
            return False
        r = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"],
                           capture_output=True, text=True)
        return str(pid) in r.stdout
    except Exception:
        return False


def _pool_fresh() -> bool:
    """proxy_pool.json 存在且未过期（POOL_MAX_AGE 秒内）。"""
    if not os.path.exists(POOL_FILE):
        return False
    age = time.time() - os.path.getmtime(POOL_FILE)
    return age < POOL_MAX_AGE


def ensure_proxy_ready():
    """L4 自检：Mihomo 未运行或代理池过期 → 自动 refresh。"""
    if _mihomo_alive() and _pool_fresh():
        log.info("[L4] Mihomo 运行中，代理池新鲜，跳过 refresh")
        return
    reason = []
    if not _mihomo_alive():
        reason.append("Mihomo 未运行")
    if not _pool_fresh():
        reason.append("proxy_pool.json 过期或不存在")
    log.info(f"[L4] {' + '.join(reason)}，自动执行 refresh …")
    script = os.path.join(BASE, "start_lb_proxy.py")
    subprocess.run([sys.executable, script, "refresh"], check=False)


class PoolManager:
    def __init__(self, check_interval: int = 180, fail_threshold: int = 8,
                 verify_timeout: int = 10):
        self.check_interval = check_interval   # 主动复核周期（秒）
        self.fail_threshold = fail_threshold   # 连续失败多少次触发主动复核
        self.verify_timeout = verify_timeout
        self._lock = threading.Lock()
        self._active = {}        # exit_ip -> {"port","proxy","exit_ip"}
        self._port_ip = {}       # port -> exit_ip（最近一次校验）
        self._fails = {}         # port -> 连续失败计数（被动信号）
        self._stop_ports = set() # 被判死、worker 应退出的端口
        self._monitor_thread = None
        self._monitor_stop = threading.Event()

    # ── 端口来源 ────────────────────────────────────────────────
    def _all_ports(self) -> list[int]:
        """Mihomo 常驻的全部端口（含去重掉的，用于检测恢复）。"""
        try:
            with open(PID_FILE) as f:
                return json.load(f).get("ports", [])
        except Exception:
            # 回退到 proxy_pool.json
            try:
                with open(POOL_FILE, encoding="utf-8") as f:
                    return [e["port"] for e in json.load(f)]
            except Exception:
                return []

    # ── 单端口出口 IP 校验 ──────────────────────────────────────
    def _verify_port(self, port: int) -> str | None:
        proxy = f"http://127.0.0.1:{port}"
        px = {"http": proxy, "https": proxy}
        for ep in IP_ENDPOINTS:
            try:
                r = cr.get(ep, proxies=px, verify=False,
                           timeout=self.verify_timeout, impersonate="chrome124")
                ip = r.text.strip()
                if ip and len(ip) <= 45 and not ip.startswith(("192.", "10.", "127.")):
                    return ip
            except Exception:
                continue
        return None

    # ── 一轮全量校验 + 去重 ─────────────────────────────────────
    def _scan(self) -> dict:
        """并发校验所有端口，按出口 IP 去重，返回 {exit_ip: entry}。"""
        ports = self._all_ports()
        results = {}
        rlock = threading.Lock()

        def _chk(port):
            ip = self._verify_port(port)
            if ip:
                with rlock:
                    results[port] = ip

        ts = [threading.Thread(target=_chk, args=(p,), daemon=True) for p in ports]
        for t in ts:
            t.start(); time.sleep(0.05)
        for t in ts:
            t.join(timeout=self.verify_timeout + 5)

        # 按出口 IP 去重：每个 IP 保留最小端口号
        dedup = {}
        for port in sorted(results):
            ip = results[port]
            if ip not in dedup:
                dedup[ip] = {"port": port, "proxy": f"http://127.0.0.1:{port}",
                             "exit_ip": ip}
        with self._lock:
            self._port_ip = dict(results)
        return dedup

    # ── 启动首轮 ────────────────────────────────────────────────
    def bootstrap(self) -> list:
        dedup = self._scan()
        with self._lock:
            self._active = dedup
            self._stop_ports.clear()   # 新一轮，清掉上轮判死的端口
            self._fails.clear()
        log.info(f"[pool_mgr] bootstrap: {len(self._all_ports())} 端口 → "
                 f"{len(dedup)} 独立IP")
        return self.active_entries()

    def active_entries(self) -> list:
        with self._lock:
            return list(self._active.values())

    # ── 被动健康：worker 上报 ───────────────────────────────────
    def report(self, port: int, ok: bool):
        with self._lock:
            if ok:
                self._fails[port] = 0
            else:
                self._fails[port] = self._fails.get(port, 0) + 1

    def should_stop(self, port: int) -> bool:
        with self._lock:
            return port in self._stop_ports

    # ── 主动复核 + diff，驱动回调 ───────────────────────────────
    def _refresh_once(self, on_add, on_remove):
        # 被动信号：找出连续失败超阈值的活跃端口，重点复核
        with self._lock:
            suspect = {ip: e for ip, e in self._active.items()
                       if self._fails.get(e["port"], 0) >= self.fail_threshold}

        dedup = self._scan()  # 全量复核（含死端口是否复活）

        with self._lock:
            old_ips = set(self._active)
            new_ips = set(dedup)

            added   = new_ips - old_ips
            removed = old_ips - new_ips

            # 被动判死：suspect 里这轮仍没校验出 IP 的，强制移除
            for ip, e in suspect.items():
                if e["port"] not in self._port_ip and ip in self._active:
                    removed.add(ip)

            for ip in removed:
                e = self._active.get(ip)
                if e:
                    self._stop_ports.add(e["port"])
                    self._active.pop(ip, None)
            for ip in added:
                self._active[ip] = dedup[ip]
                self._stop_ports.discard(dedup[ip]["port"])
                self._fails[dedup[ip]["port"]] = 0

            added_entries   = [dedup[ip] for ip in added]
            removed_entries = [{"exit_ip": ip} for ip in removed]

        for e in added_entries:
            log.info(f"[pool_mgr] + 新增 IP {e['exit_ip']} (port {e['port']})")
            if on_add:
                on_add(e)
        for e in removed_entries:
            log.info(f"[pool_mgr] - 移除 IP {e['exit_ip']}")
            if on_remove:
                on_remove(e)
        if added_entries or removed_entries:
            log.info(f"[pool_mgr] 活跃 IP: {len(self._active)}")

    # ── 监控线程 ────────────────────────────────────────────────
    def start_monitor(self, on_add=None, on_remove=None):
        def _loop():
            while not self._monitor_stop.wait(self.check_interval):
                try:
                    self._refresh_once(on_add, on_remove)
                except Exception as ex:
                    log.warning(f"[pool_mgr] 复核异常: {ex}")
        self._monitor_thread = threading.Thread(target=_loop, daemon=True)
        self._monitor_thread.start()
        log.info(f"[pool_mgr] 监控线程启动（周期 {self.check_interval}s）")

    def stop_monitor(self):
        self._monitor_stop.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
