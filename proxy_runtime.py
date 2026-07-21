"""
proxy_runtime.py — 独立 Mihomo 启停、端口监听、PID 归属与异常清理。

只管理本系统启动的独立实例，不触碰用户主 Clash（7897）。
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field

import yaml

from config import (
    CLASH_VERGE_HOME,
    DATA_DIR,
    MIHOMO_BIN,
    PROXY_BASE_PORT,
    PROXY_CTRL_PORT,
    PROXY_DAEMON_BLUE_GREEN_BASE_PORT,
    PROXY_GEO_DB_FILES,
    PROXY_LB_DIR,
    PROXY_PID_FILE,
    PROXY_PORT_RANGE_END,
)

log = logging.getLogger("proxy_runtime")

_job_handle_lock = threading.Lock()
_job_handles: dict[int, int] = {}


def _assign_kill_on_close_job(proc: subprocess.Popen) -> None:
    """Windows 下把 Mihomo 放入由 daemon 持有的 kill-on-close Job。"""
    if os.name != "nt":
        return
    import ctypes
    from ctypes import wintypes

    class IO_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("ReadOperationCount", ctypes.c_ulonglong),
            ("WriteOperationCount", ctypes.c_ulonglong),
            ("OtherOperationCount", ctypes.c_ulonglong),
            ("ReadTransferCount", ctypes.c_ulonglong),
            ("WriteTransferCount", ctypes.c_ulonglong),
            ("OtherTransferCount", ctypes.c_ulonglong),
        ]

    class BASIC_LIMITS(ctypes.Structure):
        _fields_ = [
            ("PerProcessUserTimeLimit", ctypes.c_longlong),
            ("PerJobUserTimeLimit", ctypes.c_longlong),
            ("LimitFlags", wintypes.DWORD),
            ("MinimumWorkingSetSize", ctypes.c_size_t),
            ("MaximumWorkingSetSize", ctypes.c_size_t),
            ("ActiveProcessLimit", wintypes.DWORD),
            ("Affinity", ctypes.c_size_t),
            ("PriorityClass", wintypes.DWORD),
            ("SchedulingClass", wintypes.DWORD),
        ]

    class EXTENDED_LIMITS(ctypes.Structure):
        _fields_ = [
            ("BasicLimitInformation", BASIC_LIMITS),
            ("IoInfo", IO_COUNTERS),
            ("ProcessMemoryLimit", ctypes.c_size_t),
            ("JobMemoryLimit", ctypes.c_size_t),
            ("PeakProcessMemoryUsed", ctypes.c_size_t),
            ("PeakJobMemoryUsed", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
    ]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
    kernel32.CloseHandle.restype = wintypes.BOOL

    job = kernel32.CreateJobObjectW(None, None)
    if not job:
        raise ctypes.WinError(ctypes.get_last_error())
    info = EXTENDED_LIMITS()
    info.BasicLimitInformation.LimitFlags = 0x00002000  # KILL_ON_JOB_CLOSE
    if not kernel32.SetInformationJobObject(job, 9, ctypes.byref(info), ctypes.sizeof(info)):
        error = ctypes.get_last_error()
        kernel32.CloseHandle(job)
        raise ctypes.WinError(error)
    if not kernel32.AssignProcessToJobObject(job, wintypes.HANDLE(int(proc._handle))):
        error = ctypes.get_last_error()
        kernel32.CloseHandle(job)
        raise ctypes.WinError(error)
    with _job_handle_lock:
        _job_handles[int(proc.pid)] = int(job)


def _release_job_handle(pid: int) -> None:
    if os.name != "nt":
        return
    with _job_handle_lock:
        handle = _job_handles.pop(int(pid), None)
    if handle:
        import ctypes
        ctypes.WinDLL("kernel32", use_last_error=True).CloseHandle(handle)


@dataclass
class RuntimeResult:
    ok: bool
    pid: int | None = None
    ports: list[int] = field(default_factory=list)
    config_path: str = ""
    error: str = ""
    error_code: str = ""
    log_tail: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def _atomic_write_json(path: str, data: dict | list) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.{os.getpid()}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _pid_alive(pid: int) -> bool:
    if not pid or pid <= 0:
        return False
    if os.name == "nt":
        # 不再为每次心跳启动 tasklist.exe。守护循环会高频调用本函数，
        # tasklist 即使 CREATE_NO_WINDOW 仍会制造大量短命 conhost。
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel32.OpenProcess.restype = wintypes.HANDLE
        kernel32.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        kernel32.GetExitCodeProcess.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.OpenProcess(0x1000, False, int(pid))  # QUERY_LIMITED_INFORMATION
        if not handle:
            return False
        try:
            exit_code = wintypes.DWORD()
            return bool(kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))) and exit_code.value == 259
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def _port_listening(port: int, host: str = "127.0.0.1", timeout: float = 0.5) -> bool:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        return s.connect_ex((host, port)) == 0
    except OSError:
        return False
    finally:
        s.close()


def read_pid_record(pid_file: str | None = None) -> dict | None:
    path = pid_file or PROXY_PID_FILE
    if not os.path.isfile(path):
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        return data
    except Exception as e:
        log.warning("[runtime] 读取 PID 文件失败: %s", e)
        return None


def pid_alive(pid: int) -> bool:
    """公开包装：供其他模块（如 proxy_daemon）复用同一套存活判定，避免重复实现。"""
    return _pid_alive(pid)


def owned_mihomo_running(pid_file: str | None = None) -> int | None:
    rec = read_pid_record(pid_file)
    if not rec:
        return None
    pid = rec.get("pid")
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return None
    return pid if _pid_alive(pid) else None


def build_mihomo_config(
    nodes: list[dict],
    base_port: int | None = None,
    ctrl_port: int | None = None,
    ports: list[int] | None = None,
) -> dict:
    """构建 Mihomo 配置。

    ports 若提供则按节点一一对应使用稳定端口（不因排序变化而重排）；
    否则退回 base+i 顺序分配。
    """
    base = PROXY_BASE_PORT if base_port is None else base_port
    ctrl = PROXY_CTRL_PORT if ctrl_port is None else ctrl_port
    if ports is not None and len(ports) != len(nodes):
        raise ValueError(f"ports 长度({len(ports)})与 nodes({len(nodes)})不一致")
    groups, listeners = [], []
    clean_nodes = []
    for i, n in enumerate(nodes):
        node = {k: v for k, v in n.items() if not str(k).startswith("_")}
        clean_nodes.append(node)
        gname = f"node_{i}"
        port = ports[i] if ports is not None else base + i
        groups.append({"name": gname, "type": "select", "proxies": [node["name"]]})
        listeners.append({
            "name": f"http-in-{port}",
            "type": "http",
            "port": port,
            "proxy": gname,
        })
    return {
        "mixed-port": 0,
        "allow-lan": False,
        "mode": "rule",
        "log-level": "warning",
        "ipv6": False,
        "external-controller": f"127.0.0.1:{ctrl}",
        "proxies": clean_nodes,
        "proxy-groups": groups,
        "listeners": listeners,
        "rules": ["MATCH,DIRECT"],
    }


def _copy_geo_dbs(lb_dir: str) -> None:
    if not CLASH_VERGE_HOME:
        return
    for name in PROXY_GEO_DB_FILES:
        src = os.path.join(CLASH_VERGE_HOME, name)
        dst = os.path.join(lb_dir, name)
        if os.path.isfile(src) and not os.path.isfile(dst):
            shutil.copy2(src, dst)


def _read_log_tail(log_path: str, max_chars: int = 2000) -> str:
    if not os.path.isfile(log_path):
        return ""
    try:
        with open(log_path, encoding="utf-8", errors="replace") as f:
            data = f.read()
        return data[-max_chars:]
    except Exception:
        return ""


def stop_owned_mihomo(pid_file: str | None = None, wait_sec: float = 5.0) -> RuntimeResult:
    """停止本系统记录的独立 Mihomo；不触碰主 Clash。"""
    path = pid_file or PROXY_PID_FILE
    rec = read_pid_record(path)
    if not rec:
        return RuntimeResult(ok=True, error="无运行记录", error_code="NOT_RUNNING")

    pid = rec.get("pid")
    try:
        pid_i = int(pid)
    except (TypeError, ValueError):
        if os.path.exists(path):
            os.remove(path)
        return RuntimeResult(ok=False, error=f"PID 记录非法: {pid}", error_code="BAD_PID")

    if _pid_alive(pid_i):
        try:
            subprocess.run(
                ["taskkill", "/PID", str(pid_i), "/F"],
                capture_output=True, text=True, timeout=15,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0,
            )
        except Exception as e:
            return RuntimeResult(ok=False, pid=pid_i, error=f"taskkill 失败: {e}",
                                 error_code="STOP_FAILED")

    deadline = time.time() + wait_sec
    while time.time() < deadline and _pid_alive(pid_i):
        time.sleep(0.2)

    if _pid_alive(pid_i):
        return RuntimeResult(ok=False, pid=pid_i, error="进程未退出", error_code="STILL_ALIVE")

    _release_job_handle(pid_i)
    if os.path.exists(path):
        os.remove(path)
    return RuntimeResult(ok=True, pid=pid_i, ports=list(rec.get("ports") or []))


def wait_ports_ready(ports: list[int], pid: int, timeout: float = 20.0) -> RuntimeResult:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _pid_alive(pid):
            return RuntimeResult(
                ok=False, pid=pid, ports=ports,
                error="Mihomo 进程提前退出", error_code="PROCESS_EXITED",
            )
        ready = [p for p in ports if _port_listening(p)]
        if len(ready) == len(ports):
            return RuntimeResult(ok=True, pid=pid, ports=ports)
        time.sleep(0.3)
    ready = [p for p in ports if _port_listening(p)]
    if not ready:
        return RuntimeResult(
            ok=False, pid=pid, ports=ports,
            error="端口均未监听", error_code="PORTS_NOT_READY",
        )
    # 部分端口就绪也返回 ok=False，由上层决定是否继续健康检查子集
    return RuntimeResult(
        ok=False, pid=pid, ports=ready,
        error=f"仅 {len(ready)}/{len(ports)} 端口就绪",
        error_code="PORTS_PARTIAL",
    )


def _write_mihomo_config(
    nodes: list[dict],
    ports: list[int],
    directory: str,
    ctrl_port: int | None = None,
) -> str:
    os.makedirs(directory, exist_ok=True)
    _copy_geo_dbs(directory)
    cfg = build_mihomo_config(nodes, ports=ports, ctrl_port=ctrl_port)
    cfg_path = os.path.join(directory, "config.yaml")
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)
    return cfg_path


def reload_mihomo_config(
    nodes: list[dict],
    ports: list[int],
    *,
    lb_dir: str | None = None,
    pid_file: str | None = None,
    ctrl_port: int | None = None,
    ready_timeout: float = 20.0,
) -> RuntimeResult:
    """热重载配置（不杀进程）：写 config.yaml + PUT /configs?force=true。

    用于纯新增/剔除节点且稳定端口不变的场景，避免清空活池。
    """
    import urllib.error
    import urllib.request

    pid = owned_mihomo_running(pid_file)
    if not pid:
        return RuntimeResult(ok=False, error="无运行中的 Mihomo", error_code="NOT_RUNNING")
    if len(nodes) != len(ports):
        return RuntimeResult(ok=False, error="nodes/ports 长度不一致", error_code="BAD_PORTS")

    directory = lb_dir or PROXY_LB_DIR
    ctrl = PROXY_CTRL_PORT if ctrl_port is None else ctrl_port
    cfg_path = _write_mihomo_config(nodes, ports, directory, ctrl_port=ctrl)
    abs_cfg = os.path.abspath(cfg_path).replace("\\", "/")
    url = f"http://127.0.0.1:{ctrl}/configs?force=true"
    body = json.dumps({"path": abs_cfg}).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, method="PUT",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            if getattr(resp, "status", 200) >= 400:
                return RuntimeResult(
                    ok=False, pid=pid, ports=ports, config_path=cfg_path,
                    error=f"热重载 HTTP {resp.status}", error_code="RELOAD_HTTP_ERROR",
                )
    except Exception as e:
        return RuntimeResult(
            ok=False, pid=pid, ports=ports, config_path=cfg_path,
            error=f"热重载失败: {e}", error_code="RELOAD_FAILED",
        )

    ready = wait_ports_ready(ports, pid, timeout=ready_timeout)
    listening = [p for p in ports if _port_listening(p)]
    if not listening:
        return RuntimeResult(
            ok=False, pid=pid, ports=ports, config_path=cfg_path,
            error="热重载后无端口监听", error_code="PORTS_NOT_READY",
        )
    _atomic_write_json(pid_file or PROXY_PID_FILE, {
        "pid": pid,
        "ports": ports,
        "listening_ports": listening,
        "owned_by": "amz_proxy_pool",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "config_path": cfg_path,
        "reloaded_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    })
    return RuntimeResult(
        ok=True, pid=pid, ports=listening, config_path=cfg_path,
        error="" if len(listening) == len(ports) else f"部分端口就绪 {len(listening)}/{len(ports)}",
        error_code="" if len(listening) == len(ports) else "PORTS_PARTIAL",
    )


def start_mihomo(
    nodes: list[dict],
    lb_dir: str | None = None,
    pid_file: str | None = None,
    base_port: int | None = None,
    ports: list[int] | None = None,
    ready_timeout: float = 25.0,
    stop_existing: bool = True,
) -> RuntimeResult:
    """启动独立 Mihomo；失败时尽量清理。

    ports：可选稳定端口列表；不传则用 base+i。
    stop_existing=False：留给蓝绿切换，由调用方自行管理旧实例。
    """
    if not nodes:
        return RuntimeResult(ok=False, error="无节点可启动", error_code="NO_NODES")
    mihomo = MIHOMO_BIN
    if not mihomo or not os.path.isfile(mihomo):
        return RuntimeResult(ok=False, error=f"Mihomo 不存在: {mihomo}", error_code="MIHOMO_MISSING")

    directory = lb_dir or PROXY_LB_DIR
    pid_path = pid_file or PROXY_PID_FILE
    base = PROXY_BASE_PORT if base_port is None else base_port
    if ports is None:
        ports = [base + i for i in range(len(nodes))]
    elif len(ports) != len(nodes):
        return RuntimeResult(ok=False, error="nodes/ports 长度不一致", error_code="BAD_PORTS")
    # 主端口段上限检查；蓝绿临时段（>= PROXY_DAEMON_BLUE_GREEN_BASE_PORT）放行
    for p in ports:
        if p < PROXY_DAEMON_BLUE_GREEN_BASE_PORT and p > PROXY_PORT_RANGE_END:
            return RuntimeResult(
                ok=False,
                error=f"端口超出范围 {PROXY_BASE_PORT}-{PROXY_PORT_RANGE_END}: {p}",
                error_code="PORT_RANGE_EXCEEDED",
            )

    if stop_existing:
        old = owned_mihomo_running(pid_path)
        if old:
            stop_res = stop_owned_mihomo(pid_path)
            if not stop_res.ok and stop_res.error_code != "NOT_RUNNING":
                return RuntimeResult(
                    ok=False, error=f"无法停止旧实例: {stop_res.error}",
                    error_code="OLD_INSTANCE_BUSY",
                )
            time.sleep(1)

    cfg_path = _write_mihomo_config(nodes, ports, directory)
    log_path = os.path.abspath(os.path.join(directory, "mihomo.log"))
    log_fh = open(log_path, "w", encoding="utf-8", errors="replace")
    proc = None
    try:
        creationflags = 0
        if os.name == "nt":
            # Mihomo 必须由代理守护进程管理。CREATE_BREAKAWAY_FROM_JOB 会让它
            # 逃离父进程生命周期，守护进程异常退出后留下孤儿/控制台宿主。
            # CREATE_NO_WINDOW 足以隐藏控制台，同时保留可清理的父子关系。
            creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        proc = subprocess.Popen(
            [mihomo, "-d", os.path.abspath(directory)],
            stdout=log_fh, stderr=log_fh,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
        )
        # 只有成功加入 Job 后才发布 PID。daemon 被任何不可捕获方式终止时，
        # Windows 会关闭 Job 句柄并同步杀掉 Mihomo，不依赖 finally。
        _assign_kill_on_close_job(proc)
        _atomic_write_json(pid_path, {
            "pid": proc.pid,
            "ports": ports,
            "owned_by": "amz_proxy_pool",
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "config_path": cfg_path,
        })
        time.sleep(1.5)
        if proc.poll() is not None:
            log_fh.close()
            if os.path.exists(pid_path):
                os.remove(pid_path)
            return RuntimeResult(
                ok=False, pid=proc.pid, ports=ports, config_path=cfg_path,
                error="Mihomo 启动后立即退出", error_code="PROCESS_EXITED",
                log_tail=_read_log_tail(log_path),
            )

        ready = wait_ports_ready(ports, proc.pid, timeout=ready_timeout)
        if not ready.ok and ready.error_code == "PROCESS_EXITED":
            log_fh.close()
            if os.path.exists(pid_path):
                os.remove(pid_path)
            ready.log_tail = _read_log_tail(log_path)
            ready.config_path = cfg_path
            return ready

        listening = [p for p in ports if _port_listening(p)]
        if not listening:
            stop_owned_mihomo(pid_path)
            log_fh.close()
            return RuntimeResult(
                ok=False, pid=proc.pid, ports=ports, config_path=cfg_path,
                error="无端口监听", error_code="PORTS_NOT_READY",
                log_tail=_read_log_tail(log_path),
            )
        _atomic_write_json(pid_path, {
            "pid": proc.pid,
            "ports": ports,
            "listening_ports": listening,
            "owned_by": "amz_proxy_pool",
            "started_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "config_path": cfg_path,
        })
        return RuntimeResult(
            ok=True, pid=proc.pid, ports=listening, config_path=cfg_path,
            error="" if len(listening) == len(ports) else f"部分端口就绪 {len(listening)}/{len(ports)}",
            error_code="" if len(listening) == len(ports) else "PORTS_PARTIAL",
        )
    except Exception as e:
        if proc is not None and proc.poll() is None:
            try:
                proc.kill()
            except Exception:
                pass
        if proc is not None:
            _release_job_handle(proc.pid)
        if os.path.exists(pid_path):
            try:
                os.remove(pid_path)
            except OSError:
                pass
        return RuntimeResult(ok=False, error=str(e), error_code="START_EXCEPTION")
    finally:
        try:
            log_fh.close()
        except Exception:
            pass


def start_mihomo_blue_green(
    nodes: list[dict],
    primary_ports: list[int],
    *,
    alt_base_port: int,
    lb_dir: str | None = None,
    pid_file: str | None = None,
    ready_timeout: float = 25.0,
) -> RuntimeResult:
    """蓝绿切换：在临时端口段拉起新实例 → 就绪后停旧实例 → 再以主端口重启。

    返回的 ports 始终为主端口列表（primary_ports），供活池继续使用稳定端口。
    中间短暂窗口活池应暂停派发（由调用方处理）。
    """
    if len(nodes) != len(primary_ports):
        return RuntimeResult(ok=False, error="nodes/ports 长度不一致", error_code="BAD_PORTS")

    directory = lb_dir or PROXY_LB_DIR
    alt_dir = os.path.join(directory, "blue_green")
    alt_pid = (pid_file or PROXY_PID_FILE) + ".bg"
    alt_ports = [alt_base_port + i for i in range(len(nodes))]

    # 1) 临时实例（不碰主 PID）
    bg = start_mihomo(
        nodes, lb_dir=alt_dir, pid_file=alt_pid, ports=alt_ports,
        ready_timeout=ready_timeout, stop_existing=True,
    )
    if not bg.ok:
        stop_owned_mihomo(alt_pid)
        return RuntimeResult(
            ok=False, error=f"蓝绿临时实例失败: {bg.error}",
            error_code=bg.error_code or "BLUE_GREEN_TEMP_FAILED",
            log_tail=bg.log_tail,
        )

    # 2) 停主实例，把主端口腾出来；若停不掉就别硬着头皮抢主端口——
    # 那样新实例大概率绑定失败，或更糟：check_listener 误把仍存活的
    # 旧进程当成"新实例已就绪"。停不掉时保留旧实例继续对外服务，
    # 直接失败返回，让调用方按退避重试。
    stop_res = stop_owned_mihomo(pid_file)
    if not stop_res.ok and stop_res.error_code != "NOT_RUNNING":
        stop_owned_mihomo(alt_pid)  # 清理临时实例，避免残留占用 alt 端口
        return RuntimeResult(
            ok=False, error=f"蓝绿切换无法停止主实例: {stop_res.error}",
            error_code="BLUE_GREEN_STOP_PRIMARY_FAILED",
        )
    time.sleep(0.5)

    # 3) 停临时实例，用主端口正式启动（端口已空闲）
    stop_owned_mihomo(alt_pid)
    time.sleep(0.3)
    final = start_mihomo(
        nodes, lb_dir=directory, pid_file=pid_file, ports=primary_ports,
        ready_timeout=ready_timeout, stop_existing=False,
    )
    if not final.ok:
        return final
    final.error = (final.error + "; blue_green").strip("; ")
    return final


def cleanup_owned_resources(pid_file: str | None = None) -> RuntimeResult:
    """异常退出后的清理入口。"""
    return stop_owned_mihomo(pid_file)


def check_listener(port: int, pid: int | None = None) -> dict:
    alive = _pid_alive(pid) if pid else None
    listening = _port_listening(port)
    return {
        "port": port,
        "listening": listening,
        "pid": pid,
        "process_alive": alive,
        "ok": bool(listening and (alive is None or alive)),
    }
