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
import time
from dataclasses import asdict, dataclass, field

import yaml

from config import (
    CLASH_VERGE_HOME,
    DATA_DIR,
    MIHOMO_BIN,
    PROXY_BASE_PORT,
    PROXY_CTRL_PORT,
    PROXY_GEO_DB_FILES,
    PROXY_LB_DIR,
    PROXY_PID_FILE,
    PROXY_PORT_RANGE_END,
)

log = logging.getLogger("proxy_runtime")


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
    try:
        r = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True, text=True, timeout=10,
        )
        return str(pid) in (r.stdout or "")
    except Exception as e:
        log.warning("[runtime] tasklist 失败: %s", e)
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


def build_mihomo_config(nodes: list[dict], base_port: int | None = None,
                        ctrl_port: int | None = None) -> dict:
    base = PROXY_BASE_PORT if base_port is None else base_port
    ctrl = PROXY_CTRL_PORT if ctrl_port is None else ctrl_port
    groups, listeners = [], []
    clean_nodes = []
    for i, n in enumerate(nodes):
        node = {k: v for k, v in n.items() if not str(k).startswith("_")}
        clean_nodes.append(node)
        gname = f"node_{i}"
        port = base + i
        groups.append({"name": gname, "type": "select", "proxies": [node["name"]]})
        listeners.append({
            "name": f"http-in-{i}",
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
            )
        except Exception as e:
            return RuntimeResult(ok=False, pid=pid_i, error=f"taskkill 失败: {e}",
                                 error_code="STOP_FAILED")

    deadline = time.time() + wait_sec
    while time.time() < deadline and _pid_alive(pid_i):
        time.sleep(0.2)

    if _pid_alive(pid_i):
        return RuntimeResult(ok=False, pid=pid_i, error="进程未退出", error_code="STILL_ALIVE")

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


def start_mihomo(
    nodes: list[dict],
    lb_dir: str | None = None,
    pid_file: str | None = None,
    base_port: int | None = None,
    ready_timeout: float = 25.0,
) -> RuntimeResult:
    """启动独立 Mihomo；失败时尽量清理。"""
    if not nodes:
        return RuntimeResult(ok=False, error="无节点可启动", error_code="NO_NODES")
    mihomo = MIHOMO_BIN
    if not mihomo or not os.path.isfile(mihomo):
        return RuntimeResult(ok=False, error=f"Mihomo 不存在: {mihomo}", error_code="MIHOMO_MISSING")

    directory = lb_dir or PROXY_LB_DIR
    pid_path = pid_file or PROXY_PID_FILE
    base = PROXY_BASE_PORT if base_port is None else base_port
    end_port = base + len(nodes) - 1
    if end_port > PROXY_PORT_RANGE_END:
        return RuntimeResult(
            ok=False,
            error=f"端口超出范围 {base}-{PROXY_PORT_RANGE_END}，需要 {len(nodes)} 个",
            error_code="PORT_RANGE_EXCEEDED",
        )

    # 单实例：先停旧的
    old = owned_mihomo_running(pid_path)
    if old:
        stop_res = stop_owned_mihomo(pid_path)
        if not stop_res.ok and stop_res.error_code != "NOT_RUNNING":
            return RuntimeResult(
                ok=False, error=f"无法停止旧实例: {stop_res.error}",
                error_code="OLD_INSTANCE_BUSY",
            )
        time.sleep(1)

    os.makedirs(directory, exist_ok=True)
    _copy_geo_dbs(directory)

    cfg = build_mihomo_config(nodes, base_port=base)
    cfg_path = os.path.join(directory, "config.yaml")
    with open(cfg_path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)

    ports = [base + i for i in range(len(nodes))]
    log_path = os.path.abspath(os.path.join(directory, "mihomo.log"))
    log_fh = open(log_path, "w", encoding="utf-8", errors="replace")
    proc = None
    try:
        creationflags = 0
        if os.name == "nt":
            creationflags = 0x01000000 | getattr(subprocess, "CREATE_NO_WINDOW", 0)
        proc = subprocess.Popen(
            [mihomo, "-d", os.path.abspath(directory)],
            stdout=log_fh, stderr=log_fh,
            creationflags=creationflags,
        )
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

        # 部分端口也可继续：把实际监听端口回写
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
