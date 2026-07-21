"""Web API 单实例启动器：端口预检、有限重启和可审计退出。"""
from __future__ import annotations

import http.client
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
MAX_RESTARTS = 5
BACKOFF_BASE_SEC = 2.0
BACKOFF_MAX_SEC = 30.0


def _emit(message: str, *, event: str = "info", log_path: Path | None = None, **fields) -> None:
    """同时写终端和审计日志；日志失败不得影响服务启动。"""
    print(message, flush=True)
    target = log_path or DATA_DIR / "start_server.log"
    record = {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "event": event,
        "supervisor_pid": os.getpid(),
        **fields,
    }
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    except OSError:
        pass


class SingleInstanceLock:
    """按端口加 OS 文件锁；进程异常退出时锁由操作系统自动释放。"""

    def __init__(self, path: Path, *, host: str, port: int):
        self.path = path
        self.host = host
        self.port = port
        self._fh = None
        self.error = ""

    def acquire(self) -> bool:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = self.path.open("a+b")
            self._fh.seek(0, os.SEEK_END)
            if self._fh.tell() == 0:
                self._fh.write(b" ")
                self._fh.flush()
            self._fh.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            owner = {
                "pid": os.getpid(),
                "host": self.host,
                "port": self.port,
                "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            }
            try:
                owner_path = _lock_owner_path(self.path)
                tmp = owner_path.with_suffix(owner_path.suffix + f".{os.getpid()}.tmp")
                tmp.write_text(json.dumps(owner, ensure_ascii=False), encoding="utf-8")
                os.replace(tmp, owner_path)
            except OSError as exc:
                self.error = f"锁已取得，但拥有者信息写入失败: {exc}"
            return True
        except (OSError, IOError) as exc:
            self.error = str(exc)
            if self._fh is not None:
                try:
                    self._fh.close()
                except OSError:
                    pass
                self._fh = None
            return False

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            self._fh.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except (OSError, IOError):
            pass
        try:
            self._fh.close()
        finally:
            self._fh = None


def _lock_owner_path(path: Path) -> Path:
    return Path(str(path) + ".owner.json")


def _read_lock_owner(path: Path) -> dict:
    try:
        return json.loads(_lock_owner_path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {}


def _listening_pid(port: int) -> int | None:
    """Windows 上从 netstat 获取监听 PID；失败只影响提示信息。"""
    if os.name != "nt":
        return None
    try:
        result = subprocess.run(
            ["netstat", "-ano", "-p", "tcp"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=3,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        suffix = f":{port}"
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 5 and parts[0].upper() == "TCP":
                local, state, pid = parts[1], parts[3].upper(), parts[4]
                if local.endswith(suffix) and state == "LISTENING" and pid.isdigit():
                    return int(pid)
    except (OSError, subprocess.SubprocessError):
        pass
    return None


def probe_port(host: str, port: int, *, timeout: float = 0.8) -> dict:
    """区分端口空闲、本项目 API 已运行和其他程序占用。"""
    connect_host = "127.0.0.1" if host in ("0.0.0.0", "::") else host
    try:
        with socket.create_connection((connect_host, port), timeout=timeout):
            pass
    except OSError:
        return {"occupied": False, "project_api": False, "pid": None}

    project_api = False
    detail = "端口已占用，但健康检查未识别为本项目 API"
    try:
        conn = http.client.HTTPConnection(connect_host, port, timeout=timeout)
        conn.request("GET", "/api/v2/proxy_status")
        response = conn.getresponse()
        raw = response.read(65536)
        conn.close()
        payload = json.loads(raw.decode("utf-8", errors="replace"))
        project_api = (
            response.status == 200
            and isinstance(payload, dict)
            and "status" in payload
            and "detail" in payload
        )
        if project_api:
            detail = "本项目 API 已运行"
    except (OSError, ValueError, http.client.HTTPException):
        pass
    return {
        "occupied": True,
        "project_api": project_api,
        "pid": _listening_pid(port),
        "detail": detail,
    }


def supervise(host: str, port: int, *, dev_reload: bool, env: dict) -> int:
    """只对意外非零退出做有限重启；正常退出和端口冲突立即停止。"""
    creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0) if os.name == "nt" else 0
    cmd = [sys.executable, "-m", "uvicorn", "api_server:app", "--host", host, "--port", str(port)]
    if dev_reload:
        cmd += ["--reload", "--reload-dir", str(BASE_DIR)]

    restart_count = 0
    child = None
    while True:
        started_at = time.monotonic()
        try:
            child = subprocess.Popen(
                cmd,
                cwd=str(BASE_DIR),
                env=env,
                creationflags=creationflags,
            )
            _emit(
                f"[守护] API worker 已启动 pid={child.pid}",
                event="worker_started", child_pid=child.pid, restart_count=restart_count,
            )
            code = child.wait()
        except KeyboardInterrupt:
            _emit("[守护] 收到停止请求，正在关闭。", event="supervisor_stopping")
            if child is not None and child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=3)
            return 0
        except OSError as exc:
            code = None
            _emit(
                f"[守护] 无法创建 API worker：{exc}",
                event="worker_spawn_failed", error=str(exc), restart_count=restart_count,
            )

        alive_sec = time.monotonic() - started_at
        if code == 0:
            _emit(
                f"[守护] API worker 正常退出（存活 {alive_sec:.1f}s），不再重启。",
                event="worker_clean_exit", return_code=0, alive_sec=round(alive_sec, 3),
            )
            return 0

        occupied = probe_port(host, port)
        if occupied["occupied"]:
            _emit(
                f"[守护] worker 退出后端口 {port} 已被占用"
                f"（pid={occupied.get('pid') or 'unknown'}），停止重启。",
                event="restart_blocked_by_port", return_code=code,
                alive_sec=round(alive_sec, 3), port_owner_pid=occupied.get("pid"),
            )
            return 2

        restart_count += 1
        if restart_count > MAX_RESTARTS:
            _emit(
                f"[守护] API worker 已异常退出 {restart_count} 次，达到硬上限，停止重启。",
                event="restart_exhausted", return_code=code,
                alive_sec=round(alive_sec, 3), restart_count=restart_count,
            )
            return 1

        wait_sec = min(BACKOFF_MAX_SEC, BACKOFF_BASE_SEC * (2 ** (restart_count - 1)))
        _emit(
            f"[守护] API worker 异常退出（code={code}, 存活 {alive_sec:.1f}s），"
            f"{wait_sec:.0f} 秒后重启（{restart_count}/{MAX_RESTARTS}）。",
            event="worker_restart_scheduled", return_code=code,
            alive_sec=round(alive_sec, 3), restart_count=restart_count, wait_sec=wait_sec,
        )
        try:
            time.sleep(wait_sec)
        except KeyboardInterrupt:
            _emit("[守护] 重启等待期间收到停止请求，不再重启。", event="supervisor_stopping")
            return 0


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    try:
        port = int(args[0]) if args else 8081
    except (TypeError, ValueError):
        print("[启动失败] 端口必须是整数。", flush=True)
        return 2
    if not 1 <= port <= 65535:
        print("[启动失败] 端口必须在 1-65535。", flush=True)
        return 2

    host = os.getenv("BIND_HOST", "127.0.0.1")
    dev_reload = os.getenv("DEV_RELOAD", "0") == "1"
    env = os.environ.copy()
    env.setdefault("DB_BACKEND", "sqlite")
    lock_path = DATA_DIR / f"api_server_{port}.lock"
    lock = SingleInstanceLock(lock_path, host=host, port=port)

    if not lock.acquire():
        owner = _read_lock_owner(lock_path)
        _emit(
            f"[无需启动] {host}:{port} 的服务正在运行或启动中"
            f"（supervisor pid={owner.get('pid') or 'unknown'}）。",
            event="duplicate_supervisor", owner=owner, lock_error=lock.error,
        )
        return 0

    try:
        state = probe_port(host, port)
        if state["occupied"]:
            if state["project_api"]:
                _emit(
                    f"[无需启动] 本项目 API 已在 http://127.0.0.1:{port} 运行"
                    f"（pid={state.get('pid') or 'unknown'}）。",
                    event="api_already_running", port_owner_pid=state.get("pid"),
                )
                return 0
            _emit(
                f"[启动失败] 端口 {port} 已被其他程序占用"
                f"（pid={state.get('pid') or 'unknown'}），不会循环重启。",
                event="port_conflict", port_owner_pid=state.get("pid"),
            )
            return 2

        _emit(
            f"[守护] API 服务启动，访问 http://127.0.0.1:{port}/dashboard.html",
            event="supervisor_started", host=host, port=port, dev_reload=dev_reload,
        )
        if host == "0.0.0.0":
            _emit("[守护] 警告：服务已绑定局域网地址。", event="lan_binding_warning")
        try:
            return supervise(host, port, dev_reload=dev_reload, env=env)
        except KeyboardInterrupt:
            _emit("[守护] 启动阶段收到停止请求。", event="supervisor_stopping")
            return 0
    finally:
        lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
