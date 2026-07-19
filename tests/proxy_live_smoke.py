"""
隔离业务冒烟：强制代理、最多 2 类目 / 1 页 / 20 详情，不写正式库。
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

PY = sys.executable
RUN_ID = time.strftime("SMOKE-%Y%m%d-%H%M%S")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def port_open(port: int) -> bool:
    import socket
    s = socket.socket()
    s.settimeout(0.3)
    try:
        return s.connect_ex(("127.0.0.1", port)) == 0
    finally:
        s.close()


def main() -> int:
    formal_db = ROOT / "data" / "categories.db"
    formal_hash_before = sha256_file(formal_db) if formal_db.is_file() else ""

    smoke_db = ROOT / "data" / f"categories_proxy_smoke_{RUN_ID}.db"
    shutil.copy2(formal_db, smoke_db)

    # 清空冒烟库中的业务表，避免误读旧数据；保留类目树
    conn = sqlite3.connect(smoke_db)
    for table in ("new_arrivals", "product_sightings"):
        try:
            conn.execute(f"DELETE FROM {table}")
        except sqlite3.OperationalError:
            pass
    conn.commit()
    conn.close()

    from proxy_session import load_proxy_pool
    from proxy_health import fetch_exit_ip

    loaded = load_proxy_pool(required=True, allow_direct=False)
    if not loaded.ok:
        print("POOL_LOAD_FAIL", loaded.error_code, loaded.error)
        return 2

    # 请求前确认至少两个出口 IP 真实可用
    seen_ips = []
    for entry in loaded.entries[:8]:
        res = fetch_exit_ip(entry["proxy"])
        if res.ok and res.ip not in seen_ips:
            seen_ips.append(res.ip)
        if len(seen_ips) >= 2:
            break
    if len(seen_ips) < 2:
        print("UNIQUE_IP_CHECK_FAIL", seen_ips)
        return 3

    env = os.environ.copy()
    env.update({
        "DB_BACKEND": "sqlite",
        "AMZ_DB_FILE": str(smoke_db),
        "DB_FILE": str(smoke_db),
        "PROXY_REQUIRED": "1",
        "ALLOW_DIRECT_FALLBACK": "0",
        "AMZ_MAX_DETAILS": "20",
        "PYTHONIOENCODING": "utf-8",
    })

    roots = [x.strip() for x in os.environ.get(
        "PROXY_SMOKE_ROOTS", "11965981,21490696011"
    ).split(",") if x.strip()][:2]
    cmd = [
        PY, "-u", str(ROOT / "fetch_new_arrivals.py"),
        "--site", "US",
        "--roots", *roots,
        "--exact-roots",
        "--max-pages", "1",
        "--sample", "2",
    ]
    print("RUN", RUN_ID, flush=True)
    print("CMD", cmd, flush=True)
    t0 = time.time()
    proc = subprocess.run(cmd, cwd=str(ROOT), env=env, capture_output=True, text=True, encoding="utf-8", errors="replace")
    elapsed = round(time.time() - t0, 1)
    log_path = ROOT / "data" / f"proxy_smoke_{RUN_ID}.log"
    log_path.write_text(
        (proc.stdout or "") + "\n---STDERR---\n" + (proc.stderr or ""),
        encoding="utf-8",
    )
    print("EXIT", proc.returncode, "ELAPSED", elapsed, "LOG", log_path)

    # 代理携带证据：日志中应出现强制加载 / warmup proxy=
    out = (proc.stdout or "") + (proc.stderr or "")
    proxy_loaded = "强制加载" in out or "强制代理池就绪" in out or "[pool] 强制加载" in out
    warmup_proxy = "proxy=" in out
    no_direct = "使用直连" not in out and "allow_direct" not in out.lower()

    # 停止独立 Mihomo 并检查端口残留
    from proxy_pool_manager import stop_proxy_pool
    from proxy_runtime import owned_mihomo_running, read_pid_record

    rec = read_pid_record() or {}
    ports = list(rec.get("ports") or [])
    stop_res = stop_proxy_pool()
    time.sleep(1.5)
    residual_pid = owned_mihomo_running()
    residual_ports = [p for p in ports if port_open(p)]
    # 主 Clash 端口应仍在
    main_7897 = port_open(7897)

    formal_hash_after = sha256_file(formal_db) if formal_db.is_file() else ""

    conn = sqlite3.connect(smoke_db)
    try:
        new_arrivals_count = conn.execute("SELECT COUNT(*) FROM new_arrivals").fetchone()[0]
    except sqlite3.OperationalError:
        new_arrivals_count = 0
    finally:
        conn.close()

    report = {
        "run_id": RUN_ID,
        "exit_code": proc.returncode,
        "elapsed_sec": elapsed,
        "proxy_loaded_in_log": proxy_loaded,
        "warmup_proxy_logged": warmup_proxy,
        "no_direct_mention": no_direct,
        "precheck_unique_ips": seen_ips,
        "pool_size": len(loaded.entries),
        "roots": roots,
        "new_arrivals_count": new_arrivals_count,
        "stop_result": stop_res,
        "residual_pid": residual_pid,
        "residual_ports": residual_ports,
        "main_clash_7897_alive": main_7897,
        "formal_db_hash_before": formal_hash_before,
        "formal_db_hash_after": formal_hash_after,
        "formal_db_unchanged": formal_hash_before == formal_hash_after,
        "smoke_db": str(smoke_db),
        "log": str(log_path),
    }
    out_json = ROOT / "data" / f"proxy_smoke_{RUN_ID}.json"
    out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))

    ok = (
        proc.returncode == 0
        and proxy_loaded
        and no_direct
        and len(seen_ips) >= 2
        and new_arrivals_count > 0
        and residual_pid is None
        and not residual_ports
        and main_7897
        and formal_hash_before == formal_hash_after
    )
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
