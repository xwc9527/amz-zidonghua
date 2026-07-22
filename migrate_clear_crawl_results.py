"""一次性清理正式抓取表（product_sightings / new_arrivals），保留收藏表。

用法（需显式确认，不会随 API 启动自动执行）:
  python migrate_clear_crawl_results.py --confirm YES_CLEAR_CRAWL_RESULTS

安全门槛:
  - 爬虫进程必须不在运行（枚举失败则拒绝）
  - 必须读取持久化 lifecycle 文件且 status==idle（不读进程内内存态）
  - 跨进程迁移锁覆盖：lifecycle/PID 复核 → 备份 → 数量复核 → 删除
  - 先备份并校验后再 DELETE；任何校验失败立即回滚/中止
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone

from config import (
    DB_FILE, PROXY_POOL_STATUS_FILE, CRAWL_MIGRATION_LOCK_FILE,
    assert_testing_paths_safe, is_testing,
)


# 与 proxy_pool_manager 对齐；本脚本不依赖其内存态
STATUS_IDLE = "idle"
_ACTIVE_STATUSES = frozenset({
    "preparing_proxy", "proxy_ready", "starting_crawler",
    "running", "stopping", "proxy_failed", "retry_pending",
})


def _utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


class MigrationLock:
    """跨进程清表锁：阻止 API 启动抓取与清表并发。"""

    def __init__(self, path: str | None = None):
        self.path = path or CRAWL_MIGRATION_LOCK_FILE
        self._fh = None

    def acquire(self, timeout: float = 0.0) -> bool:
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        deadline = time.time() + max(0.0, timeout)
        while True:
            try:
                self._fh = open(self.path, "a+b")
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
                return True
            except OSError:
                if self._fh:
                    try:
                        self._fh.close()
                    except Exception:
                        pass
                    self._fh = None
                if time.time() >= deadline:
                    return False
                time.sleep(0.05)

    def release(self) -> None:
        if not self._fh:
            return
        try:
            self._fh.seek(0)
            if os.name == "nt":
                import msvcrt
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            self._fh.close()
        except Exception:
            pass
        self._fh = None


@contextmanager
def hold_migration_lock(timeout: float = 0.0):
    lock = MigrationLock()
    if not lock.acquire(timeout=timeout):
        raise RuntimeError("无法获取清表迁移锁（可能 API 启动/另一次清表占用）")
    try:
        yield lock
    finally:
        lock.release()


def migration_in_progress() -> bool:
    """API 启动探测：锁被占用则视为清表进行中。"""
    lock = MigrationLock()
    if not lock.acquire(timeout=0.0):
        return True
    lock.release()
    return False


def _crawl_procs() -> list[str]:
    """枚举抓取进程；命令失败必须抛错（失败关闭），不得假装无进程。"""
    out = subprocess.run(
        ["wmic", "process", "where", "name='python.exe'", "get", "CommandLine"],
        capture_output=True, text=True, errors="replace", check=False,
    )
    if out.returncode != 0:
        raise RuntimeError(
            f"进程枚举失败 returncode={out.returncode}: {(out.stderr or out.stdout or '').strip()}"
        )
    lines = []
    for ln in (out.stdout or "").splitlines():
        if "fetch_products" in ln or "fetch_new_arrivals" in ln:
            lines.append(ln.strip())
    return lines


def _read_persisted_lifecycle(status_file: str | None = None) -> dict:
    """读取 API 写入的持久化状态文件；不使用进程内 get_status()。"""
    path = status_file or PROXY_POOL_STATUS_FILE
    if not os.path.isfile(path):
        raise RuntimeError(f"生命周期状态文件不存在: {path}")
    try:
        with open(path, encoding="utf-8") as f:
            state = json.load(f)
    except Exception as e:
        raise RuntimeError(f"生命周期状态文件损坏: {e}") from e
    if not isinstance(state, dict):
        raise RuntimeError("生命周期状态文件格式无效")
    status = str(state.get("status") or "").strip().lower()
    if not status:
        raise RuntimeError("生命周期状态缺失 status 字段")
    if status != STATUS_IDLE:
        raise RuntimeError(
            f"生命周期非 idle，拒绝清表: status={status!r} run_id={state.get('run_id')!r}"
        )
    if state.get("active") is True or state.get("running") is True:
        raise RuntimeError("生命周期文件标记 active/running=true，拒绝清表")
    return state


def _require_lifecycle_idle(status_file: str | None = None) -> dict:
    return _read_persisted_lifecycle(status_file)


def _count_crawl_tables(con: sqlite3.Connection) -> tuple[int, int, int]:
    ps = con.execute("SELECT COUNT(*) FROM product_sightings").fetchone()[0]
    try:
        na = con.execute("SELECT COUNT(*) FROM new_arrivals").fetchone()[0]
    except Exception:
        na = 0
    try:
        fav = con.execute("SELECT COUNT(*) FROM favorite_products").fetchone()[0]
    except Exception:
        fav = 0
    return int(ps), int(na), int(fav)


def _verify_backup(bak: str, *, expected_ps: int, expected_na: int, expected_fav: int) -> str:
    con = sqlite3.connect(f"file:{bak.replace(os.sep, '/')}?mode=ro", uri=True)
    try:
        integrity = con.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise RuntimeError(f"备份 integrity_check 失败: {integrity}")
        ps, na, fav = _count_crawl_tables(con)
        if ps != expected_ps or na != expected_na or fav != expected_fav:
            raise RuntimeError(
                f"备份数量不匹配: ps={ps}/{expected_ps} na={na}/{expected_na} fav={fav}/{expected_fav}"
            )
    finally:
        con.close()
    return hashlib.sha256(open(bak, "rb").read()).hexdigest()


def _sqlite_backup_and_clear(
    *,
    expected_ps: int | None,
    expected_na: int | None,
    status_file: str | None = None,
) -> dict:
    """在迁移锁内完成：复核 idle → 备份 → 源表复核 → 删除。"""
    assert_testing_paths_safe(db_path=DB_FILE)
    src = DB_FILE
    backup_dir = (
        os.environ.get("AMZ_MIGRATION_BACKUP_DIR")
        or os.path.join(os.path.dirname(os.path.abspath(src)), "backups")
    )
    os.makedirs(backup_dir, exist_ok=True)
    stamp = f"{_utc()}_{os.getpid()}_{time.time_ns()}"
    bak = os.path.join(backup_dir, f"categories_pre_clear_{stamp}.db")

    with hold_migration_lock(timeout=0.0):
        # 锁内再次复核进程与生命周期，缩小 TOCTOU
        procs = _crawl_procs()
        if procs:
            raise RuntimeError("锁内检测到抓取进程仍在运行: " + "; ".join(procs[:3]))
        _require_lifecycle_idle(status_file)

        # 保持观察连接贯穿基线、备份和删除事务。data_version 能识别
        # “删除一条再插入一条”等行数不变的外部提交。
        con = sqlite3.connect(src, timeout=30)
        try:
            baseline_version = int(con.execute("PRAGMA data_version").fetchone()[0])
            ps, na, fav = _count_crawl_tables(con)

            if expected_ps is not None and ps != expected_ps:
                raise RuntimeError(f"product_sightings count mismatch: got {ps} expected {expected_ps}")
            if expected_na is not None and na != expected_na:
                raise RuntimeError(f"new_arrivals count mismatch: got {na} expected {expected_na}")

            src_con = sqlite3.connect(src, timeout=30)
            try:
                dst_con = sqlite3.connect(bak)
                try:
                    src_con.backup(dst_con)
                finally:
                    dst_con.close()
            finally:
                src_con.close()

            bak_hash = _verify_backup(bak, expected_ps=ps, expected_na=na, expected_fav=fav)

            con.execute("PRAGMA journal_mode=WAL")
            cur = con.cursor()
            cur.execute("BEGIN IMMEDIATE")
            current_version = int(con.execute("PRAGMA data_version").fetchone()[0])
            if current_version != baseline_version:
                con.rollback()
                raise RuntimeError(
                    "备份后数据库发生外部提交，拒绝清表并保留全部记录"
                )
            ps_tx, na_tx, fav_tx = _count_crawl_tables(con)
            if ps_tx != ps or na_tx != na or fav_tx != fav:
                con.rollback()
                raise RuntimeError(
                    f"删除事务内数量与备份不一致，已回滚: "
                    f"ps={ps_tx}/{ps} na={na_tx}/{na} fav={fav_tx}/{fav}"
                )
            cur.execute("DELETE FROM product_sightings")
            try:
                cur.execute("DELETE FROM new_arrivals")
            except Exception:
                pass
            fav2 = _count_crawl_tables(con)[2]
            if fav2 != fav:
                con.rollback()
                raise RuntimeError("favorite_products changed during migration; rolled back")
            ps2, na2, _ = _count_crawl_tables(con)
            if ps2 != 0 or na2 != 0:
                con.rollback()
                raise RuntimeError("clear incomplete; rolled back")
            con.commit()
            return {
                "backend": "sqlite",
                "backup": bak,
                "backup_sha256": bak_hash,
                "product_sightings_before": ps,
                "new_arrivals_before": na,
                "favorites": fav,
                "product_sightings_after": ps2,
                "new_arrivals_after": na2,
                "lifecycle": "idle",
                "status_file": status_file or PROXY_POOL_STATUS_FILE,
            }
        finally:
            con.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--confirm", required=True, help="必须为 YES_CLEAR_CRAWL_RESULTS")
    ap.add_argument("--expected-ps", type=int, default=None)
    ap.add_argument("--expected-na", type=int, default=None)
    ap.add_argument("--backend", default=os.getenv("DB_BACKEND", "sqlite"))
    ap.add_argument(
        "--status-file",
        default=None,
        help="仅 TESTING=1 允许：覆盖生命周期状态文件路径",
    )
    args = ap.parse_args()
    if args.confirm != "YES_CLEAR_CRAWL_RESULTS":
        print("拒绝执行：确认口令不正确", file=sys.stderr)
        return 2
    if args.status_file is not None and not is_testing():
        print("拒绝执行：正式环境禁止 --status-file（仅 TESTING=1 可用）", file=sys.stderr)
        return 7
    try:
        assert_testing_paths_safe(db_path=DB_FILE)
    except Exception as e:
        print(f"拒绝执行：路径检查失败（失败关闭）: {e}", file=sys.stderr)
        return 3
    if args.backend == "pg":
        print("PG 清理请使用专用快照流程；本脚本当前仅支持 sqlite 正式库。", file=sys.stderr)
        return 4
    try:
        result = _sqlite_backup_and_clear(
            expected_ps=args.expected_ps,
            expected_na=args.expected_na,
            status_file=args.status_file,
        )
    except Exception as e:
        print(f"清表失败: {e}", file=sys.stderr)
        return 6
    print("OK", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
