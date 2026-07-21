"""抓取进程到代理守护进程的本地、可审计反馈通道。"""
from __future__ import annotations

import logging
import os
import sqlite3
import time
from dataclasses import dataclass

from config import PROXY_EVENT_DB_FILE

log = logging.getLogger("proxy_events")


@dataclass(frozen=True)
class ProxyEvent:
    event_id: int
    created_at: float
    node_key: str
    proxy: str
    exit_ip: str
    target: str
    outcome: str
    latency_ms: int
    detail: str


def _connect(path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    # 反馈是控制面信号，不能因多 worker 争用 SQLite 反向拖慢数据面。
    # 250ms 内抢不到写锁就由 report_proxy_event 静默降级丢弃单条事件。
    conn = sqlite3.connect(path, timeout=0.25)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=250")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS proxy_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at REAL NOT NULL,
            node_key TEXT NOT NULL,
            proxy TEXT NOT NULL DEFAULT '',
            exit_ip TEXT NOT NULL DEFAULT '',
            target TEXT NOT NULL DEFAULT 'US',
            outcome TEXT NOT NULL,
            latency_ms INTEGER NOT NULL DEFAULT 0,
            detail TEXT NOT NULL DEFAULT ''
        );
        CREATE INDEX IF NOT EXISTS idx_proxy_events_created ON proxy_events(created_at, id);
        CREATE INDEX IF NOT EXISTS idx_proxy_events_node ON proxy_events(node_key, target, id);

        CREATE TABLE IF NOT EXISTS proxy_node_target_stats (
            node_key TEXT NOT NULL,
            target TEXT NOT NULL,
            successes INTEGER NOT NULL DEFAULT 0,
            captcha INTEGER NOT NULL DEFAULT 0,
            rate_limited INTEGER NOT NULL DEFAULT 0,
            forbidden INTEGER NOT NULL DEFAULT 0,
            network_errors INTEGER NOT NULL DEFAULT 0,
            other_errors INTEGER NOT NULL DEFAULT 0,
            latency_ewma REAL NOT NULL DEFAULT 0,
            last_success_at REAL NOT NULL DEFAULT 0,
            last_error_at REAL NOT NULL DEFAULT 0,
            last_outcome TEXT NOT NULL DEFAULT '',
            updated_at REAL NOT NULL DEFAULT 0,
            PRIMARY KEY(node_key, target)
        );
        """
    )
    return conn


def report_proxy_event(
    entry: dict | None,
    outcome: str,
    *,
    target: str = "US",
    latency_ms: int = 0,
    detail: str = "",
    db_path: str | None = None,
) -> bool:
    """写入单条运行时结果。反馈失败绝不能反向打断抓取。"""
    entry = entry or {}
    node_key = str(entry.get("node_key") or "").strip()
    proxy = str(entry.get("proxy") or "").strip()
    if not node_key and not proxy:
        return False
    conn = None
    try:
        conn = _connect(db_path or PROXY_EVENT_DB_FILE)
        with conn:
            conn.execute(
                """INSERT INTO proxy_events
                   (created_at,node_key,proxy,exit_ip,target,outcome,latency_ms,detail)
                   VALUES(?,?,?,?,?,?,?,?)""",
                (
                    time.time(), node_key or proxy, proxy,
                    str(entry.get("exit_ip") or ""), str(target or "US").upper(),
                    str(outcome or "UNKNOWN").upper(), max(0, int(latency_ms or 0)),
                    str(detail or "")[:500],
                ),
            )
        return True
    except Exception as exc:
        log.warning("[feedback] 写入代理事件失败: %s", exc)
        return False
    finally:
        if conn is not None:
            conn.close()


def _bucket(outcome: str) -> str:
    code = str(outcome or "").upper()
    if code == "SUCCESS":
        return "successes"
    if code == "CAPTCHA":
        return "captcha"
    if code == "HTTP_429":
        return "rate_limited"
    if code == "HTTP_403":
        return "forbidden"
    if code in {
        "CONNECT_TIMEOUT", "READ_TIMEOUT", "PROXY_CONNECT_ERROR", "TLS_ERROR",
        "CONNECTION_RESET", "REQUEST_ERROR", "EMPTY_RESPONSE", "OTHER_HTTP_STATUS",
    }:
        return "network_errors"
    return "other_errors"


def drain_proxy_events(*, limit: int = 1000, db_path: str | None = None) -> list[ProxyEvent]:
    """原子消费事件并把长期质量指标聚合留存在 SQLite 中。"""
    path = db_path or PROXY_EVENT_DB_FILE
    if not os.path.isfile(path):
        return []
    conn = _connect(path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            """SELECT id,created_at,node_key,proxy,exit_ip,target,outcome,latency_ms,detail
               FROM proxy_events ORDER BY id LIMIT ?""",
            (max(1, int(limit)),),
        ).fetchall()
        events = [ProxyEvent(*row) for row in rows]
        for event in events:
            bucket = _bucket(event.outcome)
            existing = conn.execute(
                "SELECT latency_ewma FROM proxy_node_target_stats WHERE node_key=? AND target=?",
                (event.node_key, event.target),
            ).fetchone()
            old_latency = float(existing[0] or 0) if existing else 0.0
            latency = old_latency
            if event.latency_ms > 0:
                latency = float(event.latency_ms) if old_latency <= 0 else old_latency * 0.8 + event.latency_ms * 0.2
            success_at = event.created_at if bucket == "successes" else 0.0
            error_at = 0.0 if bucket == "successes" else event.created_at
            conn.execute(
                f"""INSERT INTO proxy_node_target_stats
                    (node_key,target,{bucket},latency_ewma,last_success_at,last_error_at,last_outcome,updated_at)
                    VALUES(?,?,1,?,?,?,?,?)
                    ON CONFLICT(node_key,target) DO UPDATE SET
                      {bucket}={bucket}+1,
                      latency_ewma=excluded.latency_ewma,
                      last_success_at=MAX(last_success_at,excluded.last_success_at),
                      last_error_at=MAX(last_error_at,excluded.last_error_at),
                      last_outcome=excluded.last_outcome,
                      updated_at=excluded.updated_at""",
                (
                    event.node_key, event.target, latency, success_at, error_at,
                    event.outcome, event.created_at,
                ),
            )
        if events:
            conn.execute(
                f"DELETE FROM proxy_events WHERE id IN ({','.join('?' for _ in events)})",
                [event.event_id for event in events],
            )
        conn.commit()
        return events
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def read_proxy_quality(*, db_path: str | None = None) -> list[dict]:
    path = db_path or PROXY_EVENT_DB_FILE
    if not os.path.isfile(path):
        return []
    conn = _connect(path)
    try:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute(
            "SELECT * FROM proxy_node_target_stats ORDER BY node_key,target"
        )]
    finally:
        conn.close()
