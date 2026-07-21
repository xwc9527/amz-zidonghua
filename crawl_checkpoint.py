"""SQLite断点存储：为最新到货P1/P2提供线程安全、可审计的逐项恢复。"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone

from config import DATA_DIR


def canonical_signature(config: dict) -> str:
    payload = json.dumps(config, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


class NewArrivalsCheckpoint:
    VERSION = 1

    def __init__(self, signature: str, config: dict, *, resume: bool = True, directory: str | None = None):
        root = directory or os.environ.get("AMZ_CHECKPOINT_DIR") or os.path.join(DATA_DIR, "checkpoints")
        os.makedirs(root, exist_ok=True)
        self.path = os.path.join(root, f"new_arrivals_{signature}.sqlite")
        self.signature = signature
        self.config = config
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._init_schema()
        previous_signature = self._meta_get("signature")
        if previous_signature and previous_signature != signature:
            raise RuntimeError("断点签名不一致，拒绝错误恢复")
        completed = self._meta_get("status") == "completed"
        if not resume or completed:
            self.reset()
        self._meta_set("version", str(self.VERSION))
        self._meta_set("signature", signature)
        self._meta_set("config", json.dumps(config, ensure_ascii=False, sort_keys=True))
        self._meta_set("status", "running")
        self._meta_set("updated_at", self._now())

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _init_schema(self):
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS p1_nodes(
                node_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                error_code TEXT,
                item_count INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p1_asins(
                asin TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS p2_results(
                asin TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                error_code TEXT,
                final_reason TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                exit_ips TEXT,
                reasons TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_p2_status ON p2_results(status);
            """
        )
        self._conn.commit()

    def _meta_get(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def _meta_set(self, key: str, value: str):
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )
            self._conn.commit()

    def reset(self):
        with self._lock:
            self._conn.executescript("DELETE FROM p1_nodes; DELETE FROM p1_asins; DELETE FROM p2_results;")
            self._conn.commit()

    def p1_done_ids(self) -> set[str]:
        with self._lock:
            return {r[0] for r in self._conn.execute("SELECT node_id FROM p1_nodes WHERE status='done'")}

    def save_p1_node(
        self, node_id: str, items: list[dict], *, status: str = "done",
        error_code: str = "", attempts: int = 0,
    ):
        now = self._now()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for item in items:
                    self._conn.execute(
                        "INSERT INTO p1_asins(asin,payload,updated_at) VALUES(?,?,?) "
                        "ON CONFLICT(asin) DO NOTHING",
                        (item["asin"], json.dumps(item, ensure_ascii=False, sort_keys=True), now),
                    )
                self._conn.execute(
                    "INSERT INTO p1_nodes(node_id,status,error_code,item_count,attempts,updated_at) "
                    "VALUES(?,?,?,?,?,?) ON CONFLICT(node_id) DO UPDATE SET "
                    "status=excluded.status,error_code=excluded.error_code,item_count=excluded.item_count,"
                    "attempts=excluded.attempts,updated_at=excluded.updated_at",
                    (node_id, status, error_code or None, len(items), attempts, now),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def load_asins(self) -> dict[str, dict]:
        with self._lock:
            rows = self._conn.execute("SELECT asin,payload FROM p1_asins ORDER BY rowid").fetchall()
        return {r["asin"]: json.loads(r["payload"]) for r in rows}

    def p2_done_ids(self) -> set[str]:
        with self._lock:
            return {
                r[0] for r in self._conn.execute(
                    "SELECT asin FROM p2_results WHERE status IN ('matched','filtered')"
                )
            }

    def save_p2_result(
        self,
        asin: str,
        status: str,
        *,
        error_code: str = "",
        final_reason: str = "",
        attempts: int = 0,
        exit_ips: list[str] | None = None,
        reasons: list[str] | None = None,
    ):
        with self._lock:
            self._conn.execute(
                "INSERT INTO p2_results(asin,status,error_code,final_reason,attempts,exit_ips,reasons,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(asin) DO UPDATE SET "
                "status=excluded.status,error_code=excluded.error_code,final_reason=excluded.final_reason,"
                "attempts=excluded.attempts,exit_ips=excluded.exit_ips,reasons=excluded.reasons,"
                "updated_at=excluded.updated_at",
                (
                    asin, status, error_code or None, final_reason or None, attempts,
                    json.dumps(exit_ips or [], ensure_ascii=False),
                    json.dumps(reasons or [], ensure_ascii=False), self._now(),
                ),
            )
            self._conn.commit()

    def set_phase(self, phase: str):
        self._meta_set("phase", phase)

    def complete(self):
        self._meta_set("status", "completed")
        self._meta_set("completed_at", self._now())

    def summary(self) -> dict:
        with self._lock:
            p1_rows = self._conn.execute(
                "SELECT status,COUNT(*) FROM p1_nodes GROUP BY status"
            ).fetchall()
            asins = self._conn.execute("SELECT COUNT(*) FROM p1_asins").fetchone()[0]
            rows = self._conn.execute("SELECT status,COUNT(*) FROM p2_results GROUP BY status").fetchall()
        p1 = {r[0]: r[1] for r in p1_rows}
        return {
            "path": self.path,
            "signature": self.signature,
            "phase": self._meta_get("phase") or "",
            "status": self._meta_get("status") or "",
            "p1_nodes": sum(p1.values()),
            "p1": p1,
            "asins": asins,
            "p2": {r[0]: r[1] for r in rows},
        }

    def close(self):
        with self._lock:
            self._conn.close()


class ProductsCheckpoint:
    """榜单抓取断点：按 node_id 记录完成状态，支持同配置恢复与失败重试。"""

    VERSION = 1

    def __init__(self, signature: str, config: dict, *, resume: bool = True, directory: str | None = None):
        root = directory or os.environ.get("AMZ_CHECKPOINT_DIR") or os.path.join(DATA_DIR, "checkpoints")
        os.makedirs(root, exist_ok=True)
        self.path = os.path.join(root, f"products_{signature}.sqlite")
        self.signature = signature
        self.config = config
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.path, timeout=30, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._init_schema()
        previous_signature = self._meta_get("signature")
        if previous_signature and previous_signature != signature:
            raise RuntimeError("断点签名不一致，拒绝错误恢复")
        completed = self._meta_get("status") == "completed"
        if not resume or completed:
            self.reset()
        self._meta_set("version", str(self.VERSION))
        self._meta_set("signature", signature)
        self._meta_set("config", json.dumps(config, ensure_ascii=False, sort_keys=True))
        self._meta_set("status", "running")
        self._meta_set("updated_at", self._now())

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def _init_schema(self):
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS nodes(
                node_id TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                error_code TEXT,
                products_found INTEGER NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_nodes_status ON nodes(status);
            """
        )
        self._conn.commit()

    def _meta_get(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row[0] if row else None

    def _meta_set(self, key: str, value: str):
        with self._lock:
            self._conn.execute(
                "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, str(value)),
            )
            self._conn.commit()

    def reset(self):
        with self._lock:
            self._conn.execute("DELETE FROM nodes")
            self._conn.commit()

    def done_ids(self) -> set[str]:
        with self._lock:
            return {r[0] for r in self._conn.execute("SELECT node_id FROM nodes WHERE status='done'")}

    def save_node(
        self, node_id: str, *, status: str = "done",
        error_code: str = "", products_found: int = 0, attempts: int = 0,
    ):
        with self._lock:
            self._conn.execute(
                "INSERT INTO nodes(node_id,status,error_code,products_found,attempts,updated_at) "
                "VALUES(?,?,?,?,?,?) ON CONFLICT(node_id) DO UPDATE SET "
                "status=excluded.status,error_code=excluded.error_code,"
                "products_found=excluded.products_found,attempts=excluded.attempts,"
                "updated_at=excluded.updated_at",
                (node_id, status, error_code or None, products_found, attempts, self._now()),
            )
            self._conn.commit()

    def set_phase(self, phase: str):
        self._meta_set("phase", phase)

    def complete(self):
        self._meta_set("status", "completed")
        self._meta_set("completed_at", self._now())

    def summary(self) -> dict:
        with self._lock:
            rows = self._conn.execute("SELECT status,COUNT(*) FROM nodes GROUP BY status").fetchall()
        by_status = {r[0]: r[1] for r in rows}
        return {
            "path": self.path,
            "signature": self.signature,
            "phase": self._meta_get("phase") or "",
            "status": self._meta_get("status") or "",
            "nodes": sum(by_status.values()),
            "by_status": by_status,
        }

    def close(self):
        with self._lock:
            self._conn.close()
