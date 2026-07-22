"""受限真实 Amazon 四榜单 E2E runner（run_cache 架构；飙升榜已下线）。

安全原则：
- 仅 US；正式 SQLite 及其 WAL/SHM 只读快照，测试前后逐文件比较。
- 所有项目可写路径在导入生产模块前重定向到临时 runtime。
- 真实模式强制代理；BLOCKED / NOT_EXECUTED 永不返回 0。
- 成功依据为真实请求账本 + product_run_cache + 正式抓取表保持为空。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from bs4 import BeautifulSoup


ROOT = Path(__file__).resolve().parents[1]
FORMAL_DB = ROOT / "data" / "categories.db"
ALL_CHARTS = (
    "new-releases",
    "bestsellers",
    "most-wished-for",
    "most-gifted",
)
VALIDITY_COLUMN = {
    "new-releases": "nr_valid",
    "bestsellers": "bs_valid",
    "most-wished-for": "mw_valid",
    "most-gifted": None,
}
CHART_SHORT = {
    "new-releases": "nr",
    "bestsellers": "bs",
    "most-wished-for": "mw",
    "most-gifted": "mg",
}
EXIT_PASS = 0
EXIT_FAIL = 1
EXIT_SAFETY = 2
EXIT_BLOCKED = 3
EXIT_FORMAL_CHANGED = 4
EXIT_TOOL_ERROR = 5
# CLI 不可降低的强制最低门槛；允许用户设置更高值。
MIN_PROXY_NODES_FLOOR = 8
MIN_UNIQUE_IPS_FLOOR = 2
ASIN_RE = re.compile(r"^[A-Z0-9]{10}$")


class SafetyError(RuntimeError):
    pass


class ExternalBlocked(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def file_snapshot(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"path": str(path), "exists": False}
    stat = path.stat()
    return {
        "path": str(path),
        "exists": True,
        "sha256": sha256(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }


def formal_storage_snapshot(db_path: Path = FORMAL_DB) -> dict[str, dict[str, Any]]:
    return {
        suffix or "db": file_snapshot(Path(str(db_path) + suffix))
        for suffix in ("", "-wal", "-shm")
    }


def formal_storage_changed(before: dict, after: dict) -> bool:
    return before != after


def sqlite_backup(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    src = sqlite3.connect(f"file:{source.as_posix()}?mode=ro", uri=True)
    try:
        dst = sqlite3.connect(destination)
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def query_rows(db: Path, sql: str, params=()) -> list[dict]:
    con = sqlite3.connect(db)
    con.row_factory = sqlite3.Row
    try:
        return [dict(row) for row in con.execute(sql, params).fetchall()]
    finally:
        con.close()


def query_scalar(db: Path, sql: str, params=()):
    """安全断言用标量查询：表缺失/损坏必须上抛，禁止伪装成 0。"""
    con = sqlite3.connect(db)
    try:
        row = con.execute(sql, params).fetchone()
        return row[0] if row else None
    finally:
        con.close()


def require_tables(db: Path, tables: list[str]) -> None:
    con = sqlite3.connect(db)
    try:
        existing = {
            str(r[0]) for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    finally:
        con.close()
    missing = [t for t in tables if t not in existing]
    if missing:
        raise RuntimeError(f"required tables missing: {missing}")


def git_state() -> dict[str, Any]:
    def run(*args):
        proc = subprocess.run(
            ["git", *args], cwd=ROOT, capture_output=True, text=True,
            encoding="utf-8", errors="replace", check=False,
        )
        return proc.returncode, (proc.stdout or "").strip(), (proc.stderr or "").strip()

    rc, commit, _ = run("rev-parse", "HEAD")
    status_rc, status, status_err = run("status", "--short")
    return {
        "commit": commit if rc == 0 else "",
        "dirty": bool(status),
        "status": status.splitlines(),
        "errors": [x for x in (status_err if status_rc else "",) if x],
    }


def normalize_charts(raw: str) -> list[str]:
    if raw.strip().lower() == "all":
        return list(ALL_CHARTS)
    charts = [item.strip().lower() for item in raw.split(",") if item.strip()]
    unknown = [item for item in charts if item not in ALL_CHARTS]
    if unknown:
        raise SafetyError(f"unsupported charts: {unknown}")
    if not charts:
        raise SafetyError("at least one chart is required")
    return list(dict.fromkeys(charts))


def validate_args(args) -> list[str]:
    if args.site.upper() != "US":
        raise SafetyError("only US is permitted")
    if args.max_pages not in (1, 2):
        raise SafetyError("--max-pages must be exactly 1 or 2")
    if not 1 <= args.details_per_list <= 10:
        raise SafetyError("--details-per-list must be within 1..10")
    if not 1 <= args.candidate_limit <= 50:
        raise SafetyError("--candidate-limit must be within 1..50")
    if args.min_proxy_nodes < MIN_PROXY_NODES_FLOOR:
        raise SafetyError(
            f"--min-proxy-nodes must be >= {MIN_PROXY_NODES_FLOOR} "
            f"(got {args.min_proxy_nodes})"
        )
    if args.min_unique_ips < MIN_UNIQUE_IPS_FLOOR:
        raise SafetyError(
            f"--min-unique-ips must be >= {MIN_UNIQUE_IPS_FLOOR} "
            f"(got {args.min_unique_ips})"
        )
    return normalize_charts(args.charts)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Bounded run_cache E2E for four Amazon ranking charts"
    )
    parser.add_argument("--site", default="US")
    parser.add_argument("--charts", default="all")
    parser.add_argument("--details-per-list", type=int, default=3)
    parser.add_argument("--max-pages", type=int, default=1)
    parser.add_argument("--candidate-limit", type=int, default=10)
    parser.add_argument("--min-proxy-nodes", type=int, default=8)
    parser.add_argument("--min-unique-ips", type=int, default=2)
    parser.add_argument("--keep-temp", action="store_true")
    parser.add_argument("--report-dir", default="")
    parser.add_argument("--skip-pg", action="store_true")
    parser.add_argument(
        "--no-live", action="store_true",
        help="安全演练；不发网络请求，最终状态固定 NOT_EXECUTED",
    )
    return parser.parse_args(argv)


def prepare_runtime(args, run_id: str) -> dict[str, Path]:
    runtime = Path(tempfile.mkdtemp(prefix=f"{run_id}-"))
    data_dir = runtime / "data"
    output_dir = runtime / "output"
    checkpoints = runtime / "checkpoints"
    exports = runtime / "exports"
    for path in (data_dir, output_dir, checkpoints, exports):
        path.mkdir(parents=True, exist_ok=True)

    test_db = data_dir / "categories_e2e.db"
    cache_db = data_dir / "product_run_cache_e2e.db"
    # 不能直接用 SQLite 打开正式库：即使 mode=ro 也可能触碰正式 -shm。
    # 先只做文件读取复制，再仅对临时快照执行 SQLite backup。
    source_snapshot_dir = runtime / "source_snapshot"
    source_snapshot_dir.mkdir()
    staged_db = source_snapshot_dir / "categories.db"
    shutil.copy2(FORMAL_DB, staged_db)
    formal_wal = Path(str(FORMAL_DB) + "-wal")
    if formal_wal.is_file():
        shutil.copy2(formal_wal, Path(str(staged_db) + "-wal"))
    sqlite_backup(staged_db, test_db)
    con = sqlite3.connect(test_db)
    try:
        for table in ("product_sightings", "new_arrivals", "favorite_products"):
            try:
                con.execute(f"DELETE FROM {table}")
            except sqlite3.OperationalError:
                pass
        con.commit()
    finally:
        con.close()

    # 代理池输入复制到隔离 data 目录；生产副本永不被运行时更新。
    for name in (
        "proxy_pool.json",
        "proxy_pool.last_good.json",
        "proxy_pool.candidate.json",
    ):
        source = ROOT / "data" / name
        if source.is_file():
            shutil.copy2(source, data_dir / name)

    report_root = (
        Path(args.report_dir).resolve()
        if args.report_dir
        else Path(tempfile.gettempdir()) / "amz-ranking-e2e-reports"
    )
    report_root.mkdir(parents=True, exist_ok=True)
    stable_report_dir = report_root / run_id
    stable_report_dir.mkdir(parents=True, exist_ok=False)
    return {
        "runtime": runtime,
        "data": data_dir,
        "output": output_dir,
        "checkpoints": checkpoints,
        "exports": exports,
        "test_db": test_db,
        "cache_db": cache_db,
        "ledger": runtime / "request_ledger.jsonl",
        "report_dir": stable_report_dir,
    }


def configure_environment(paths: dict[str, Path], run_id: str) -> None:
    env = {
        "TESTING": "1",
        "DB_BACKEND": "sqlite",
        "PRODUCT_RESULT_MODE": "run_cache",
        "DB_FILE": str(paths["test_db"]),
        "AMZ_DB_FILE": str(paths["test_db"]),
        "AMZ_RUN_CACHE_FILE": str(paths["cache_db"]),
        "AMZ_DATA_DIR": str(paths["data"]),
        "AMZ_OUTPUT_DIR": str(paths["output"]),
        "AMZ_CHECKPOINT_DIR": str(paths["checkpoints"]),
        "AMZ_CRAWL_MIGRATION_LOCK_FILE": str(paths["data"] / "crawl_migration.lock"),
        "AMZ_TEST_EXPORT_DIR": str(paths["exports"]),
        "AMZ_FETCH_PRODUCTS_LOG": str(paths["data"] / "fetch_products.log"),
        "AMZ_FETCH_PRODUCTS_AUDIT_LOG": str(paths["data"] / "fetch_products_attempts.jsonl"),
        "PROXY_REQUIRED": "1",
        "ALLOW_DIRECT_FALLBACK": "0",
        "AMZ_RUN_ID": run_id,
        "PYTHONIOENCODING": "utf-8",
    }
    os.environ.update(env)


def assert_module_paths(paths, fp, config, run_cache) -> None:
    runtime = paths["runtime"].resolve()
    actual = {
        "config.DB_FILE": Path(config.DB_FILE).resolve(),
        "config.DATA_DIR": Path(config.DATA_DIR).resolve(),
        "run_cache": Path(run_cache.cache_path()).resolve(),
        "fetch_products.LOG_PATH": Path(fp.LOG_PATH).resolve(),
        "fetch_products.AUDIT_PATH": Path(fp._AUDIT_PATH).resolve(),
    }
    escaped = {
        name: str(path) for name, path in actual.items()
        if runtime != path and runtime not in path.parents
    }
    if escaped:
        raise SafetyError(f"runtime path escaped sandbox: {escaped}")


def stop_pool(pool) -> dict[str, Any]:
    """ForcedProxyPool 无 close()；停止热重载并实证线程已退出。"""
    result: dict[str, Any] = {
        "stop_called": False,
        "had_live_reload_thread": False,
        "thread_alive_before": False,
        "thread_alive_after": False,
        "thread_name": "",
        "ok": True,
        "error": "",
    }
    if pool is None:
        return result
    thread = getattr(pool, "_live_reload_thread", None)
    result["had_live_reload_thread"] = thread is not None
    result["thread_alive_before"] = bool(thread is not None and thread.is_alive())
    if thread is not None:
        result["thread_name"] = str(getattr(thread, "name", "") or "")
    stopper = getattr(pool, "stop_live_reload", None)
    if callable(stopper):
        stopper()
        result["stop_called"] = True
    thread_after = getattr(pool, "_live_reload_thread", None)
    alive_after = bool(thread_after is not None and thread_after.is_alive())
    result["thread_alive_after"] = alive_after
    # 仅当停止前线程仍存活、停止后仍存活时判定清理失败。
    if result["thread_alive_before"] and alive_after:
        result["ok"] = False
        result["error"] = "live_reload_thread_still_alive_after_stop"
    return result


def request_has_verified_proxy_evidence(row: dict) -> bool:
    """账本行必须携带经代理实测的出口证据，池声明 IP 单独出现不算。"""
    evidence_rows = list(row.get("proxy_evidence") or [])
    if not evidence_rows:
        return False
    for evidence in evidence_rows:
        if not isinstance(evidence, dict):
            return False
        identity_ok = bool(
            evidence.get("node_key")
            or evidence.get("proxy")
            or (
                evidence.get("proxy_name")
                and evidence.get("port") is not None
            )
        )
        verified_ip = str(evidence.get("verified_exit_ip") or "").strip()
        probe_ok = bool(evidence.get("probe_ok")) and bool(verified_ip)
        probe_meta_ok = bool(evidence.get("probe_at")) and bool(
            evidence.get("probe_source")
        )
        via_proxy = bool(evidence.get("amazon_via_proxy"))
        if not (identity_ok and probe_ok and probe_meta_ok and via_proxy):
            return False
    return True


def verified_exit_ips_from_requests(requests: list[dict]) -> list[str]:
    ips: set[str] = set()
    for row in requests:
        for evidence in row.get("proxy_evidence") or []:
            if not isinstance(evidence, dict):
                continue
            ip = str(evidence.get("verified_exit_ip") or "").strip()
            if ip and evidence.get("probe_ok"):
                ips.add(ip)
        for ip in row.get("verified_exit_ips") or []:
            if ip:
                ips.add(str(ip))
    return sorted(ips)


def find_iproyal_nodes(nodes: list[dict]) -> list[dict]:
    """复用 is_iproyal_node，覆盖 name/server/provider/source/remark 等字段。"""
    from proxy_node_source import is_iproyal_node

    hits = []
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if is_iproyal_node(node):
            hits.append({
                "name": node.get("name"),
                "provider": node.get("provider"),
                "source": node.get("source"),
                "server": node.get("server"),
                "remark": node.get("remark") or node.get("remarks"),
            })
    return hits


def load_raw_subscription_nodes() -> tuple[list[dict], str]:
    """加载订阅原始 proxies；失败返回 ([], error)。"""
    try:
        import yaml
        from proxy_node_source import resolve_active_profile

        fingerprint = resolve_active_profile()
        with open(fingerprint.path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}
        proxies = data.get("proxies")
        if not isinstance(proxies, list):
            return [], "subscription proxies missing or not a list"
        return [row for row in proxies if isinstance(row, dict)], ""
    except Exception as exc:
        return [], f"{type(exc).__name__}:{exc}"


def probe_verified_exits(entries: list[dict], *, limit: int = 16) -> dict[str, Any]:
    """经各代理节点实测出口；禁止直连。"""
    from proxy_health import fetch_exit_ip

    probes = []
    unique_ips: set[str] = set()
    for entry in entries[: max(0, limit)]:
        proxy_url = str(entry.get("proxy") or "").strip()
        if not proxy_url:
            continue
        result = fetch_exit_ip(proxy_url)
        row = {
            "node_key": entry.get("node_key") or "",
            "proxy_name": entry.get("name") or "",
            "port": entry.get("port"),
            "proxy": proxy_url,
            "probe_ok": bool(result.ok and result.ip),
            "verified_exit_ip": result.ip if result.ok else "",
            "probe_endpoint": result.endpoint or "",
            "probe_error": result.error or result.error_code or "",
            "probe_source": "proxy_health.fetch_exit_ip",
            "probe_at": utc_now(),
            "pool_declared_exit_ip": entry.get("exit_ip") or "",
        }
        probes.append(row)
        if row["probe_ok"]:
            unique_ips.add(row["verified_exit_ip"])
    return {
        "probes": probes,
        "unique_verified_exit_ips": sorted(unique_ips),
        "verified_count": len(unique_ips),
    }


@dataclass
class RecordingClient:
    inner: Any
    ledger_path: Path
    chart: str = ""
    node_id: str = ""

    def __post_init__(self):
        self.pool = self.inner.pool
        self.requests: list[dict[str, Any]] = []

    def get(self, url, **kwargs):
        from proxy_worker import is_captcha_page

        started = time.monotonic()
        outcome = self.inner.get(url, **kwargs)
        elapsed = int((time.monotonic() - started) * 1000)
        html = outcome.html or ""
        evidence = [dict(item) for item in (outcome.proxy_evidence or []) if isinstance(item, dict)]
        verified_ips = sorted({
            str(item.get("verified_exit_ip") or "")
            for item in evidence
            if item.get("probe_ok") and item.get("verified_exit_ip")
        })
        row = {
            "timestamp": utc_now(),
            "phase": kwargs.get("phase") or "",
            "chart": self.chart,
            "node_id": self.node_id,
            "item_id": kwargs.get("item_id") or "",
            "url": url,
            "ok": bool(outcome.ok),
            "status_code": outcome.status_code,
            "attempts": int(outcome.attempts or 0),
            # 仅记录经代理实测的 verified IPs；不再把池声明值写入 exit_ips。
            "exit_ips": list(verified_ips),
            "verified_exit_ips": list(verified_ips),
            "proxy_evidence": evidence,
            "response_bytes": len(html.encode("utf-8", errors="ignore")),
            "elapsed_ms": int(outcome.elapsed_ms or elapsed),
            "error_code": outcome.error_code or "",
            "final_reason": outcome.final_reason or "",
            "captcha": bool(html) and is_captcha_page(html),
        }
        self.requests.append(row)
        with self.ledger_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        return outcome

    def close(self) -> dict[str, Any]:
        close_result: dict[str, Any] = {"client_closed": False, "pool_stop": {}}
        try:
            self.inner.close()
            close_result["client_closed"] = True
        finally:
            close_result["pool_stop"] = stop_pool(getattr(self, "pool", None))
        return close_result


def candidate_rows(test_db: Path, chart: str, limit: int) -> list[dict]:
    """按榜单有效性列优先选节点；有效候选不足 limit 时用通用深层节点补足
    （有效候选始终排在前面优先尝试），确保发现阶段有多个候选可退——
    不会因单一候选撞上代理抖动就直接判该榜 BLOCKED_NO_LIVE_NODE。"""
    validity = VALIDITY_COLUMN[chart]
    base_where = [
        "site='US'",
        "node_id IS NOT NULL",
        "TRIM(node_id)<>''",
        "COALESCE(depth,0)>=2",
    ]

    def _query(where_parts: list[str], row_limit: int) -> list[dict]:
        sql = f"""
            SELECT node_id,name,depth,url,slug,COALESCE(child_count,999999) AS child_count
            FROM categories
            WHERE {' AND '.join(where_parts)}
            ORDER BY CASE WHEN depth=3 THEN 0 ELSE 1 END,
                     COALESCE(child_count,999999), name, node_id
            LIMIT ?
        """
        return query_rows(test_db, sql, (row_limit,))

    if not validity:
        return _query(base_where, limit)

    valid_rows = _query([*base_where, f"{validity}=1"], limit)
    if len(valid_rows) >= limit:
        return valid_rows
    seen_ids = {str(row["node_id"]) for row in valid_rows}
    fallback_rows = _query(base_where, limit + len(seen_ids))
    padded = list(valid_rows)
    for row in fallback_rows:
        if len(padded) >= limit:
            break
        if str(row["node_id"]) in seen_ids:
            continue
        padded.append(row)
        seen_ids.add(str(row["node_id"]))
    return padded


def _html_product_asins(html: str) -> list[str]:
    """只从榜单主内容/商品候选容器提取 ASIN，排除页头页脚等噪声。"""
    soup = BeautifulSoup(html or "", "html.parser")
    selectors = (
        "#zg",
        "#zg-center-div",
        '[id^="gridItemRoot"]',
        ".zg-grid-general-faceout",
        ".p13n-sc-uncoverable-faceout",
        ".zg-item-immersion",
        '[id^="p13n-asin-index"]',
    )
    excluded_tokens = (
        "footer", "navfooter", "recommendation", "rhf", "related-products",
    )
    values = set()
    for container in soup.select(",".join(selectors)):
        for candidate in container.select('[data-asin],a[href*="/dp/"]'):
            excluded = False
            current = candidate
            while current is not None:
                attrs = " ".join([
                    str(current.get("id") or ""),
                    " ".join(current.get("class") or []),
                ]).lower()
                if current.name in {"header", "footer", "nav"} or any(
                    token in attrs for token in excluded_tokens
                ):
                    excluded = True
                    break
                if current is container:
                    break
                current = current.parent
            if excluded:
                continue
            data_asin = str(candidate.get("data-asin") or "").upper()
            if ASIN_RE.fullmatch(data_asin):
                values.add(data_asin)
            href = str(candidate.get("href") or "")
            match = re.search(
                r"/dp/([A-Z0-9]{10})(?:[/?#&]|$)",
                href,
                flags=re.IGNORECASE,
            )
            if match and ASIN_RE.fullmatch(match.group(1).upper()):
                values.add(match.group(1).upper())
    return sorted(values)


def classify_discovery_response(
    outcome,
    url: str,
    html: str,
    product_items: int,
    *,
    captcha: bool = False,
) -> str:
    """对 discovery 响应给出互斥、可审计的失败分类；有商品时返回空串。"""
    if not outcome.ok or outcome.status_code != 200:
        return "HTTP_BLOCKED"
    if captcha:
        return "CAPTCHA_BLOCKED"
    if product_items > 0:
        return ""
    product_asins = _html_product_asins(html)
    if len(product_asins) >= 2:
        return "PARSE_MISS"
    return "NO_PRODUCTS_UNKNOWN"


def discover_live_node(
    fp,
    client: RecordingClient,
    chart: str,
    candidates: list[dict],
):
    from proxy_worker import is_captcha_page

    attempts = []
    for row in candidates:
        slug = (row.get("slug") or fp.extract_slug(row.get("url") or "") or "generic").strip("/")
        url = f"{fp._DOMAIN}/gp/{chart}/{slug}/{row['node_id']}/"
        client.chart = chart
        client.node_id = str(row["node_id"])
        outcome = client.get(
            url,
            phase="LIST",
            item_id=f"discover:{row['node_id']}:{chart}",
            referer=f"{fp._DOMAIN}/",
        )
        html = outcome.html or ""
        parsed = []
        if outcome.ok and outcome.status_code == 200 and not is_captcha_page(html):
            parsed = fp.parse_products(
                html,
                str(row["node_id"]),
                row.get("name") or "",
                slug,
                int(row.get("depth") or 0),
                chart,
                fp._count_product_items(html),
                review_max=0,
            )
        asins = sorted({
            str(item.get("asin") or "").upper()
            for item in parsed
            if ASIN_RE.fullmatch(str(item.get("asin") or "").upper())
        })
        product_items = fp._count_product_items(html) if html else 0
        failure_class = classify_discovery_response(
            outcome,
            url,
            html,
            product_items,
            captcha=is_captcha_page(html),
        )
        attempt = {
            "node_id": str(row["node_id"]),
            "name": row.get("name") or "",
            "depth": int(row.get("depth") or 0),
            "slug": slug,
            "url": url,
            "ok": bool(outcome.ok),
            "status_code": outcome.status_code,
            "captcha": is_captcha_page(html),
            "chart_unavailable": failure_class == "EXTERNAL_CHART_EMPTY",
            "failure_class": failure_class,
            "product_items": product_items,
            "valid_asins": asins,
            "verified_exit_ips": sorted({
                str(item.get("verified_exit_ip") or "")
                for item in (outcome.proxy_evidence or [])
                if isinstance(item, dict) and item.get("probe_ok") and item.get("verified_exit_ip")
            }),
            "proxy_evidence": [
                dict(item) for item in (outcome.proxy_evidence or [])
                if isinstance(item, dict)
            ],
            "error": outcome.final_reason or outcome.error_code or "",
        }
        attempts.append(attempt)
        if (
            attempt["ok"]
            and attempt["status_code"] == 200
            and not attempt["captcha"]
            and attempt["product_items"] > 0
            and asins
        ):
            chosen = dict(row)
            chosen["slug"] = slug
            chosen["url"] = url
            return chosen, attempts
    return None, attempts


def discovery_failure_counts(attempts: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for attempt in attempts:
        failure_class = str(attempt.get("failure_class") or "")
        if failure_class:
            counts[failure_class] = counts.get(failure_class, 0) + 1
    return dict(sorted(counts.items()))


def proxy_preflight(fp, args) -> tuple[Any, dict]:
    pool = fp.ProxyPool()
    snapshot = pool.health_snapshot()
    # 完整原始池条目（含 provider/source 等），不只 health_snapshot 的精简 name。
    pool_entries = [dict(row) for row in (getattr(pool, "_all", None) or [])]
    raw_nodes, raw_err = load_raw_subscription_nodes()
    iproyal_hits = find_iproyal_nodes(raw_nodes) + find_iproyal_nodes(pool_entries)
    # 去重展示
    seen = set()
    iproyal_unique = []
    for hit in iproyal_hits:
        key = (
            str(hit.get("name") or ""),
            str(hit.get("provider") or ""),
            str(hit.get("server") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        iproyal_unique.append(hit)

    # 经代理实测出口；池声明 cached_exit_ips 仅信息展示，不作为门槛依据。
    cached_ips = sorted({
        str(row.get("exit_ip") or "")
        for row in pool_entries
        if row.get("exit_ip")
    })
    probe_result = probe_verified_exits(pool_entries, limit=max(16, args.min_proxy_nodes))
    verified_ips = list(probe_result["unique_verified_exit_ips"])
    result = {
        "total": int(snapshot.get("total") or 0),
        "usable": int(snapshot.get("usable") or 0),
        "disabled": int(snapshot.get("disabled") or 0),
        "cooling": int(snapshot.get("cooling") or 0),
        "cached_exit_ips": cached_ips,
        "verified_exit_ips": verified_ips,
        "exit_probes": probe_result["probes"],
        "raw_subscription_nodes": len(raw_nodes),
        "raw_subscription_error": raw_err,
        "iproyal_nodes": iproyal_unique,
        "required_nodes": args.min_proxy_nodes,
        "required_unique_ips": args.min_unique_ips,
        "note": (
            "preflight unique IPs require live proxy probes; "
            "final proof uses request ledger verified evidence"
        ),
    }
    reasons = []
    if result["usable"] < args.min_proxy_nodes:
        reasons.append(f"usable={result['usable']}<{args.min_proxy_nodes}")
    if len(verified_ips) < args.min_unique_ips:
        reasons.append(
            f"verified_unique_ips={len(verified_ips)}<{args.min_unique_ips}"
        )
    if raw_err:
        reasons.append(f"raw subscription scan failed: {raw_err}")
    if iproyal_unique:
        reasons.append(f"IPRoyal present: {iproyal_unique}")
    if os.getenv("PROXY_REQUIRED") != "1" or os.getenv("ALLOW_DIRECT_FALLBACK") != "0":
        reasons.append("proxy env safety mismatch")
    result["passed"] = not reasons
    result["reasons"] = reasons
    if reasons:
        stop_info = stop_pool(pool)
        result["cleanup_on_block"] = stop_info
        raise ExternalBlocked("proxy preflight blocked: " + "; ".join(reasons))
    return pool, result


def request_policy_failures(
    requests: list[dict],
    *,
    max_pages: int,
    require_page2: bool = False,
    require_page2_success: bool = False,
) -> list[str]:
    failures = []
    list_rows = [row for row in requests if row.get("phase") == "LIST"]
    list_urls = [row.get("url") or "" for row in list_rows]
    page2_rows = [
        row for row in list_rows
        if re.search(r"[?&]pg=2(?:&|$)", row.get("url") or "")
    ]
    has_page2 = bool(page2_rows)
    has_page3 = any(re.search(r"[?&]pg=3(?:&|$)", url) for url in list_urls)
    if max_pages == 1 and has_page2:
        failures.append("PAGE2_REQUESTED_IN_ONE_PAGE_MODE")
    if require_page2 and not has_page2:
        failures.append("PAGE2_NOT_REQUESTED")
    if require_page2_success and page2_rows:
        ok_page2 = [
            row for row in page2_rows
            if row.get("ok")
            and row.get("status_code") == 200
            and int(row.get("response_bytes") or 0) > 0
            and not row.get("captcha")
        ]
        if not ok_page2:
            failures.append("PAGE2_REQUEST_FAILED")
    if has_page3:
        failures.append("PAGE3_REQUESTED")
    # 证据完整性只对成功请求生效：失败尝试正是因为出口探测本身失败才失败，
    # 天然不可能携带证据；探测成功后才会发真实请求，因此每条成功行必有证据。
    # 要求失败行也带证据，会让"有界重试后恢复成功"结构性地不可能通过审计。
    successful_rows = [row for row in requests if row.get("ok")]
    if any(not request_has_verified_proxy_evidence(row) for row in successful_rows):
        failures.append("PROXY_EVIDENCE_MISSING")
    return failures


def ledger_proxy_failures(
    requests: list[dict],
    *,
    min_unique_ips: int,
) -> list[str]:
    """最终代理证据：完整 request ledger 的 verified exit，不以池元数据代替。
    只对成功请求要求证据——失败尝试正是因为出口探测本身失败才失败。"""
    failures = []
    if not requests:
        return ["PROXY_LEDGER_EMPTY"]
    successful_rows = [row for row in requests if row.get("ok")]
    if any(not request_has_verified_proxy_evidence(row) for row in successful_rows):
        failures.append("PROXY_EVIDENCE_MISSING")
    unique = verified_exit_ips_from_requests(requests)
    if len(unique) < min_unique_ips:
        failures.append(
            f"LEDGER_UNIQUE_IPS_LOW:{len(unique)}<{min_unique_ips}"
        )
    return failures


def formal_table_failures(counts: dict[str, int]) -> list[str]:
    return [
        f"FORMAL_TEST_TABLE_NOT_EMPTY:{table}:{count}"
        for table, count in counts.items()
        if int(count) != 0
    ]


def assert_formal_tables_empty(db: Path) -> tuple[list[str], dict[str, int]]:
    tables = ["product_sightings", "new_arrivals", "favorite_products"]
    require_tables(db, tables)
    counts = {
        table: int(query_scalar(db, f"SELECT COUNT(*) FROM {table}") or 0)
        for table in tables
    }
    return formal_table_failures(counts), counts


def process_status_failures(status, error) -> list[str]:
    failures = []
    if status != "done":
        failures.append(f"PROCESS_STATUS_NOT_DONE:{status}")
    if error:
        failures.append(f"PROCESS_ERROR:{error}")
    return failures


CHART_NODE_RETRY_ATTEMPTS = 3


def run_chart(fp, run_cache, client, chart, chosen, args, base_run_id) -> dict:
    """按生产断点重试语义跑一个榜单：node status!=done 时对同一节点有界重试；
    已成功的 ASIN 详情命中同 run_id 缓存直接跳过，不会重复发请求
    （与生产"下次只重试失败节点"完全一致，只是把跨进程重试压缩进本次调用）。"""
    run_id = f"{base_run_id}-{CHART_SHORT[chart]}"
    run_cache.create_generation(run_id, "products")
    run_cache.activate_generation(run_id, "products")
    fp._RUN_ID = run_id
    before = len(client.requests)
    client.chart = chart
    client.node_id = str(chosen["node_id"])
    node_retries = 0
    for retry in range(CHART_NODE_RETRY_ATTEMPTS):
        # 每次重试都要重新清空全局 ASIN 去重集，否则同一节点的 ASIN 会被
        # parse_products 当作"已见过"整批过滤掉，导致重试形同空转
        # （本函数每次只处理一个节点，清空不会影响其它节点的跨节点去重）。
        with fp._seen_lock:
            fp._seen_asins.clear()
        status, error, found, attempts = fp.process_node(
            chosen,
            [chart],
            review_max=0,
            min_list_size=0,
            client=client,
            max_pages=args.max_pages,
            delay=0.0,
            detail_filters={},
            list_limit=max(1, args.details_per_list),
        )
        if status == "done":
            break
        node_retries = retry + 1
    request_slice = client.requests[before:]
    list_requests = [row for row in request_slice if row["phase"] == "LIST"]
    detail_requests = [row for row in request_slice if row["phase"] == "DETAIL"]
    cache_rows = run_cache.query_products(
        {"site": "US", "detail_only": False, "limit": 5000},
        chart="products",
    )
    chart_rows = [row for row in cache_rows if row.get("list_type") == chart]
    detail_ok = [row for row in chart_rows if int(row.get("detail_scraped") or 0) == 1]
    detail_failed = [row for row in chart_rows if int(row.get("detail_scraped") or 0) == 2]
    duplicate_keys = query_rows(
        Path(run_cache.cache_path()),
        """SELECT run_id,site,asin,node_id,list_type,COUNT(*) AS copies
           FROM product_cache
           GROUP BY run_id,site,asin,node_id,list_type
           HAVING COUNT(*)>1""",
    )
    failures = []
    failures.extend(process_status_failures(status, error))
    if not list_requests:
        failures.append("NO_LIST_REQUEST")
    if not any(row["ok"] and row["status_code"] == 200 for row in list_requests):
        failures.append("NO_SUCCESSFUL_LIST_REQUEST")
    if found <= 0 or not chart_rows:
        failures.append("NO_CACHE_PRODUCTS")
    if not detail_requests:
        failures.append("NO_DETAIL_REQUEST")
    if not detail_ok:
        failures.append("NO_DETAIL_SUCCESS")
    if run_cache.get_active_run_id() != run_id:
        failures.append("ACTIVE_RUN_MISMATCH")
    if any(row.get("run_id") != run_id for row in chart_rows):
        failures.append("CACHE_RUN_MISMATCH")
    if any(row.get("site") != "US" for row in chart_rows):
        failures.append("CACHE_SITE_MISMATCH")
    if any(row.get("chart") != "products" for row in chart_rows):
        failures.append("CACHE_CHART_MISMATCH")
    if any(row.get("list_type") != chart for row in chart_rows):
        failures.append("CACHE_LIST_TYPE_MISMATCH")
    if any(not row.get("cache_id") for row in chart_rows):
        failures.append("CACHE_ID_MISSING")
    if duplicate_keys:
        failures.append("CACHE_DUPLICATE_KEYS")
    failures.extend(request_policy_failures(
        request_slice, max_pages=args.max_pages, require_page2=False,
    ))
    return {
        "chart": chart,
        "run_id": run_id,
        "selected_node": chosen,
        "process_status": status,
        "process_error": error,
        "node_retries": node_retries,
        "products_found": found,
        "attempts": attempts,
        "list_requests": len(list_requests),
        "detail_requests": len(detail_requests),
        "parsed_asins": sorted({row.get("asin") for row in chart_rows}),
        "detail_success": len(detail_ok),
        "detail_failed": len(detail_failed),
        "cache_rows": len(chart_rows),
        "duplicate_keys": duplicate_keys,
        "verified_exit_ips": verified_exit_ips_from_requests(request_slice),
        "failures": failures,
        "status": "PASS" if not failures else "FAIL",
    }


def run_page_test(fp, run_cache, client, selected: dict, base_run_id: str) -> dict:
    chart = "new-releases" if "new-releases" in selected else next(iter(selected), "")
    if not chart:
        return {"status": "BLOCKED", "reason": "no selected live node"}
    chosen = selected[chart]
    run_id = f"{base_run_id}-page2"
    run_cache.create_generation(run_id, "products")
    run_cache.activate_generation(run_id, "products")
    fp._RUN_ID = run_id
    before = len(client.requests)
    client.chart = chart
    client.node_id = str(chosen["node_id"])
    for retry in range(CHART_NODE_RETRY_ATTEMPTS):
        with fp._seen_lock:
            fp._seen_asins.clear()
        status, error, found, attempts = fp.process_node(
            chosen,
            [chart],
            review_max=0,
            min_list_size=0,
            client=client,
            price_min=999999,
            max_pages=2,
            delay=0.0,
            detail_filters={},
            list_limit=0,
        )
        if status != "error":
            break
    request_slice = client.requests[before:]
    requests = [row for row in request_slice if row.get("phase") == "LIST"]
    urls = [row["url"] for row in requests]
    page2 = [url for url in urls if re.search(r"[?&]pg=2(?:&|$)", url)]
    page3 = [url for url in urls if re.search(r"[?&]pg=3(?:&|$)", url)]
    failures = []
    # 页数探测允许因不可能筛选导致无商品，但 process 本身不能是 error。
    if status == "error":
        failures.append(f"PROCESS_STATUS_NOT_DONE:{status}")
    if error and status == "error":
        failures.append(f"PROCESS_ERROR:{error}")
    failures.extend(request_policy_failures(
        requests,
        max_pages=2,
        require_page2=True,
        require_page2_success=True,
    ))
    return {
        "chart": chart,
        "run_id": run_id,
        "process_status": status,
        "process_error": error,
        "products_found_after_impossible_filter": found,
        "attempts": attempts,
        "list_urls": urls,
        "page2_requested": bool(page2),
        "page3_requested": bool(page3),
        "page2_ok": any(
            row.get("ok")
            and row.get("status_code") == 200
            and int(row.get("response_bytes") or 0) > 0
            and not row.get("captcha")
            for row in requests
            if re.search(r"[?&]pg=2(?:&|$)", row.get("url") or "")
        ),
        "detail_requests": len([
            row for row in request_slice if row.get("phase") == "DETAIL"
        ]),
        "failures": failures,
        "status": "PASS" if not failures else "FAIL",
    }


def expected_descendants(test_db: Path, root_id: str) -> set[str]:
    rows = query_rows(
        test_db,
        """WITH RECURSIVE sub(node_id) AS (
               SELECT node_id FROM categories WHERE site='US' AND node_id=?
               UNION
               SELECT c.node_id FROM categories c
               JOIN sub s ON c.parent_node_id=s.node_id
               WHERE c.site='US'
           )
           SELECT node_id FROM sub WHERE node_id IS NOT NULL""",
        (root_id,),
    )
    return {str(row["node_id"]) for row in rows}


def run_scope_test(fp, test_db: Path, selected: dict) -> dict:
    roots = query_rows(
        test_db,
        """SELECT c.node_id,c.name,c.depth,c.url
           FROM categories c
           WHERE c.site='US' AND c.node_id IS NOT NULL AND c.node_id<>''
             AND EXISTS (
                 SELECT 1 FROM categories child
                 WHERE child.site='US' AND child.parent_node_id=c.node_id
             )
           ORDER BY CASE WHEN c.depth=2 THEN 0 ELSE 1 END,
                    COALESCE(c.child_count,999999),c.name
           LIMIT 1""",
    )
    chosen = roots[0] if roots else next(iter(selected.values()), None)
    if not chosen:
        return {"status": "BLOCKED", "reason": "no selected node"}
    root = str(chosen["node_id"])
    expected = expected_descendants(test_db, root)
    exact_rows = fp.get_descendant_nodes(
        [root], list(ALL_CHARTS), site="US", include_descendants=False
    )
    actual_rows = fp.get_descendant_nodes(
        [root], list(ALL_CHARTS), site="US", include_descendants=True
    )
    exact = [str(row["node_id"]) for row in exact_rows]
    actual = [str(row["node_id"]) for row in actual_rows]
    duplicates = sorted({node for node in actual if actual.count(node) > 1})
    missing = sorted(expected - set(actual))
    unexpected = sorted(set(actual) - expected)
    descendant_only = sorted(expected - {root})
    overlap_roots = [root, descendant_only[0]] if descendant_only else [root]
    overlap_rows = fp.get_descendant_nodes(
        overlap_roots, list(ALL_CHARTS), site="US", include_descendants=True
    )
    overlap = [str(row["node_id"]) for row in overlap_rows]
    overlap_duplicates = sorted({node for node in overlap if overlap.count(node) > 1})
    overlap_missing = sorted(expected - set(overlap))
    overlap_unexpected = sorted(set(overlap) - expected)
    expected_depth3 = {
        str(row["node_id"]) for row in query_rows(
            test_db,
            """SELECT DISTINCT node_id FROM categories
               WHERE site='US' AND depth=3 AND node_id IS NOT NULL AND node_id<>''""",
        )
    }
    actual_depth3_rows = fp.get_nodes_by_depth([3], site="US")
    actual_depth3 = [str(row["node_id"]) for row in actual_depth3_rows]
    depth_duplicates = sorted({
        node for node in actual_depth3 if actual_depth3.count(node) > 1
    })
    depth_missing = sorted(expected_depth3 - set(actual_depth3))
    depth_unexpected = sorted(set(actual_depth3) - expected_depth3)
    failures = []
    if exact != [root]:
        failures.append("EXACT_ROOT_MISMATCH")
    if missing:
        failures.append("DESCENDANTS_MISSING")
    if unexpected:
        failures.append("UNEXPECTED_DESCENDANTS")
    if duplicates:
        failures.append("DESCENDANT_DUPLICATES")
    if overlap_missing or overlap_unexpected:
        failures.append("OVERLAP_SCOPE_MISMATCH")
    if overlap_duplicates:
        failures.append("OVERLAP_DUPLICATES")
    if depth_missing or depth_unexpected:
        failures.append("DEPTH_SCOPE_MISMATCH")
    if depth_duplicates:
        failures.append("DEPTH_DUPLICATES")
    return {
        "root_id": root,
        "expected_node_ids": sorted(expected),
        "actual_node_ids": actual,
        "exact_node_ids": exact,
        "missing_node_ids": missing,
        "unexpected_node_ids": unexpected,
        "duplicate_node_ids": duplicates,
        "overlap_root_ids": overlap_roots,
        "overlap_actual_node_ids": overlap,
        "overlap_missing_node_ids": overlap_missing,
        "overlap_unexpected_node_ids": overlap_unexpected,
        "overlap_duplicate_node_ids": overlap_duplicates,
        "depth3_expected_count": len(expected_depth3),
        "depth3_actual_count": len(actual_depth3),
        "depth3_missing_node_ids": depth_missing,
        "depth3_unexpected_node_ids": depth_unexpected,
        "depth3_duplicate_node_ids": depth_duplicates,
        "failures": failures,
        "status": "PASS" if not failures else "FAIL",
    }


def write_report(report: dict, paths: dict[str, Path]) -> tuple[Path, Path, Path]:
    report_dir = paths["report_dir"]
    json_path = report_dir / "report.json"
    md_path = report_dir / "report.md"
    ledger_path = report_dir / "request_ledger.jsonl"
    json_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    lines = [
        f"# Ranking E2E {report['run_id']}",
        "",
        f"- Final status: **{report.get('final_status')}**",
        f"- Started: {report.get('started_at')}",
        f"- Finished: {report.get('finished_at')}",
        f"- Runtime: `{report.get('runtime_root')}`",
        f"- Formal storage unchanged: `{report.get('formal_storage_unchanged')}`",
        "",
        "## Chart results",
    ]
    for chart in report.get("requested_charts") or []:
        item = (report.get("chart_results") or {}).get(chart) or {}
        lines.append(
            f"- {chart}: {item.get('status', 'NOT_EXECUTED')} "
            f"(cache={item.get('cache_rows', 0)}, detail_ok={item.get('detail_success', 0)})"
        )
    if report.get("failures"):
        lines.extend(["", "## Failures", *[f"- {x}" for x in report["failures"]]])
    if report.get("blocked"):
        lines.extend(["", "## Blocked", *[f"- {x}" for x in report["blocked"]]])
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    if paths["ledger"].exists():
        shutil.copy2(paths["ledger"], ledger_path)
    else:
        ledger_path.write_text("", encoding="utf-8")
    return json_path, md_path, ledger_path


def persist_runtime_diagnostics(report: dict, paths: dict[str, Path]) -> None:
    """复制 runtime 诊断到稳定 report，并修正报告中的证据路径。"""
    source_root = paths["runtime"] / "diagnostics"
    if not source_root.is_dir() or paths["runtime"] == paths["report_dir"]:
        return
    shutil.copytree(
        source_root,
        paths["report_dir"] / "diagnostics",
        dirs_exist_ok=True,
    )
    runtime = paths["runtime"].resolve()
    for discovery in (report.get("discovery") or {}).values():
        for attempt in discovery.get("attempts") or []:
            raw_path = attempt.get("evidence_path")
            if not raw_path:
                continue
            try:
                relative = Path(raw_path).resolve().relative_to(runtime)
            except ValueError:
                continue
            stable_path = paths["report_dir"] / relative
            if stable_path.is_file():
                attempt["evidence_path"] = str(stable_path)
        raw_dir = discovery.get("evidence_dir")
        if raw_dir:
            try:
                relative_dir = Path(raw_dir).resolve().relative_to(runtime)
            except ValueError:
                continue
            discovery["evidence_dir"] = str(paths["report_dir"] / relative_dir)


def determine_final_status(report: dict) -> str:
    """SQLite 四榜单车道判定。PG 仅写 report['pg']，不得阻塞本车道 PASS。"""
    if report.get("formal_storage_unchanged") is False:
        return "FAIL"
    if report.get("failures"):
        return "FAIL"
    if report.get("blocked"):
        return "BLOCKED"
    requested = report.get("requested_charts") or []
    results = report.get("chart_results") or {}
    if not requested or any((results.get(chart) or {}).get("status") != "PASS" for chart in requested):
        return "NOT_EXECUTED"
    if (report.get("page_test") or {}).get("status") != "PASS":
        return "FAIL"
    if (report.get("scope_test") or {}).get("status") != "PASS":
        return "FAIL"
    return "PASS"


def exit_code_for(report: dict) -> int:
    if report.get("formal_storage_unchanged") is False:
        return EXIT_FORMAL_CHANGED
    if report.get("tool_error"):
        return EXIT_TOOL_ERROR
    status = report.get("final_status")
    if status == "PASS":
        return EXIT_PASS
    if status in ("BLOCKED", "NOT_EXECUTED"):
        return EXIT_BLOCKED
    if report.get("safety_failure"):
        return EXIT_SAFETY
    return EXIT_FAIL


def main(argv=None) -> int:
    args = parse_args(argv)
    run_id = (
        datetime.now(timezone.utc).strftime("RANK-E2E-%Y%m%dT%H%M%SZ-")
        + secrets.token_hex(3)
    )
    before = formal_storage_snapshot()
    paths = None
    client = None
    report: dict[str, Any] = {
        "run_id": run_id,
        "started_at": utc_now(),
        "finished_at": None,
        "python": sys.version,
        "git": git_state(),
        "requested_charts": [],
        "environment": {
            "site": args.site,
            "testing": True,
            "db_backend": "sqlite",
            "result_mode": "run_cache",
            "proxy_required": True,
            "direct_fallback": False,
        },
        "formal_storage_before": before,
        "formal_storage_after": None,
        "formal_storage_unchanged": None,
        "proxy_preflight": {},
        "discovery": {},
        "chart_results": {},
        "page_test": {},
        "scope_test": {},
        "database_assertions": {},
        "proxy_ledger": {},
        "pg": {
            "status": "NOT_EXECUTED" if args.skip_pg else "BLOCKED_NOT_IMPLEMENTED",
            "blocks_sqlite_pass": False,
            "note": "PG lane is informational only; SQLite four-chart PASS is independent",
        },
        "cleanup": {},
        "failures": [],
        "blocked": [],
        "final_status": "NOT_EXECUTED",
    }
    pool = None
    try:
        charts = validate_args(args)
        report["requested_charts"] = charts
        # PG 未实现不得写入 blocked，否则 SQLite 车道永远无法 PASS。
        if not FORMAL_DB.is_file():
            raise SafetyError(f"formal database missing: {FORMAL_DB}")
        paths = prepare_runtime(args, run_id)
        report["runtime_root"] = str(paths["runtime"])
        report["report_dir"] = str(paths["report_dir"])
        configure_environment(paths, run_id)

        # 生产模块必须在环境重定向后导入。
        if str(ROOT) not in sys.path:
            sys.path.insert(0, str(ROOT))
        import config
        import fetch_products as fp
        import product_run_cache as run_cache

        assert_module_paths(paths, fp, config, run_cache)
        config.assert_testing_paths_safe(
            db_path=str(paths["test_db"]), cache_path=str(paths["cache_db"])
        )
        run_cache.ensure_schema()
        mp = config.get_marketplace("US")
        fp._SITE = "US"
        fp._DOMAIN = mp["domain"]
        fp._LANG = mp["lang"]
        fp._CURRENCY = mp["currency"]
        fp._DECIMAL_SEP = mp["decimal_sep"]
        fp._RATING_PAT = mp["rating_pattern"]
        fp._RESULTS_PAT = mp["results_pattern"]

        # 无网络安全演练仍须证明路径隔离，但不得进入 E2E PASS。
        if args.no_live:
            report["blocked"].append("NOT_EXECUTED_NO_LIVE")
        else:
            pool, proxy_result = proxy_preflight(fp, args)
            report["proxy_preflight"] = proxy_result
            client = RecordingClient(
                fp.WorkerProxyClient(
                    pool, worker_id=91, warmup=False, verify_exit=True,
                ),
                paths["ledger"],
            )
            selected = {}
            for chart in charts:
                candidates = candidate_rows(paths["test_db"], chart, args.candidate_limit)
                chosen, attempts = discover_live_node(
                    fp,
                    client,
                    chart,
                    candidates,
                )
                failure_counts = discovery_failure_counts(attempts)
                report["discovery"][chart] = {
                    "candidate_count": len(candidates),
                    "attempts": attempts,
                    "selected": chosen,
                    "failure_class_counts": failure_counts,
                }
                if not chosen:
                    report["blocked"].append(f"BLOCKED_NO_LIVE_NODE:{chart}")
                    failures = ["NO_LIVE_NODE"]
                    result_status = "BLOCKED"
                    report["chart_results"][chart] = {
                        "chart": chart,
                        "status": result_status,
                        "failures": failures,
                        "failure_class_counts": failure_counts,
                    }
                    continue
                selected[chart] = chosen
                result = run_chart(fp, run_cache, client, chart, chosen, args, run_id)
                report["chart_results"][chart] = result
                report["failures"].extend(
                    f"{chart}:{failure}" for failure in result["failures"]
                )

            report["page_test"] = run_page_test(
                fp, run_cache, client, selected, run_id
            )
            report["failures"].extend(
                f"page_test:{failure}"
                for failure in report["page_test"].get("failures") or []
            )
            report["scope_test"] = run_scope_test(fp, paths["test_db"], selected)
            report["failures"].extend(
                f"scope_test:{failure}"
                for failure in report["scope_test"].get("failures") or []
            )
            unique_ips = verified_exit_ips_from_requests(client.requests)
            ledger_failures = ledger_proxy_failures(
                client.requests, min_unique_ips=args.min_unique_ips,
            )
            report["proxy_ledger"] = {
                "request_count": len(client.requests),
                "unique_verified_exit_ips": unique_ips,
                "unique_exit_ips": unique_ips,
                "failures": ledger_failures,
                "evidence_complete": not ledger_failures,
            }
            report["failures"].extend(
                f"proxy_ledger:{failure}" for failure in ledger_failures
            )

        formal_failures, formal_counts = assert_formal_tables_empty(paths["test_db"])
        report["database_assertions"] = {
            "formal_test_table_counts": formal_counts,
            "run_cache_exists": paths["cache_db"].is_file(),
            "run_cache_path": str(paths["cache_db"]),
        }
        report["failures"].extend(formal_failures)
    except SafetyError as exc:
        report["safety_failure"] = True
        report["failures"].append(f"SAFETY:{exc}")
    except ExternalBlocked as exc:
        report["blocked"].append(str(exc))
    except Exception as exc:
        report["failures"].append(f"TOOL_ERROR:{type(exc).__name__}:{exc}")
        report["tool_error"] = True
    finally:
        if client is not None:
            try:
                close_info = client.close()
                report["cleanup"]["client_closed"] = bool(
                    close_info.get("client_closed")
                )
                pool_stop = close_info.get("pool_stop") or {}
                report["cleanup"]["pool_stop"] = pool_stop
                report["cleanup"]["pool_reload_stopped"] = bool(
                    pool_stop.get("ok") and (
                        not pool_stop.get("had_live_reload_thread")
                        or pool_stop.get("stop_called")
                    )
                    and not pool_stop.get("thread_alive_after")
                )
                if not pool_stop.get("ok", True):
                    report["failures"].append(
                        f"CLEANUP_POOL_RELOAD_FAILED:{pool_stop.get('error')}"
                    )
                    report["cleanup"]["cleanup_failure"] = pool_stop.get("error")
            except Exception as exc:
                report["cleanup"]["client_close_error"] = str(exc)
                report["cleanup"]["pool_reload_stopped"] = False
                report["failures"].append(f"CLIENT_CLOSE_FAILED:{exc}")
        elif pool is not None:
            try:
                pool_stop = stop_pool(pool)
                report["cleanup"]["pool_stop"] = pool_stop
                report["cleanup"]["pool_reload_stopped"] = bool(
                    pool_stop.get("ok")
                    and (
                        not pool_stop.get("had_live_reload_thread")
                        or pool_stop.get("stop_called")
                    )
                    and not pool_stop.get("thread_alive_after")
                )
                if not pool_stop.get("ok", True):
                    report["failures"].append(
                        f"CLEANUP_POOL_RELOAD_FAILED:{pool_stop.get('error')}"
                    )
                    report["cleanup"]["cleanup_failure"] = pool_stop.get("error")
            except Exception as exc:
                report["cleanup"]["pool_stop_error"] = str(exc)
                report["cleanup"]["pool_reload_stopped"] = False
                report["failures"].append(f"POOL_STOP_FAILED:{exc}")

        after = formal_storage_snapshot()
        report["formal_storage_after"] = after
        report["formal_storage_unchanged"] = not formal_storage_changed(before, after)
        if not report["formal_storage_unchanged"]:
            report["failures"].append("FORMAL_STORAGE_CHANGED")
        report["finished_at"] = utc_now()
        report["final_status"] = determine_final_status(report)

        if paths is None:
            emergency_root = (
                Path(args.report_dir).resolve()
                if args.report_dir
                else Path(tempfile.gettempdir()) / "amz-ranking-e2e-reports"
            )
            emergency = emergency_root / run_id
            emergency.mkdir(parents=True, exist_ok=True)
            paths = {
                "runtime": emergency,
                "report_dir": emergency,
                "ledger": emergency / "request_ledger.jsonl",
            }
            report["runtime_root"] = str(emergency)
            report["report_dir"] = str(emergency)
        persist_runtime_diagnostics(report, paths)
        json_path, md_path, ledger_path = write_report(report, paths)
        report["cleanup"]["report_json"] = str(json_path)
        report["cleanup"]["report_md"] = str(md_path)
        report["cleanup"]["request_ledger"] = str(ledger_path)
        # 写入包含最终产物路径的最终版本。
        json_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        code = exit_code_for(report)
        print(json.dumps({
            "run_id": run_id,
            "final_status": report["final_status"],
            "exit_code": code,
            "report": str(json_path),
            "runtime": report.get("runtime_root"),
            "failures": report["failures"],
            "blocked": report["blocked"],
        }, ensure_ascii=False, indent=2))
        if (
            report["final_status"] == "PASS"
            and not args.keep_temp
            and paths["runtime"] != paths["report_dir"]
        ):
            shutil.rmtree(paths["runtime"], ignore_errors=True)
        return code


if __name__ == "__main__":
    raise SystemExit(main())
