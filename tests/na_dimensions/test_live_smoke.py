"""真实 Amazon 五场景冒烟：必须测试库 + 显式授权环境变量。"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tests.na_dimensions.helpers import CaseResult, SuiteCollector

SCENARIOS = [
    ("S1-BASELINE", "无筛选基线", {}),
    ("S2-LOOSE", "单个宽松条件", {"--price-max": "500"}),
    ("S3-MISS", "明显不命中", {"--weight-max": "0.001"}),
    ("S4-COMBO", "多维组合", {
        "--price-min": "5", "--price-max": "200",
        "--rating-min": "3", "--review-max": "100000",
        "--weight-max": "50", "--fulfillment-type": "FBA",
    }),
    ("S5-FULL", "全维度宽松", {
        "--price-min": "1", "--price-max": "999",
        "--rating-min": "1", "--rating-max": "5",
        "--review-min": "0", "--review-max": "1000000",
        "--bsr-main-max": "9999999", "--bsr-sub-max": "9999999",
        "--variant-max": "999", "--sellers-max": "999",
        "--weight-max": "100", "--dim-l": "100", "--dim-w": "100", "--dim-h": "100",
        "--fba-fee-max": "999", "--date-range": "3650",
    }),
]


def run_live_smoke(col: SuiteCollector):
    if os.getenv("RUN_LIVE_NA", "").strip() not in ("1", "true", "TRUE", "yes"):
        for sid, title, _ in SCENARIOS:
            col.add(CaseResult(
                f"LIVE-{sid}", "真实抓取", "运行控制", "BLOCKED",
                detail=(
                    f"场景「{title}」未执行。请同时设置：\n"
                    "  RUN_LIVE_NA=1\n"
                    "  LIVE_NA_ROOTS=<node_id1[,node_id2]>\n"
                    "  LIVE_NA_SITE=US（可选）\n"
                    "  LIVE_NA_MAX_PAGES=1 LIVE_NA_MAX_DETAILS=20\n"
                    "并将 DB_FILE 指向测试库（套件会自动用 data/categories_test_live.db）。"
                    "禁止写入正式 categories.db。"
                ),
            ))
        return

    roots = [r.strip() for r in os.getenv("LIVE_NA_ROOTS", "").split(",") if r.strip()]
    if not roots:
        col.add(CaseResult(
            "LIVE-NEED-ROOTS", "真实抓取", "运行控制", "BLOCKED",
            detail="已开启 RUN_LIVE_NA 但缺少 LIVE_NA_ROOTS。请提供最多 2 个 US 类目 node_id。",
        ))
        return

    roots = roots[:2]
    site = os.getenv("LIVE_NA_SITE", "US").upper()
    max_pages = int(os.getenv("LIVE_NA_MAX_PAGES", "1") or "1")
    max_details = int(os.getenv("LIVE_NA_MAX_DETAILS", "20") or "20")
    test_db = ROOT / "data" / "categories_test_live.db"
    run_id = col.run_id

    # 准备测试库：复制类目表结构（只读从正式库拷 categories）
    _prepare_test_db(test_db)

    results = []
    for sid, title, extra in SCENARIOS:
        started = time.time()
        cmd = [
            sys.executable, "-u", str(ROOT / "fetch_new_arrivals.py"),
            "--site", site, "--roots", *roots,
            "--max-pages", str(max_pages), "--exact-roots",
        ]
        for k, v in extra.items():
            cmd += [k, v]

        env = os.environ.copy()
        env["DB_BACKEND"] = "sqlite"
        env["DB_FILE"] = str(test_db)
        env["AMZ_DB_FILE"] = str(test_db)
        env["TEST_RUN_ID"] = run_id
        env["AMZ_MAX_DETAILS"] = str(max_details)
        # 清理测试库本场景前的 new_arrivals，便于统计「本场景新增」
        _clear_na(test_db)

        # 硬超时 20 分钟
        try:
            proc = subprocess.run(
                cmd, cwd=str(ROOT), env=env,
                capture_output=True, timeout=20 * 60,
            )
            def _dec(b: bytes | None) -> str:
                if not b:
                    return ""
                return b.decode("utf-8", errors="replace")

            stop = f"exit={proc.returncode}"
            log_tail = (_dec(proc.stdout)[-2000:] + _dec(proc.stderr)[-1000:])
        except subprocess.TimeoutExpired:
            stop = "timeout_20min"
            log_tail = "TIMEOUT"
            proc = None

        elapsed = time.time() - started
        stats = _read_live_stats(test_db, site)
        log_stats = _parse_log_stats(ROOT / "data" / "fetch_new_arrivals.log")
        payload = {
            "site": site,
            "roots": roots,
            "category_names": {
                "11965981": "Accordion Accessories",
                "21490696011": "Activity Cubes",
            },
            "scope": "exact-roots(仅所选)",
            "pages": max_pages,
            "list_items": log_stats.get("asins_unique"),
            "detail_requests": log_stats.get("detail_phase_count"),
            "parse_ok": log_stats.get("details_ok"),
            "filter_pass": log_stats.get("details_ok"),
            "filter_reject": log_stats.get("details_filtered"),
            "network_errors": log_stats.get("details_error"),
            "captcha": log_stats.get("captcha"),
            "http_429": log_stats.get("http_429", 0),
            "db_inserted": stats.get("count"),
            "db_dup": 0,
            "asins": stats.get("asins", []),
            "elapsed_sec": round(elapsed, 1),
            "stop": stop,
            "max_details": max_details,
            "log_tail": log_tail[:500],
        }
        results.append((sid, title, payload))
        # 不要求固定数量；进程正常结束即主体 PASS，异常 FAIL
        ok = proc is not None and proc.returncode == 0
        col.add(CaseResult(
            f"LIVE-{sid}", "真实抓取", "运行控制",
            "PASS" if ok else "FAIL",
            "进程正常结束", payload,
            detail=json.dumps(payload, ensure_ascii=False)[:800],
            severity="" if ok else "P1",
        ))

    col.meta = getattr(col, "meta", {})
    col.meta["live"] = results


def _clear_na(test_db: Path):
    import sqlite3
    if not test_db.exists():
        return
    conn = sqlite3.connect(str(test_db))
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        if "new_arrivals" in tables:
            conn.execute("DELETE FROM new_arrivals")
            conn.commit()
    finally:
        conn.close()


def _prepare_test_db(test_db: Path):
    import sqlite3
    from config import DB_FILE as PROD_DB
    test_db.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(test_db))
    conn.execute(
        """CREATE TABLE IF NOT EXISTS categories (
            node_id TEXT, name TEXT, depth INTEGER,
            parent_node_id TEXT, site TEXT, PRIMARY KEY(node_id, site)
        )"""
    )
    # 若正式库存在则拷贝 categories（只读）
    if os.path.exists(PROD_DB):
        # 避免覆盖测试写入：仅在 categories 空时拷贝
        n = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        if n == 0:
            prod = sqlite3.connect(PROD_DB)
            rows = prod.execute(
                "SELECT node_id, name, depth, parent_node_id, site FROM categories"
            ).fetchall()
            prod.close()
            conn.executemany(
                "INSERT OR IGNORE INTO categories VALUES (?,?,?,?,?)", rows
            )
    # new_arrivals 由脚本自建
    conn.commit()
    conn.close()


def _parse_log_stats(log_path: Path) -> dict:
    """从日志末尾解析最近一次抓取的关键计数。"""
    import re
    out = {
        "asins_unique": None, "captcha": None, "details_ok": None,
        "details_filtered": None, "details_error": None,
        "detail_phase_count": None, "saved": None, "http_429": 0,
    }
    if not log_path.exists():
        return out
    text = log_path.read_text(encoding="utf-8", errors="replace")
    # 取最后一次 P1/P2 块
    m = re.findall(
        r"\[P1 完成\].*?去重=(\d+),\s*captcha=(\d+)", text
    )
    if m:
        out["asins_unique"], out["captcha"] = int(m[-1][0]), int(m[-1][1])
    m2 = re.findall(
        r"Phase 2: (\d+) 个 ASIN", text
    )
    if m2:
        out["detail_phase_count"] = int(m2[-1])
    m3 = re.findall(
        r"\[P2 完成\].*?命中=(\d+),\s*过滤=(\d+),\s*入库=(\d+)", text
    )
    if m3:
        out["details_ok"] = int(m3[-1][0])
        out["details_filtered"] = int(m3[-1][1])
        out["saved"] = int(m3[-1][2])
    # details_error 在另一行
    m4 = re.findall(r"P2:.*?/ (\d+)失败", text)
    if m4:
        out["details_error"] = int(m4[-1])
    out["http_429"] = len(re.findall(r"\b429\b", text[-8000:]))
    return out


def _read_live_stats(test_db: Path, site: str) -> dict:
    import sqlite3
    if not test_db.exists():
        return {"count": 0, "asins": []}
    conn = sqlite3.connect(str(test_db))
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )}
        if "new_arrivals" not in tables:
            return {"count": 0, "asins": []}
        rows = conn.execute(
            "SELECT asin FROM new_arrivals WHERE site=? ORDER BY asin", (site,)
        ).fetchall()
        return {"count": len(rows), "asins": [r[0] for r in rows]}
    finally:
        conn.close()
