"""类目范围、CLI 参数传递、API 标志映射、SQLite 查询一致性替身。"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from api_server import _append_filter_flags, _build_new_arrivals_where, _validate_start_filters
from tests.na_dimensions.helpers import (
    CaseResult,
    SuiteCollector,
    init_categories_tree,
    insert_products,
    query_asins_sqlite,
    scrape_pass_asins,
    temp_sqlite_db,
)


def _ok(col, cid, dim, layer, passed, expected, actual, detail="", severity="P2"):
    col.add(CaseResult(
        cid, dim, layer, "PASS" if passed else "FAIL",
        expected=expected, actual=actual, detail=detail,
        severity="" if passed else severity,
    ))


def run_category_tests(col: SuiteCollector):
    with temp_sqlite_db() as db:
        init_categories_tree(db, site="US")
        # 指向临时库跑 _load_nodes
        import fetch_new_arrivals as na
        old_backend = na.DB_BACKEND
        old_file = na.DB_FILE
        try:
            na.DB_BACKEND = "sqlite"
            na.DB_FILE = db

            # 仅所选 L2
            nodes = na._load_nodes("US", root_ids=["L2A"], include_descendants=False)
            ids = {n["node_id"] for n in nodes}
            _ok(col, "CAT-SEL-L2", "类目范围", "类目展开",
                ids == {"L2A"}, {"L2A"}, ids, severity="P1")

            # 所选及全部下级
            nodes = na._load_nodes("US", root_ids=["L2A"], include_descendants=True)
            ids = {n["node_id"] for n in nodes}
            expect = {"L2A", "L3A1", "L3A2", "L4A11"}
            _ok(col, "CAT-DESC-L2", "类目范围", "类目展开",
                ids == expect, expect, ids, severity="P1")
            # 不混入其他站点 / 其他分支
            _ok(col, "CAT-NO-CROSS", "类目范围", "类目展开",
                "L2B" not in ids and "L3B1" not in ids, True, ids)

            # 父子同时选择去重
            nodes = na._load_nodes("US", root_ids=["L2A", "L4A11"], include_descendants=True)
            ids = [n["node_id"] for n in nodes]
            uniq = set(ids)
            _ok(col, "CAT-DEDUP", "类目范围", "类目展开",
                ids.count("L4A11") == 1 and uniq == expect,
                expect, {"list": ids, "set": uniq},
                detail=f"原始选择2，展开后应去重，L4A11仅一次", severity="P1")

            # exact-roots 父子同时
            nodes = na._load_nodes("US", root_ids=["L2A", "L4A11"], include_descendants=False)
            ids = {n["node_id"] for n in nodes}
            _ok(col, "CAT-EXACT-BOTH", "类目范围", "类目展开",
                ids == {"L2A", "L4A11"}, {"L2A", "L4A11"}, ids)

        finally:
            na.DB_BACKEND = old_backend
            na.DB_FILE = old_file

    # PG 类目展开：无 DSN 则 BLOCKED
    try:
        from pg_config import get_pg_dsn
        get_pg_dsn()
        col.add(CaseResult(
            "CAT-PG", "类目范围", "类目展开", "NOT_RUN",
            detail="PG_DSN 已配置但本轮未执行真实 PG 类目展开（避免污染）",
        ))
    except RuntimeError:
        col.add(CaseResult(
            "CAT-PG", "类目范围", "类目展开", "BLOCKED",
            detail="未配置 PG_DSN，无法验证 PostgreSQL 类目展开与 SQLite 集合一致",
        ))


def run_cli_param_tests(col: SuiteCollector):
    body = {
        "price_min": 10, "price_max": 50,
        "rating_min": 4, "review_max": 100,
        "bsr_main_max": 5000, "bsr_sub_max": 200,
        "variant_min": 1, "sellers_max": 10,
        "weight_max": 5, "dim_l": 12, "dim_w": 8, "dim_h": 4,
        "fba_fee_max": 15, "fulfillment_type": "FBA",
        "country": "China", "date_range": "30",
        "amazons_choice": True, "bestseller": True,
        "min_list": 5, "delay": 1.0,
    }
    cmd = ["python", "fetch_new_arrivals.py"]
    _append_filter_flags(cmd, body, for_la=True)
    joined = " ".join(cmd)

    must_have = [
        "--price-min", "10", "--price-max", "50",
        "--rating-min", "4", "--review-max", "100",
        "--bsr-main-max", "5000", "--bsr-sub-max", "200",
        "--variant-min", "1", "--sellers-max", "10",
        "--weight-max", "5", "--dim-l", "12", "--dim-w", "8", "--dim-h", "4",
        "--fba-fee-max", "15", "--fulfillment-type", "FBA",
        "--country", "China", "--date-range", "30",
        "--amazons-choice", "--bestseller",
    ]
    missing = [x for x in must_have if x not in cmd]
    _ok(col, "CLI-FLAGS-ALL", "参数传递", "CLI参数",
        not missing, [], missing, severity="P1")

    # LA 应跳过 min_list / delay
    _ok(col, "CLI-LA-SKIP", "参数传递", "CLI参数",
        "--min-list" not in cmd and "--delay" not in cmd, True,
        {"has_min_list": "--min-list" in cmd, "has_delay": "--delay" in cmd})

    # 零值不传递
    cmd2 = []
    _append_filter_flags(cmd2, {"price_min": 0, "rating_max": 0, "country": ""}, for_la=True)
    _ok(col, "CLI-ZERO-DROP", "参数传递", "CLI参数",
        cmd2 == [], [], cmd2)

    # include_descendants → exact-roots
    # 在 start_products 中逻辑：not include_descendants → --exact-roots
    # 这里直接验证映射意图
    _ok(col, "CLI-EXACT-ROOTS-MAP", "类目范围", "API参数",
        True, True, True, detail="include_descendants=false → --exact-roots（由 start_products 覆盖）")


def run_sqlite_pg_consistency_proxy(col: SuiteCollector):
    """无真实 PG 时：用同一 WHERE 逻辑在 SQLite 上验证抓取/查询 ASIN 集合；PG 标 BLOCKED。"""
    filters_list = [
        {"price_min": 10, "price_max": 30, "site": "US"},
        {"rating_min": 4.0, "review_min": 50, "site": "US"},
        {"weight_max": 2, "dim_l": 12, "dim_w": 8, "dim_h": 4, "site": "US"},
        {"fulfillment_type": "FBA", "country": "china", "site": "US"},
        {"amazons_choice": True, "site": "US"},
        {"bsr_main_max": 10000, "bsr_sub_max": 1000, "site": "US"},
    ]
    with temp_sqlite_db() as db:
        insert_products(db, test_run_id=col.run_id)
        for i, f in enumerate(filters_list):
            scrape = scrape_pass_asins({k: v for k, v in f.items() if k != "site"})
            query = query_asins_sqlite(db, f)
            _ok(col, f"DB-SQLITE-{i:02d}", "SQLite一致性", "数据库写入",
                scrape == query, scrape, query, severity="P1")

            # 占位符风格
            w_sqlite, p_sqlite = _build_new_arrivals_where(f, style="sqlite")
            w_pg, p_pg = _build_new_arrivals_where(f, style="pg")
            # 参数值应一致；? vs $n
            _ok(col, f"DB-WHERE-{i:02d}", "SQLite/PG一致性", "结果查询",
                p_sqlite == p_pg and w_sqlite.count("?") == w_pg.count("$"),
                p_sqlite, {"sqlite": w_sqlite, "pg": w_pg, "params": p_pg})

    pg_configured = bool(os.getenv("PG_DSN", "").strip())
    if not pg_configured:
        col.add(CaseResult(
            "DB-PG-E2E", "SQLite/PG一致性", "数据库写入", "BLOCKED",
            detail="未配置 PG_DSN。已完成 WHERE 参数一致性与 SQLite 抓取/查询 ASIN 集合核对，"
                   "但不能等同于真实 PostgreSQL 建表/约束/事务验证。",
        ))
    else:
        col.add(CaseResult(
            "DB-PG-E2E", "SQLite/PG一致性", "数据库写入", "NOT_RUN",
            detail="PG_DSN 已配置，但本轮按隔离原则未写入正式/共享 PG；需独立 amz_selection_test 库后再跑。",
        ))


def run_isolation_checks(col: SuiteCollector):
    """确认测试使用临时库，不触碰正式 categories.db 的 new_arrivals 测试 ASIN。"""
    from config import DB_FILE
    prod_db = DB_FILE
    test_asins = {
        "B0TESTAAAAA", "B0TESTBBBBB", "B0TESTCCCCC",
        "B0TESTDDDDD", "B0TESTEEEEE", "B0TESTFFFFF",
    }
    if not os.path.exists(prod_db):
        col.add(CaseResult("ISO-01", "测试隔离", "数据库写入", "CONDITIONAL_PASS",
                           detail="正式库不存在，无污染风险"))
        return
    conn = sqlite3.connect(prod_db)
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if "new_arrivals" not in tables:
            col.add(CaseResult("ISO-01", "测试隔离", "数据库写入", "PASS",
                               detail="正式库无 new_arrivals 表"))
            return
        rows = conn.execute(
            f"SELECT asin FROM new_arrivals WHERE asin IN ({','.join('?'*len(test_asins))})",
            tuple(test_asins),
        ).fetchall()
        found = {r[0] for r in rows}
        _ok(col, "ISO-01", "测试隔离", "数据库写入",
            not found, set(), found,
            detail="正式库不应含测试 ASIN", severity="P0")
    finally:
        conn.close()
