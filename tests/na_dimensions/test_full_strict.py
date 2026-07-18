"""严格全维度：含精选/畅销/日期；失败与缺失轮换无遗漏。"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tests.na_dimensions.filter_engine import pass_product, split_filters, with_missing
from tests.na_dimensions.fixtures.products import (
    DIM_SPEC,
    PRODUCTS,
    STRICT_DIMS,
    STRICT_FULL_FILTERS,
)
from tests.na_dimensions.helpers import (
    CaseResult,
    SuiteCollector,
    insert_products,
    query_asins_sqlite,
    temp_sqlite_db,
)
from fetch_new_arrivals import _pass_list_filters
from detail_parser import check_detail_filters
from tests.na_dimensions.fixtures.products import list_card, detail_fields


def run_full_strict(col: SuiteCollector) -> dict:
    g = PRODUCTS["G"]
    filters = dict(STRICT_FULL_FILTERS)
    dims = STRICT_DIMS

    # 列表 + 详情分别通过
    lf, df = split_filters(filters)
    list_ok = _pass_list_filters(list_card(g), lf)
    detail_ok = check_detail_filters(detail_fields(g), df)
    all_ok = list_ok and detail_ok
    col.add(CaseResult("FULL2-LIST", "全维度", "单维度",
                       "PASS" if list_ok else "FAIL", True, list_ok, severity="P1"))
    col.add(CaseResult("FULL2-DETAIL", "全维度", "单维度",
                       "PASS" if detail_ok else "FAIL", True, detail_ok, severity="P1"))
    col.add(CaseResult("FULL2-PASS", "全维度", "组合",
                       "PASS" if all_ok else "FAIL", True, all_ok, severity="P1"))

    # SQLite 写入 + 查询
    with temp_sqlite_db() as db:
        insert_products(db, [g], test_run_id=col.run_id)
        q = query_asins_sqlite(db, {**filters, "site": "US"})
        col.add(CaseResult(
            "FULL2-SQLITE-Q", "全维度", "结果查询",
            "PASS" if g["asin"] in q else "FAIL",
            {g["asin"]}, q, severity="P1",
        ))
        # 看板 API 层：直接调用 where 构建（不启服务）
        from api_server import _build_new_arrivals_where
        where, params = _build_new_arrivals_where({**filters, "site": "US"}, "sqlite")
        col.add(CaseResult(
            "FULL2-API-WHERE", "全维度", "结果查询",
            "PASS" if "is_amazon_choice" in where and "is_bestseller" in where and "listing_date" in where else "FAIL",
            "含精选/畅销/日期", where[:200],
        ))

    # PG 查询：无 PG_TEST_DSN → BLOCKED
    import os
    if not os.getenv("PG_TEST_DSN", "").strip():
        col.add(CaseResult(
            "FULL2-PG-Q", "全维度", "结果查询", "BLOCKED",
            detail="未提供 PG_TEST_DSN，无法在独立测试库验证 PostgreSQL 查询返回",
        ))
    else:
        col.add(CaseResult(
            "FULL2-PG-Q", "全维度", "结果查询", "NOT_RUN",
            detail="PG_TEST_DSN 已设置，由 test_pg_real 执行",
        ))

    # 单点失败轮换（其余维度保持满足）
    fail_covered = []
    for d in dims:
        spec = DIM_SPEC[d]
        if d == "ac":
            prod = {**g, "is_amazon_choice": 0}  # 仍保留畅销等
            ok = pass_product(prod, filters) is False
        elif d == "bs":
            prod = {**g, "is_bestseller": 0}
            ok = pass_product(prod, filters) is False
        else:
            ff = {**_merge_without(filters, spec), **spec["fail"]}
            ok = pass_product(g, ff) is False
        fail_covered.append(d if ok else None)
        col.add(CaseResult(
            f"FULL2-FAIL-{d}", d, "组合",
            "PASS" if ok else "FAIL", False, ok,
            detail=f"单点失败轮换:{d}", severity="" if ok else "P1",
        ))

    # 单点缺失轮换
    miss_covered = []
    for d in dims:
        spec = DIM_SPEC[d]
        mv = 0 if d in ("ac", "bs") else None
        mp = with_missing(g, spec["miss_fields"], mv)
        # dim_l/w/h 单轴缺失：其余轴保留
        if d in ("dim_l", "dim_w", "dim_h"):
            mp = dict(g)
            for f in spec["miss_fields"]:
                mp[f] = None
        ok = pass_product(mp, filters) is False
        miss_covered.append(d if ok else None)
        col.add(CaseResult(
            f"FULL2-MISS-{d}", d, "缺失值",
            "PASS" if ok else "FAIL", False, ok,
            detail=f"单点缺失轮换:{d}", severity="" if ok else "P1",
        ))

    fail_set = {x for x in fail_covered if x}
    miss_set = {x for x in miss_covered if x}
    missing_fail = [d for d in dims if d not in fail_set]
    missing_miss = [d for d in dims if d not in miss_set]
    stats = {
        "full_dim_count": len(dims),
        "fail_rotation_count": len(fail_set),
        "miss_rotation_count": len(miss_set),
        "uncovered_fail": missing_fail,
        "uncovered_miss": missing_miss,
    }
    col.add(CaseResult(
        "FULL2-ROTATION-STATS", "全维度", "组合",
        "PASS" if not missing_fail and not missing_miss else "FAIL",
        expected={"fail": len(dims), "miss": len(dims)},
        actual=stats,
        detail=(
            f"全维度数量:{len(dims)} 单点失败覆盖:{len(fail_set)} "
            f"单点缺失覆盖:{len(miss_set)} "
            f"未覆盖失败:{missing_fail} 未覆盖缺失:{missing_miss}"
        ),
        severity="" if not missing_fail and not missing_miss else "P0",
    ))
    col.meta = getattr(col, "meta", {})
    col.meta["full_strict"] = stats
    return stats


def _merge_without(filters: dict, spec: dict) -> dict:
    """去掉某因子 pass 相关键，便于写入 fail。"""
    drop = set(spec.get("pass", {})) | set(spec.get("fail", {}))
    return {k: v for k, v in filters.items() if k not in drop}
