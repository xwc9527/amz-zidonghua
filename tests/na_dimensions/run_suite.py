"""
最新到货全维度补全测试入口（RUN completion）。

  python tests/na_dimensions/run_suite.py

环境（可选）:
  PG_TEST_DSN=postgresql://...@.../amz_selection_test
  RUN_LIVE_NA=1 LIVE_NA_ROOTS=node1,node2
"""
from __future__ import annotations

import json
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from config import DB_FILE as PROD_DB
from tests.na_dimensions.helpers import CaseResult, SuiteCollector
from tests.na_dimensions.snapshot import compare_snapshots, snapshot_sqlite

# 既有层
from tests.na_dimensions.test_validation import run_validation_tests
from tests.na_dimensions.test_parsing import run_parsing_tests
from tests.na_dimensions.test_filters import run_single_dimension_tests, run_scrape_query_consistency
from tests.na_dimensions.test_fba import run_fba_tests
from tests.na_dimensions.test_category_and_cli import run_cli_param_tests, run_isolation_checks

# 补全层
from tests.na_dimensions.test_pairwise_full import run_pairwise_full
from tests.na_dimensions.test_strong_complete import run_strong_complete
from tests.na_dimensions.test_full_strict import run_full_strict
from tests.na_dimensions.test_param_matrix import run_param_matrix
from tests.na_dimensions.test_category_full import run_category_full
from tests.na_dimensions.test_origin_html import run_origin_html
from tests.na_dimensions.test_pg_real import run_pg_real
from tests.na_dimensions.test_live_smoke import run_live_smoke


def _safe(col: SuiteCollector, name: str, fn):
    try:
        return fn(col)
    except Exception as e:
        col.add(CaseResult(
            f"SUITE-{name}", "测试框架", "运行控制", "FAIL",
            expected="无异常", actual=str(e),
            detail=traceback.format_exc()[-1000:],
            severity="P0",
        ))
        return None


def _root_causes(fails: list) -> list[dict]:
    """将失败用例归并为独立根因。"""
    groups = {}
    for r in fails:
        if "NAN" in r.case_id.upper() or "INF" in r.case_id.upper() or "nan" in (r.detail or "").lower() \
                or "inf" in (r.detail or "").lower() or r.case_id.startswith("VAL-NAN") \
                or r.case_id.startswith("VAL-INF") or r.case_id.startswith("VAL-NINF") \
                or "PAR-START-nan" in r.case_id or "PAR-START-pos_inf" in r.case_id \
                or "PAR-START-neg_inf" in r.case_id or "PAR-START-str_" in r.case_id \
                or "PAR-NOPROC-nan" in r.case_id or "PAR-NOPROC-pos_inf" in r.case_id \
                or "PAR-NOPROC-neg_inf" in r.case_id or "PAR-NOPROC-str_" in r.case_id \
                or "PAR-NA-API-nan" in r.case_id or "PAR-NA-API-pos_inf" in r.case_id \
                or "Infinity" in r.case_id or "exp_overflow" in r.case_id:
            key = "RC-PARAM-NONFINITE"
            title = "参数校验未拒绝 NaN/Infinity/非有限数值"
        elif "DEDUP" in r.case_id or "OVERLAP" in r.case_id or "DUP-ROOTS" in r.case_id:
            key = "RC-CAT-DEDUP"
            title = "类目展开未按 site+node_id 去重"
        elif "COO-" in r.case_id and "COMBINED" in r.case_id or "TD-COLON" in r.case_id or "FULLWIDTH" in r.case_id:
            key = "RC-COO-LABEL"
            title = "产地单格「标签:值」未剥离标签前缀"
        elif r.case_id.startswith("COO-"):
            key = f"RC-COO-{r.case_id}"
            title = f"产地解析: {r.case_id}"
        else:
            key = f"RC-{r.dimension}-{r.layer}"
            title = f"{r.dimension}/{r.layer}: {r.case_id}"
        g = groups.setdefault(key, {"id": key, "title": title, "cases": [], "severity": r.severity or "P2"})
        g["cases"].append(r.case_id)
        if r.severity == "P0":
            g["severity"] = "P0"
        elif r.severity == "P1" and g["severity"] != "P0":
            g["severity"] = "P1"
    return list(groups.values())


def print_report(col: SuiteCollector, started: float, snap_before: dict, snap_after: dict, snap_diff: dict):
    counts = col.counts()
    total = sum(counts.values())
    passed = counts.get("PASS", 0) + counts.get("CONDITIONAL_PASS", 0)
    failed = counts.get("FAIL", 0)
    blocked = counts.get("BLOCKED", 0)
    not_run = counts.get("NOT_RUN", 0)
    elapsed = time.time() - started

    fails = [r for r in col.results if r.status == "FAIL"]
    roots = _root_causes(fails)
    meta = getattr(col, "meta", {})
    pw = meta.get("pairwise", {})
    full = meta.get("full_strict", {})

    polluted = snap_diff.get("hash_changed", False)
    # 若哈希变了但无授权写入正式库 → 污染
    iso_fail = any(r.status == "FAIL" and r.dimension == "测试隔离" for r in col.results)

    pw_ok = pw.get("actual_pairs") == 105 and pw.get("expected_pairs") == 105
    full_ok = (
        full.get("full_dim_count") == 17
        and full.get("fail_rotation_count") == 17
        and full.get("miss_rotation_count") == 17
        and not full.get("uncovered_fail")
        and not full.get("uncovered_miss")
    )
    pg_blocked = any(r.status == "BLOCKED" and r.case_id.startswith("PG-") for r in col.results)
    live_blocked = any(r.status == "BLOCKED" and r.case_id.startswith("LIVE-") for r in col.results)

    if failed == 0 and not polluted and not pg_blocked and not live_blocked and pw_ok and full_ok:
        ship = "可以上线"
    elif failed and not pg_blocked and not live_blocked:
        ship = "暂不可上线（存在失败用例）"
    else:
        ship = "暂不可上线（存在失败和/或外部阻塞未解除）"

    print("=" * 72)
    print("最新到货全维度补全测试报告")
    print(f"运行编号: {col.run_id}")
    print(f"续测自: RUN-20260718-120836")
    print(f"耗时: {elapsed:.1f}s")
    print("=" * 72)
    print()
    print(f"是否可以上线：{ship}")
    print(
        f"总用例数及通过率：共 {total} 项，通过 {passed}，失败 {failed}，"
        f"阻塞 {blocked}，未执行 {not_run}；通过率 {passed/total*100:.1f}%。"
        if total else "无用例"
    )
    print()

    print("失败用例与独立根因：")
    print(f"  失败用例数量: {failed}")
    print(f"  独立缺陷根因数量: {len(roots)}")
    for g in roots:
        print(f"  [{g['severity']}] {g['id']}: {g['title']}")
        print(f"       用例数={len(g['cases'])} 例: {', '.join(g['cases'][:8])}{'...' if len(g['cases'])>8 else ''}")
    if not roots:
        print("  无失败")
    print()

    print("完整 Pairwise 覆盖统计：")
    print(f"  维度数：{pw.get('dimension_count', '—')}")
    print(f"  应覆盖维度对：{pw.get('expected_pairs', '—')}")
    print(f"  实际覆盖维度对：{pw.get('actual_pairs', '—')}")
    print(f"  遗漏：{len(pw.get('missing_pairs', []))}")
    print(f"  覆盖率：{pw.get('coverage_pct', 0):.1f}%")
    if pw.get("missing_pairs"):
        print(f"  缺失清单: {pw['missing_pairs']}")
    else:
        print("  缺失清单: []")
    print(f"  声明成立: Pairwise覆盖率100% = {pw_ok}")
    print()

    print("全维度轮换统计：")
    print(f"  全维度数量：{full.get('full_dim_count', '—')}")
    print(f"  单点失败覆盖数量：{full.get('fail_rotation_count', '—')}")
    print(f"  单点缺失覆盖数量：{full.get('miss_rotation_count', '—')}")
    print(f"  未覆盖失败维度：{full.get('uncovered_fail', '—')}")
    print(f"  未覆盖缺失维度：{full.get('uncovered_miss', '—')}")
    print(f"  声明成立: 严格全维度轮换无遗漏 = {full_ok}")
    print()

    print("SQLite/PG 一致性：")
    sqlite_ok = [r for r in col.results if r.case_id.startswith("DB-SQLITE") or r.case_id.startswith("CQ-") or r.case_id.startswith("SC2-") and r.layer == "抓取/查询一致性"]
    print(f"  SQLite 抓取/查询一致性用例: "
          f"PASS={sum(1 for r in col.results if r.layer=='抓取/查询一致性' and r.status=='PASS')} "
          f"FAIL={sum(1 for r in col.results if r.layer=='抓取/查询一致性' and r.status=='FAIL')}")
    if pg_blocked:
        print("  PostgreSQL: BLOCKED — 未提供 PG_TEST_DSN（独立测试库）。")
        print("  请设置: set PG_TEST_DSN=postgresql://user:pass@host:5432/amz_selection_test")
        print("  禁止使用正式 PG_DSN 冒充测试环境。")
    else:
        pg_pass = sum(1 for r in col.results if r.case_id.startswith("PG-") and r.status == "PASS")
        print(f"  PostgreSQL 真实测试 PASS={pg_pass}")
    print()

    print("真实 Amazon 五场景结果：")
    live = [r for r in col.results if r.case_id.startswith("LIVE-")]
    if all(r.status == "BLOCKED" for r in live):
        print("  全部 BLOCKED。请提供：")
        print("    RUN_LIVE_NA=1")
        print("    LIVE_NA_ROOTS=<最多2个node_id>")
        print("    LIVE_NA_SITE=US")
        print("  套件将使用 data/categories_test_live.db，不写正式库。")
    for r in live:
        print(f"  [{r.status}] {r.case_id}: {str(r.detail)[:240]}")
    print()

    print("数据污染前后快照：")
    print(f"  正式库: {PROD_DB}")
    print(f"  前 sha256: {snap_before.get('sha256')}")
    print(f"  后 sha256: {snap_after.get('sha256')}")
    print(f"  哈希变化: {snap_diff.get('hash_changed')}")
    print(f"  表行数前: {snap_before.get('tables')}")
    print(f"  表行数后: {snap_after.get('tables')}")
    print(f"  表差异: {snap_diff.get('table_diffs')}")
    print(f"  ASIN digest 前/后: {snap_diff.get('asin_digest_before')} / {snap_diff.get('asin_digest_after')}")
    print(f"  站点计数前/后: {snap_diff.get('site_counts_before')} / {snap_diff.get('site_counts_after')}")
    print(f"  污染判定: {'是' if polluted or iso_fail else '否'}")
    print()

    print("未覆盖项 / 阻塞：")
    for r in col.results:
        if r.status in ("BLOCKED", "NOT_RUN"):
            print(f"  [{r.status}] {r.case_id}: {str(r.detail)[:200]}")
    print()

    print("复盘和后续建议：")
    print(f"  本轮补全后总用例 {total}；Pairwise 105 对×4；强相关全字段轮换；严格全维度 17 因子。")
    print("  业务代码未修改（除 config.py 增加 DB_FILE 环境覆盖入口）。失败证据已保留。")
    print("  上线前必须修复根因: RC-PARAM-NONFINITE, RC-CAT-DEDUP；（P2）RC-COO-LABEL")
    print("  解除阻塞: 提供 PG_TEST_DSN；提供 RUN_LIVE_NA+LIVE_NA_ROOTS 后重跑五场景")
    print("=" * 72)

    # 写摘要
    summary = {
        "run_id": col.run_id,
        "ship": ship,
        "counts": counts,
        "pairwise": pw,
        "full_strict": full,
        "root_causes": [
            {"id": g["id"], "title": g["title"], "severity": g["severity"],
             "case_count": len(g["cases"]), "cases": g["cases"]}
            for g in roots
        ],
        "snapshot_diff": {
            "hash_changed": snap_diff.get("hash_changed"),
            "table_diffs": snap_diff.get("table_diffs"),
        },
        "fails": [
            {"case_id": r.case_id, "dimension": r.dimension, "layer": r.layer,
             "expected": repr(r.expected), "actual": repr(r.actual), "detail": r.detail}
            for r in fails
        ],
    }
    path = ROOT / "data" / f"na_test_summary_{col.run_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(f"(摘要已写入 {path})")
    return 0 if failed == 0 and not pg_blocked and not live_blocked else 1


def main():
    run_id = datetime.now(timezone.utc).strftime("RUN-%Y%m%d-%H%M%S")
    col = SuiteCollector(run_id)
    col.meta = {}
    started = time.time()

    snap_before = snapshot_sqlite(PROD_DB)

    _safe(col, "validation", run_validation_tests)
    _safe(col, "parsing", run_parsing_tests)
    _safe(col, "fba", run_fba_tests)
    _safe(col, "single", run_single_dimension_tests)
    _safe(col, "consistency", run_scrape_query_consistency)
    _safe(col, "cli", run_cli_param_tests)
    _safe(col, "pairwise", run_pairwise_full)
    _safe(col, "strong", run_strong_complete)
    _safe(col, "full", run_full_strict)
    _safe(col, "param", run_param_matrix)
    _safe(col, "category", run_category_full)
    _safe(col, "origin", run_origin_html)
    _safe(col, "pg", run_pg_real)
    _safe(col, "live", run_live_smoke)
    _safe(col, "isolation", run_isolation_checks)

    snap_after = snapshot_sqlite(PROD_DB)
    snap_diff = compare_snapshots(snap_before, snap_after)

    # 快照结论写入用例
    col.add(CaseResult(
        "ISO-SNAPSHOT", "测试隔离", "数据库写入",
        "PASS" if not snap_diff.get("hash_changed") else "FAIL",
        "正式库哈希不变", snap_diff,
        detail=f"before={snap_before.get('sha256')} after={snap_after.get('sha256')}",
        severity="P0" if snap_diff.get("hash_changed") else "",
    ))

    code = print_report(col, started, snap_before, snap_after, snap_diff)
    sys.exit(code)


if __name__ == "__main__":
    main()
