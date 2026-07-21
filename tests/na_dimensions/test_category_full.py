"""类目范围完整矩阵（SQLite）；PG 需 PG_TEST_DSN。"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tests.na_dimensions.helpers import CaseResult, SuiteCollector, temp_sqlite_db
import fetch_new_arrivals as na


def _init_tree(db: str):
    conn = sqlite3.connect(db)
    conn.execute(
        """CREATE TABLE categories (
            node_id TEXT, name TEXT, depth INTEGER,
            parent_node_id TEXT, site TEXT, na_valid INTEGER DEFAULT 1,
            PRIMARY KEY (node_id, site)
        )"""
    )
    rows = [
        ("ROOT", "Root", 0, None, "US"),
        ("L2A", "L2 Alpha", 2, "ROOT", "US"),
        ("L3A1", "L3 Alpha-1", 3, "L2A", "US"),
        ("L3A2", "L3 Alpha-2", 3, "L2A", "US"),
        ("L4A11", "L4 Alpha-1-1", 4, "L3A1", "US"),
        ("L4A12", "L4 Alpha-1-2", 4, "L3A1", "US"),
        ("L2B", "L2 Beta", 2, "ROOT", "US"),
        ("L3B1", "L3 Beta-1", 3, "L2B", "US"),
        ("L5DEEP", "L5 Deep", 5, "L4A11", "US"),
        # 跨站同名
        ("L2A", "DE L2", 2, None, "DE"),
        ("L3A1", "DE L3", 3, "L2A", "DE"),
        # 异常 parent（指向不存在）
        ("ORPHAN", "Orphan", 3, "NO_SUCH_PARENT", "US"),
    ]
    conn.executemany(
        "INSERT INTO categories(node_id,name,depth,parent_node_id,site) VALUES(?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()


def _ids(nodes):
    return [n["node_id"] for n in nodes]


def _uniq(nodes):
    return {(n.get("site", "US"), n["node_id"]) for n in nodes}


def run_category_full(col: SuiteCollector):
    with temp_sqlite_db() as db:
        _init_tree(db)
        old_b, old_f = na.DB_BACKEND, na.DB_FILE
        try:
            na.DB_BACKEND = "sqlite"
            na.DB_FILE = db

            def load(roots, desc=True, site="US"):
                return na._load_nodes(site, root_ids=roots, include_descendants=desc)

            # 仅所选
            n = load(["L2A"], desc=False)
            col.add(CaseResult("CAT2-SEL-L2", "类目范围", "类目展开",
                               "PASS" if _ids(n) == ["L2A"] else "FAIL", ["L2A"], _ids(n), severity="P1"))

            n = load(["L2A", "L2B"], desc=False)
            col.add(CaseResult("CAT2-SEL-MULTI", "类目范围", "类目展开",
                               "PASS" if set(_ids(n)) == {"L2A", "L2B"} else "FAIL",
                               {"L2A", "L2B"}, set(_ids(n)), severity="P1"))

            n = load(["L2A", "L4A11"], desc=False)
            col.add(CaseResult("CAT2-SEL-PARENT-CHILD", "类目范围", "类目展开",
                               "PASS" if set(_ids(n)) == {"L2A", "L4A11"} else "FAIL",
                               {"L2A", "L4A11"}, set(_ids(n))))

            n = load(["L2A", "L2A"], desc=False)
            # 重复 root：集合应唯一；列表可能重复（记证据）
            col.add(CaseResult(
                "CAT2-SEL-DUP-ROOT", "类目范围", "类目展开",
                "PASS" if set(_ids(n)) == {"L2A"} else "FAIL",
                {"L2A"}, {"list": _ids(n), "set": set(_ids(n))},
                detail=f"重复数量={len(_ids(n))-len(set(_ids(n)))}",
            ))

            # 跨站：US 展开不得含 DE
            n = load(["L2A"], desc=True, site="US")
            cross = [x for x in n if x.get("site") == "DE"]
            # _load_nodes 返回无 site 字段，用 node 集合判断不应多出 DE-only 节点
            # DE 的 L3A1 在 US 树也存在，关键是不混入 DE 独有结构；用数量与期望集合
            expect = {"L2A", "L3A1", "L3A2", "L4A11", "L4A12", "L5DEEP"}
            col.add(CaseResult("CAT2-DESC-L2", "类目范围", "类目展开",
                               "PASS" if set(_ids(n)) == expect else "FAIL",
                               expect, set(_ids(n)), severity="P1"))

            # 多无交集父类目
            n = load(["L2A", "L2B"], desc=True)
            expect2 = expect | {"L2B", "L3B1"}
            col.add(CaseResult("CAT2-DESC-MULTI", "类目范围", "类目展开",
                               "PASS" if set(_ids(n)) == expect2 else "FAIL",
                               expect2, set(_ids(n))))

            # 父子包含关系展开 — 去重（预期可能 FAIL）
            n = load(["L2A", "L4A11"], desc=True)
            ids = _ids(n)
            dup = len(ids) - len(set(ids))
            col.add(CaseResult(
                "CAT2-DEDUP-PARENT-CHILD", "类目范围", "类目展开",
                "PASS" if dup == 0 and set(ids) == expect else "FAIL",
                {"dup": 0, "set": expect},
                {"list": ids, "set": set(ids), "dup": dup},
                detail="父子同选必须按 site+node_id 去重；当前预期可能 FAIL",
                severity="P1",
            ))

            # 部分重叠：L3A1 与 L2A
            n = load(["L2A", "L3A1"], desc=True)
            ids = _ids(n)
            dup = len(ids) - len(set(ids))
            col.add(CaseResult(
                "CAT2-OVERLAP", "类目范围", "类目展开",
                "PASS" if dup == 0 else "FAIL",
                0, dup, detail=f"list={ids}", severity="P1",
            ))

            n = load(["L2A", "L2A", "L4A11"], desc=True)
            ids = _ids(n)
            col.add(CaseResult(
                "CAT2-DUP-ROOTS-DESC", "类目范围", "类目展开",
                "PASS" if len(ids) == len(set(ids)) else "FAIL",
                "无重复", {"list": ids, "dup": len(ids) - len(set(ids))},
                severity="P1",
            ))

            # 空 roots → 全部 NEW（na_valid=1）类目
            n = na._load_nodes("US", root_ids=None, include_descendants=True)
            col.add(CaseResult(
                "CAT2-EMPTY-ROOTS", "类目范围", "类目展开",
                "PASS" if len(n) >= 7 else "FAIL", ">=7 NEW", len(n),
            ))

            # 不存在 root
            n = load(["NOEXIST"], desc=True)
            col.add(CaseResult(
                "CAT2-MISSING-ROOT", "类目范围", "类目展开",
                "PASS" if n == [] else "FAIL", [], _ids(n),
            ))

            # 跨站同名：查 DE 不应得到 US 下级 L4
            n = load(["L2A"], desc=True, site="DE")
            col.add(CaseResult(
                "CAT2-CROSS-SITE", "类目范围", "类目展开",
                "PASS" if set(_ids(n)) == {"L2A", "L3A1"} and "L4A11" not in _ids(n) else "FAIL",
                {"L2A", "L3A1"}, set(_ids(n)), severity="P1",
            ))

            # 深层递归
            n = load(["L2A"], desc=True)
            col.add(CaseResult(
                "CAT2-DEEP", "类目范围", "类目展开",
                "PASS" if "L5DEEP" in _ids(n) else "FAIL",
                "含 L5DEEP", _ids(n),
            ))

            # 异常 parent：ORPHAN 不应因递归爆炸；作为 root 可选中
            n = load(["ORPHAN"], desc=True)
            col.add(CaseResult(
                "CAT2-ORPHAN", "类目范围", "类目展开",
                "PASS" if _ids(n) == ["ORPHAN"] else "FAIL",
                ["ORPHAN"], _ids(n),
                detail="异常 parent 防护：不崩溃且仅自身",
            ))

        finally:
            na.DB_BACKEND, na.DB_FILE = old_b, old_f

    if not os.getenv("PG_TEST_DSN", "").strip():
        col.add(CaseResult(
            "CAT2-PG-ALL", "类目范围", "类目展开", "BLOCKED",
            detail="未提供 PG_TEST_DSN，无法对 PostgreSQL 执行类目递归/去重并与 SQLite 对比节点集合",
        ))
    else:
        col.add(CaseResult(
            "CAT2-PG-ALL", "类目范围", "类目展开", "NOT_RUN",
            detail="交由 test_pg_real 执行",
        ))
