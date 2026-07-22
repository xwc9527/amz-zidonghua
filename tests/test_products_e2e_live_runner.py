"""products_e2e_live runner 自身的安全与判定契约。"""
from __future__ import annotations

import importlib.util
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import types
import unittest
from pathlib import Path


RUNNER_PATH = Path(__file__).with_name("products_e2e_live.py")
SPEC = importlib.util.spec_from_file_location("products_e2e_live_runner", RUNNER_PATH)
runner = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


def args(**overrides):
    values = {
        "site": "US",
        "charts": "all",
        "max_pages": 1,
        "details_per_list": 3,
        "candidate_limit": 10,
        "min_proxy_nodes": 8,
        "min_unique_ips": 2,
    }
    values.update(overrides)
    return types.SimpleNamespace(**values)


def verified_evidence(ip: str, *, node_key: str = "nk1", port: int = 18001) -> dict:
    return {
        "node_key": node_key,
        "proxy_name": f"node-{node_key}",
        "port": port,
        "proxy": f"http://127.0.0.1:{port}",
        "verified_exit_ip": ip,
        "probe_ok": True,
        "probe_at": "2026-07-22T00:00:00Z",
        "probe_source": "proxy_health.fetch_exit_ip",
        "amazon_via_proxy": True,
    }


def ledger_row(ip: str, **extra) -> dict:
    row = {
        "phase": "LIST",
        "url": "https://www.amazon.com/x",
        "proxy_evidence": [verified_evidence(ip)],
        "verified_exit_ips": [ip],
        "exit_ips": [ip],
        "ok": True,
        "status_code": 200,
        "response_bytes": 10,
        "captcha": False,
    }
    row.update(extra)
    return row


class TestRunnerContracts(unittest.TestCase):
    def test_only_us_and_pages_one_or_two(self):
        self.assertEqual(runner.validate_args(args(max_pages=1)), list(runner.ALL_CHARTS))
        self.assertEqual(runner.validate_args(args(max_pages=2)), list(runner.ALL_CHARTS))
        for value in (0, 3, 99, -1):
            with self.subTest(value=value), self.assertRaises(runner.SafetyError):
                runner.validate_args(args(max_pages=value))
        with self.assertRaises(runner.SafetyError):
            runner.validate_args(args(site="UK"))

    def test_min_proxy_thresholds_cannot_go_below_floor(self):
        with self.assertRaises(runner.SafetyError):
            runner.validate_args(args(min_proxy_nodes=7))
        with self.assertRaises(runner.SafetyError):
            runner.validate_args(args(min_unique_ips=1))
        with self.assertRaises(runner.SafetyError):
            runner.validate_args(args(min_proxy_nodes=0, min_unique_ips=0))
        # 允许更高门槛
        self.assertEqual(
            runner.validate_args(args(min_proxy_nodes=9, min_unique_ips=3)),
            list(runner.ALL_CHARTS),
        )
        self.assertEqual(runner.MIN_PROXY_NODES_FLOOR, 8)
        self.assertEqual(runner.MIN_UNIQUE_IPS_FLOOR, 2)

    def test_missing_one_chart_cannot_pass(self):
        charts = list(runner.ALL_CHARTS)
        results = {chart: {"status": "PASS"} for chart in charts[:-1]}
        report = {
            "requested_charts": charts,
            "chart_results": results,
            "page_test": {"status": "PASS"},
            "scope_test": {"status": "PASS"},
            "failures": [],
            "blocked": [],
            "formal_storage_unchanged": True,
        }
        self.assertEqual(runner.determine_final_status(report), "NOT_EXECUTED")

    def test_pg_info_does_not_block_sqlite_pass(self):
        charts = list(runner.ALL_CHARTS)
        report = {
            "requested_charts": charts,
            "chart_results": {chart: {"status": "PASS"} for chart in charts},
            "page_test": {"status": "PASS"},
            "scope_test": {"status": "PASS"},
            "failures": [],
            "blocked": [],
            "formal_storage_unchanged": True,
            "pg": {
                "status": "BLOCKED_NOT_IMPLEMENTED",
                "blocks_sqlite_pass": False,
            },
        }
        self.assertEqual(runner.determine_final_status(report), "PASS")
        self.assertEqual(runner.exit_code_for({
            **report, "final_status": "PASS",
        }), 0)

    def test_blocked_and_not_executed_never_return_zero(self):
        for status in ("BLOCKED", "NOT_EXECUTED"):
            report = {"final_status": status, "formal_storage_unchanged": True}
            self.assertNotEqual(runner.exit_code_for(report), 0)

    def test_tool_error_has_dedicated_exit_code(self):
        report = {
            "final_status": "FAIL",
            "formal_storage_unchanged": True,
            "tool_error": True,
        }
        self.assertEqual(runner.exit_code_for(report), runner.EXIT_TOOL_ERROR)

    def test_process_status_must_be_done(self):
        self.assertEqual(
            runner.process_status_failures("done", ""),
            [],
        )
        self.assertIn(
            "PROCESS_STATUS_NOT_DONE:error",
            runner.process_status_failures("error", "DETAIL_FETCH_FAILED"),
        )
        self.assertIn(
            "PROCESS_ERROR:boom",
            runner.process_status_failures("done", "boom"),
        )

    def test_request_policy_requires_proxy_and_page_contract(self):
        page1_bad = [ledger_row(
            "1.1.1.1",
            url="https://www.amazon.com/x?pg=2",
        )]
        self.assertIn(
            "PAGE2_REQUESTED_IN_ONE_PAGE_MODE",
            runner.request_policy_failures(page1_bad, max_pages=1),
        )
        page2_missing = [ledger_row("1.1.1.1")]
        self.assertIn(
            "PAGE2_NOT_REQUESTED",
            runner.request_policy_failures(
                page2_missing, max_pages=2, require_page2=True,
            ),
        )
        page2_failed = [ledger_row(
            "1.1.1.1",
            url="https://www.amazon.com/x?pg=2",
            ok=False,
            status_code=503,
            response_bytes=0,
        )]
        self.assertIn(
            "PAGE2_REQUEST_FAILED",
            runner.request_policy_failures(
                page2_failed,
                max_pages=2,
                require_page2=True,
                require_page2_success=True,
            ),
        )
        no_proxy = [{
            "phase": "DETAIL",
            "url": "https://www.amazon.com/dp/B0TEST0001",
            "ok": True,
            "exit_ips": [],
            "proxy_evidence": [],
        }]
        self.assertIn(
            "PROXY_EVIDENCE_MISSING",
            runner.request_policy_failures(no_proxy, max_pages=1),
        )
        # 仅有池声明 exit_ips、无 verified evidence → 仍缺失
        pool_meta_only = [{
            "phase": "DETAIL",
            "url": "https://www.amazon.com/dp/B0TEST0001",
            "ok": True,
            "exit_ips": ["1.1.1.1"],
            "proxy_evidence": [],
        }]
        self.assertIn(
            "PROXY_EVIDENCE_MISSING",
            runner.request_policy_failures(pool_meta_only, max_pages=1),
        )
        page3 = [ledger_row("1.1.1.1", url="https://www.amazon.com/x?pg=3")]
        self.assertIn(
            "PAGE3_REQUESTED",
            runner.request_policy_failures(page3, max_pages=2),
        )
        # 失败尝试（出口探测本身失败）天然不可能携带证据，不应被判缺证据——
        # 否则有界重试后恢复成功的榜单会被这条无关的失败尝试拖累。
        exhausted_failed_attempt = [{
            "phase": "LIST",
            "url": "https://www.amazon.com/x",
            "ok": False,
            "exit_ips": [],
            "proxy_evidence": [],
        }]
        self.assertNotIn(
            "PROXY_EVIDENCE_MISSING",
            runner.request_policy_failures(exhausted_failed_attempt, max_pages=1),
        )

    def test_ledger_requires_verified_exit_not_pool_meta(self):
        self.assertIn(
            "PROXY_LEDGER_EMPTY",
            runner.ledger_proxy_failures([], min_unique_ips=2),
        )
        # 池元数据伪装的 exit_ips 不能充当实测证据
        pool_meta = [
            {"ok": True, "exit_ips": ["1.1.1.1"], "proxy_evidence": []},
            {"ok": True, "exit_ips": ["2.2.2.2"], "proxy_evidence": []},
        ]
        self.assertIn(
            "PROXY_EVIDENCE_MISSING",
            runner.ledger_proxy_failures(pool_meta, min_unique_ips=2),
        )
        one_ip = [
            ledger_row("1.1.1.1", node_key="a"),
            ledger_row("1.1.1.1", node_key="b"),
        ]
        # 修补 node_key 差异
        one_ip[0]["proxy_evidence"] = [verified_evidence("1.1.1.1", node_key="a")]
        one_ip[1]["proxy_evidence"] = [verified_evidence("1.1.1.1", node_key="b")]
        self.assertIn(
            "LEDGER_UNIQUE_IPS_LOW:1<2",
            runner.ledger_proxy_failures(one_ip, min_unique_ips=2),
        )
        two_ips = [
            ledger_row("1.1.1.1"),
            ledger_row("2.2.2.2", url="https://www.amazon.com/y"),
        ]
        two_ips[1]["proxy_evidence"] = [
            verified_evidence("2.2.2.2", node_key="nk2", port=18002)
        ]
        two_ips[1]["verified_exit_ips"] = ["2.2.2.2"]
        two_ips[1]["exit_ips"] = ["2.2.2.2"]
        self.assertEqual(
            runner.ledger_proxy_failures(two_ips, min_unique_ips=2),
            [],
        )

    def test_iproyal_provider_field_cannot_bypass(self):
        from proxy_node_source import is_iproyal_node

        bypass = {
            "name": "US-Normal-01",
            "server": "ok.example.com",
            "provider": "IPRoyal",
        }
        self.assertTrue(is_iproyal_node(bypass))
        hits = runner.find_iproyal_nodes([bypass])
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0]["provider"], "IPRoyal")
        clean = {
            "name": "新加坡1",
            "server": "sg.example.com",
            "type": "vless",
        }
        self.assertEqual(runner.find_iproyal_nodes([clean]), [])

    def test_stop_pool_requires_thread_exit_evidence(self):
        class AliveThread:
            def __init__(self, alive=True):
                self._alive = alive
                self.name = "proxy-pool-live-reload"

            def is_alive(self):
                return self._alive

        class FakePool:
            def __init__(self, alive_after=False):
                self._live_reload_thread = AliveThread(True)
                self._alive_after = alive_after

            def stop_live_reload(self):
                self._live_reload_thread = AliveThread(self._alive_after)

        ok = runner.stop_pool(FakePool(alive_after=False))
        self.assertTrue(ok["ok"])
        self.assertTrue(ok["stop_called"])
        self.assertFalse(ok["thread_alive_after"])

        bad = runner.stop_pool(FakePool(alive_after=True))
        self.assertFalse(bad["ok"])
        self.assertTrue(bad["thread_alive_after"])
        self.assertIn("still_alive", bad["error"])

    def test_formal_test_tables_must_stay_empty(self):
        self.assertEqual(
            runner.formal_table_failures({
                "product_sightings": 0,
                "new_arrivals": 0,
                "favorite_products": 0,
            }),
            [],
        )
        failures = runner.formal_table_failures({
            "product_sightings": 1,
            "new_arrivals": 0,
            "favorite_products": 0,
        })
        self.assertEqual(
            failures, ["FORMAL_TEST_TABLE_NOT_EMPTY:product_sightings:1"]
        )

    def test_query_scalar_fail_closed_on_missing_table(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "empty.db"
            sqlite3.connect(db).close()
            with self.assertRaises(sqlite3.OperationalError):
                runner.query_scalar(db, "SELECT COUNT(*) FROM product_sightings")
            with self.assertRaises(RuntimeError):
                runner.assert_formal_tables_empty(db)

    def test_wal_or_shm_change_is_detected(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "categories.db"
            db.write_bytes(b"db")
            wal = Path(str(db) + "-wal")
            wal.write_bytes(b"before")
            before = runner.formal_storage_snapshot(db)
            wal.write_bytes(b"after")
            after = runner.formal_storage_snapshot(db)
            self.assertTrue(runner.formal_storage_changed(before, after))

    def test_dynamic_candidates_use_validity_column(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "categories.db"
            con = sqlite3.connect(db)
            con.execute(
                """CREATE TABLE categories (
                    node_id TEXT, name TEXT, depth INTEGER, url TEXT, slug TEXT,
                    child_count INTEGER, site TEXT,
                    nr_valid INTEGER, bs_valid INTEGER, ms_valid INTEGER, mw_valid INTEGER
                )"""
            )
            con.executemany(
                "INSERT INTO categories VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                [
                    ("NR", "NR", 3, "/gp/new-releases/nr/NR", "nr", 1, "US", 1, 0, 0, 0),
                    ("BS", "BS", 3, "/gp/bestsellers/bs/BS", "bs", 1, "US", 0, 1, 0, 0),
                    ("UK", "UK", 3, "/gp/new-releases/uk/UK", "uk", 1, "UK", 1, 0, 0, 0),
                ],
            )
            con.commit()
            con.close()
            # limit 恰好等于本榜有效候选数：不应混入其它榜单校验过的节点。
            nr = runner.candidate_rows(db, "new-releases", 1)
            bs = runner.candidate_rows(db, "bestsellers", 1)
            self.assertEqual([row["node_id"] for row in nr], ["NR"])
            self.assertEqual([row["node_id"] for row in bs], ["BS"])

    def test_candidate_rows_pads_with_fallback_when_valid_rows_insufficient(self):
        """有效候选只有 1 个但 limit>1 时，应补足通用深层节点作为发现阶段的
        退路——避免单一候选撞上代理抖动就让整榜直接 BLOCKED_NO_LIVE_NODE。
        校验过的节点必须排在补足节点之前，优先被尝试。"""
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "categories.db"
            con = sqlite3.connect(db)
            con.execute(
                """CREATE TABLE categories (
                    node_id TEXT, name TEXT, depth INTEGER, url TEXT, slug TEXT,
                    child_count INTEGER, site TEXT,
                    nr_valid INTEGER, bs_valid INTEGER, mw_valid INTEGER
                )"""
            )
            con.executemany(
                "INSERT INTO categories VALUES (?,?,?,?,?,?,?,?,?,?)",
                [
                    ("NR", "NR", 3, "/gp/new-releases/nr/NR", "nr", 1, "US", 1, 0, 0),
                    ("G1", "G1", 3, "/gp/new-releases/g1/G1", "g1", 1, "US", 0, 0, 0),
                    ("G2", "G2", 3, "/gp/new-releases/g2/G2", "g2", 1, "US", 0, 0, 0),
                ],
            )
            con.commit()
            con.close()
            rows = runner.candidate_rows(db, "new-releases", 3)
            self.assertEqual(len(rows), 3)
            self.assertEqual(rows[0]["node_id"], "NR")
            self.assertEqual({row["node_id"] for row in rows}, {"NR", "G1", "G2"})

    def test_footer_dp_links_do_not_trigger_parse_miss(self):
        outcome = types.SimpleNamespace(ok=True, status_code=200)
        body = """
        <html><body><main><h1>Other page</h1></main>
        <footer>
          <a href="/dp/B0000000A1">Amazon Business Card</a>
          <a href="/dp/B0000000B2">Reload Balance</a>
        </footer></body></html>
        """
        self.assertEqual(runner._html_product_asins(body), [])
        self.assertEqual(
            runner.classify_discovery_response(
                outcome,
                "https://www.amazon.com/gp/bestsellers/books/",
                body,
                0,
            ),
            "NO_PRODUCTS_UNKNOWN",
        )

    def test_main_chart_container_asins_trigger_parse_miss(self):
        outcome = types.SimpleNamespace(ok=True, status_code=200)
        body = """
        <div id="zg-center-div">
          <div id="gridItemRoot-1"><a href="/dp/B0000000A1">A</a></div>
          <div class="p13n-sc-uncoverable-faceout" data-asin="B0000000B2"></div>
        </div>
        """
        self.assertEqual(
            runner._html_product_asins(body),
            ["B0000000A1", "B0000000B2"],
        )
        self.assertEqual(
            runner.classify_discovery_response(
                outcome,
                "https://www.amazon.com/gp/bestsellers/books/",
                body,
                0,
            ),
            "PARSE_MISS",
        )

    def test_discovery_response_failure_classes(self):
        ok = types.SimpleNamespace(ok=True, status_code=200)
        http_bad = types.SimpleNamespace(ok=False, status_code=503)
        cases = [
            (
                "HTTP_BLOCKED",
                http_bad,
                "https://www.amazon.com/gp/bestsellers/electronics/",
                "",
                0,
                False,
            ),
            (
                "CAPTCHA_BLOCKED",
                ok,
                "https://www.amazon.com/gp/bestsellers/electronics/",
                "<html>captcha</html>",
                0,
                True,
            ),
            (
                "PARSE_MISS",
                ok,
                "https://www.amazon.com/gp/bestsellers/electronics/",
                (
                    '<div id="zg-center-div">'
                    '<a href="/dp/B0000000A1">A</a>'
                    '<div data-asin="B0000000B2"></div></div>'
                ),
                0,
                False,
            ),
            (
                "NO_PRODUCTS_UNKNOWN",
                ok,
                "https://www.amazon.com/gp/bestsellers/electronics/",
                "<html>temporarily sparse</html>",
                0,
                False,
            ),
            (
                "",
                ok,
                "https://www.amazon.com/gp/bestsellers/electronics/",
                "<html>products</html>",
                2,
                False,
            ),
        ]
        for expected, outcome, url, body, count, captcha in cases:
            with self.subTest(expected=expected, body=body):
                self.assertEqual(
                    runner.classify_discovery_response(
                        outcome,
                        url,
                        body,
                        count,
                        captcha=captcha,
                    ),
                    expected,
                )

    def test_runtime_path_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            runtime = Path(tmp)
            paths = {"runtime": runtime}
            fp = types.SimpleNamespace(
                LOG_PATH=str(runtime / "fetch.log"),
                _AUDIT_PATH=str(runtime / "audit.jsonl"),
            )
            config = types.SimpleNamespace(
                DB_FILE=str(runtime / "db.sqlite"),
                DATA_DIR=str(runtime / "data"),
            )
            cache = types.SimpleNamespace(cache_path=lambda: str(runtime / "cache.db"))
            runner.assert_module_paths(paths, fp, config, cache)
            config.DB_FILE = str(Path(tmp).parent / "formal.db")
            with self.assertRaises(runner.SafetyError):
                runner.assert_module_paths(paths, fp, config, cache)

    def test_report_survives_runtime_deletion(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            runtime = base / "runtime"
            report_dir = base / "stable" / "run"
            runtime.mkdir()
            report_dir.mkdir(parents=True)
            ledger = runtime / "request_ledger.jsonl"
            ledger.write_text(json.dumps({"phase": "LIST"}) + "\n", encoding="utf-8")
            paths = {
                "runtime": runtime,
                "report_dir": report_dir,
                "ledger": ledger,
            }
            report = {
                "run_id": "RUN",
                "final_status": "PASS",
                "started_at": "a",
                "finished_at": "b",
                "runtime_root": str(runtime),
                "formal_storage_unchanged": True,
                "requested_charts": [],
                "chart_results": {},
                "failures": [],
                "blocked": [],
            }
            json_path, md_path, ledger_path = runner.write_report(report, paths)
            shutil.rmtree(runtime)
            self.assertTrue(json_path.is_file())
            self.assertTrue(md_path.is_file())
            self.assertTrue(ledger_path.is_file())

    def test_runtime_diagnostics_are_persisted_to_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            runtime = root / "runtime"
            report_dir = root / "report"
            source = runtime / "diagnostics" / "discovery" / "parse.html"
            source.parent.mkdir(parents=True)
            report_dir.mkdir()
            source.write_text("<html>evidence</html>", encoding="utf-8")
            report = {
                "discovery": {
                    "bestsellers": {
                        "evidence_dir": str(source.parent),
                        "attempts": [{"evidence_path": str(source)}],
                    }
                }
            }
            runner.persist_runtime_diagnostics(
                report,
                {"runtime": runtime, "report_dir": report_dir},
            )
            stable = (
                report_dir / "diagnostics" / "discovery" / "parse.html"
            )
            self.assertTrue(stable.is_file())
            discovery = report["discovery"]["bestsellers"]
            self.assertEqual(Path(discovery["evidence_dir"]), stable.parent)
            self.assertEqual(Path(discovery["attempts"][0]["evidence_path"]), stable)


if __name__ == "__main__":
    unittest.main()
