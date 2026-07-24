"""多机场订阅自动汇总 + 新增订阅自动嗅探——单测（不依赖真实 Clash 目录）。

覆盖：
- 单订阅时行为与旧实现完全一致（不破坏既有语义）。
- 两个订阅自动合并为一个候选池，指纹随任一订阅变化而变化。
- 新增第三个订阅（模拟"用户新增机场"）后，指纹变化，节点数增加——
  这就是"嗅探"在数据层面的体现：调用方无需切换 current，无需感知具体订阅数。
- merge/script/rules/proxies/groups 等派生文件不会被误当成订阅解析。
- 单个订阅解析失败（文件损坏/缺 proxies）不会拖垮其它订阅。
"""
from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from proxy_node_source import load_candidate_nodes, resolve_remote_profiles


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


AIRPORT_A = """
proxies:
  - name: "剩余流量：100GB"
    type: ss
    server: meta.a.com
    port: 1
  - name: "A-Node-1"
    type: vless
    server: a1.example
    port: 443
    uuid: uuid-a1
  - name: "A-Node-2"
    type: vless
    server: a2.example
    port: 443
    uuid: uuid-a2
"""

AIRPORT_B = """
proxies:
  - name: "IPRoyal-Test"
    type: http
    server: iproyal.example
    port: 2
  - name: "B-Node-1"
    type: hysteria2
    server: b1.example
    port: 443
"""

AIRPORT_C = """
proxies:
  - name: "C-Node-1"
    type: trojan
    server: c1.example
    port: 443
"""


class TestMultiSubscriptionAggregation(unittest.TestCase):
    def _profiles_meta(self, items_yaml: str) -> str:
        return f"current: UID_A\nitems:\n{items_yaml}"

    def test_single_subscription_matches_legacy_behavior(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            profiles = td / "profiles"
            _write(profiles / "UID_A.yaml", AIRPORT_A)
            meta = td / "profiles.yaml"
            _write(meta, self._profiles_meta(
                "  - uid: UID_A\n    name: airport-a\n    file: UID_A.yaml\n    updated: 111\n"
            ))
            res = load_candidate_nodes(profiles_meta=str(meta), profiles_dir=str(profiles))
            self.assertTrue(res.ok)
            self.assertEqual({n["name"] for n in res.nodes}, {"A-Node-1", "A-Node-2"})
            self.assertEqual(res.fingerprint.uid, "UID_A")
            self.assertEqual(res.fingerprint.updated_at, "111")
            self.assertEqual(res.stats.subscription_entries, 3)
            self.assertEqual(res.stats.candidates, 2)

    def test_two_subscriptions_merge_into_one_pool(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            profiles = td / "profiles"
            _write(profiles / "UID_A.yaml", AIRPORT_A)
            _write(profiles / "UID_B.yaml", AIRPORT_B)
            meta = td / "profiles.yaml"
            _write(meta, self._profiles_meta(
                "  - uid: UID_A\n    type: remote\n    name: airport-a\n    file: UID_A.yaml\n    updated: 111\n"
                "  - uid: UID_B\n    type: remote\n    name: airport-b\n    file: UID_B.yaml\n    updated: 222\n"
            ))
            res = load_candidate_nodes(profiles_meta=str(meta), profiles_dir=str(profiles))
            self.assertTrue(res.ok)
            names = {n["name"] for n in res.nodes}
            self.assertEqual(names, {"A-Node-1", "A-Node-2", "B-Node-1"})
            # 合成指纹：多订阅时 uid 是组合值，不再等于任一单订阅 uid
            self.assertIn("UID_A", res.fingerprint.uid)
            self.assertIn("UID_B", res.fingerprint.uid)
            self.assertEqual(res.fingerprint.entry_count, 5)  # 3(A) + 2(B)
            # 每个节点应打上来源订阅标记，便于排障溯源
            b_node = next(n for n in res.nodes if n["name"] == "B-Node-1")
            self.assertEqual(b_node["_profile_uid"], "UID_B")

    def test_new_subscription_detected_without_switching_current(self):
        """模拟"用户在 Clash 里新增一个机场订阅，但没有切换当前选中"：
        candidate_nodes() 下一次调用应自动发现并汇入，且指纹发生变化。"""
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            profiles = td / "profiles"
            _write(profiles / "UID_A.yaml", AIRPORT_A)
            meta = td / "profiles.yaml"
            _write(meta, self._profiles_meta(
                "  - uid: UID_A\n    name: airport-a\n    file: UID_A.yaml\n    updated: 111\n"
            ))
            before = load_candidate_nodes(profiles_meta=str(meta), profiles_dir=str(profiles))
            self.assertTrue(before.ok)
            self.assertEqual(len(before.nodes), 2)
            before_sha = before.fingerprint.sha256

            # 用户新增了一个订阅（Clash Verge 添加订阅链接后会自动写入 profiles.yaml，
            # current 字段不变；不需要用户手动切换）
            _write(profiles / "UID_C.yaml", AIRPORT_C)
            _write(meta, self._profiles_meta(
                "  - uid: UID_A\n    name: airport-a\n    file: UID_A.yaml\n    updated: 111\n"
                "  - uid: UID_C\n    type: remote\n    name: airport-c\n    file: UID_C.yaml\n    updated: 333\n"
            ))
            after = load_candidate_nodes(profiles_meta=str(meta), profiles_dir=str(profiles))
            self.assertTrue(after.ok)
            self.assertEqual({n["name"] for n in after.nodes}, {"A-Node-1", "A-Node-2", "C-Node-1"})
            self.assertNotEqual(after.fingerprint.sha256, before_sha)

    def test_derived_profile_types_are_not_treated_as_subscriptions(self):
        """merge/script/rules/proxies/groups 等 Clash Verge 拆分导出的派生文件
        不应被当成订阅去解析（它们的 yaml 结构与订阅完全不同）。"""
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            profiles = td / "profiles"
            _write(profiles / "UID_A.yaml", AIRPORT_A)
            _write(profiles / "Merge.yaml", "rules: []\n")
            _write(profiles / "Script.js", "// not yaml at all {{{\n")
            meta = td / "profiles.yaml"
            _write(meta, self._profiles_meta(
                "  - uid: Merge\n    type: merge\n    file: Merge.yaml\n    updated: 1\n"
                "  - uid: Script\n    type: script\n    file: Script.js\n    updated: 1\n"
                "  - uid: UID_A\n    name: airport-a\n    file: UID_A.yaml\n    updated: 111\n"
            ))
            fps = resolve_remote_profiles(profiles_meta=str(meta), profiles_dir=str(profiles))
            self.assertEqual([f.uid for f in fps], ["UID_A"])
            res = load_candidate_nodes(profiles_meta=str(meta), profiles_dir=str(profiles))
            self.assertTrue(res.ok)
            self.assertEqual({n["name"] for n in res.nodes}, {"A-Node-1", "A-Node-2"})

    def test_one_broken_subscription_does_not_break_others(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            profiles = td / "profiles"
            _write(profiles / "UID_A.yaml", AIRPORT_A)
            _write(profiles / "UID_BAD.yaml", "not_proxies_key: []\n")
            meta = td / "profiles.yaml"
            _write(meta, self._profiles_meta(
                "  - uid: UID_A\n    name: airport-a\n    file: UID_A.yaml\n    updated: 111\n"
                "  - uid: UID_BAD\n    type: remote\n    name: broken\n    file: UID_BAD.yaml\n    updated: 999\n"
            ))
            res = load_candidate_nodes(profiles_meta=str(meta), profiles_dir=str(profiles))
            self.assertTrue(res.ok)
            self.assertEqual({n["name"] for n in res.nodes}, {"A-Node-1", "A-Node-2"})
            broken_entry = next(p for p in res.stats.per_profile if p["uid"] == "UID_BAD")
            self.assertFalse(broken_entry["ok"])

    def test_all_profiles_broken_reports_error(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            profiles = td / "profiles"
            _write(profiles / "UID_BAD.yaml", "not_proxies_key: []\n")
            meta = td / "profiles.yaml"
            _write(meta, self._profiles_meta(
                "  - uid: UID_BAD\n    type: remote\n    file: UID_BAD.yaml\n    updated: 1\n"
            ))
            res = load_candidate_nodes(profiles_meta=str(meta), profiles_dir=str(profiles))
            self.assertFalse(res.ok)
            self.assertEqual(res.error_code, "ALL_PROFILES_FAILED")

    def test_no_subscription_profiles_reports_error(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            profiles = td / "profiles"
            profiles.mkdir()
            meta = td / "profiles.yaml"
            _write(meta, self._profiles_meta(
                "  - uid: Merge\n    type: merge\n    file: Merge.yaml\n    updated: 1\n"
            ))
            res = load_candidate_nodes(profiles_meta=str(meta), profiles_dir=str(profiles))
            self.assertFalse(res.ok)
            self.assertEqual(res.error_code, "NO_PROFILES")

    def test_round_robin_cap_does_not_starve_smaller_subscription(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            profiles = td / "profiles"
            big = "proxies:\n" + "".join(
                f"  - name: Big-{i}\n    type: vless\n    server: big{i}.example\n    port: 443\n"
                for i in range(10)
            )
            _write(profiles / "UID_BIG.yaml", big)
            _write(profiles / "UID_SMALL.yaml", AIRPORT_C)
            meta = td / "profiles.yaml"
            _write(meta, self._profiles_meta(
                "  - uid: UID_BIG\n    type: remote\n    file: UID_BIG.yaml\n    updated: 1\n"
                "  - uid: UID_SMALL\n    type: remote\n    file: UID_SMALL.yaml\n    updated: 1\n"
            ))
            res = load_candidate_nodes(max_n=2, profiles_meta=str(meta), profiles_dir=str(profiles))
            self.assertTrue(res.ok)
            names = {n["name"] for n in res.nodes}
            # 轮转合并：即使大订阅节点更多，小订阅的第一个节点也应该进前 2 个
            self.assertIn("C-Node-1", names)


if __name__ == "__main__":
    unittest.main()
