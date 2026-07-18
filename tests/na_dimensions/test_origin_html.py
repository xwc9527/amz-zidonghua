"""产地解析多结构 / 多站点 / 边界（单格标签形态预期 FAIL）。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from detail_parser import parse_detail_fields
from tests.na_dimensions.helpers import CaseResult, SuiteCollector

CASES = [
    ("TH-TD", "US", """
        <div id="prodDetails"><table>
          <tr><th>Country of Origin</th><td>China</td></tr>
        </table></div>""", "China", True),
    ("LI-SPAN", "US", """
        <div id="detailBullets_feature_div">
          <li><span class="a-text-bold">Country of Origin</span><span>China</span></li>
        </div>""", "China", True),
    ("TD-COLON-SPACE", "US", """
        <div id="prodDetails"><table>
          <tr><td>Country of Origin : China</td></tr>
        </table></div>""", "China", False),  # 已知缺陷，预期 FAIL
    ("TD-COLON", "US", """
        <div id="prodDetails"><table>
          <tr><td>Country of Origin: China</td></tr>
        </table></div>""", "China", False),
    ("TD-FULLWIDTH", "US", """
        <div id="prodDetails"><table>
          <tr><td>Country of Origin：China</td></tr>
        </table></div>""", "China", False),
    ("TD-SPLIT-TWO", "US", """
        <div id="prodDetails"><table>
          <tr><td>Country of Origin</td><td>China</td></tr>
        </table></div>""", "China", True),
    ("DE-HERKUNFT", "DE", """
        <div id="prodDetails"><table>
          <tr><th>Herkunftsland</th><td>China</td></tr>
        </table></div>""", "China", True),
    ("JP-ORIGIN", "JP", """
        <div id="prodDetails"><table>
          <tr><th>原産国</th><td>中国</td></tr>
        </table></div>""", "中国", True),
    ("BIDI", "US", """
        <div id="prodDetails"><table>
          <tr><th>Country of Origin</th><td>\u200eChina\u200f</td></tr>
        </table></div>""", "China", True),  # 允许含控制符，比较时 strip
    ("SPACES", "US", """
        <div id="prodDetails"><table>
          <tr><th>Country of Origin</th><td>  China  </td></tr>
        </table></div>""", "China", True),
    ("CASE", "US", """
        <div id="prodDetails"><table>
          <tr><th>country of origin</th><td>china</td></tr>
        </table></div>""", "china", True),
    ("MISSING", "US", """
        <div id="prodDetails"><table>
          <tr><th>Item Weight</th><td>1 lb</td></tr>
        </table></div>""", None, True),
    ("EMPTY", "US", """
        <div id="prodDetails"><table>
          <tr><th>Country of Origin</th><td></td></tr>
        </table></div>""", None, True),
    ("MULTI", "US", """
        <div id="prodDetails"><table>
          <tr><th>Country of Origin</th><td>China</td></tr>
          <tr><th>Country of Origin</th><td>USA</td></tr>
        </table></div>""", "China", True),  # 取首次或末次均可，但必须是纯国家名
]


def run_origin_html(col: SuiteCollector):
    for name, site, html, expected, expect_pass_quality in CASES:
        d = parse_detail_fields(f"<!doctype html><html><body>{html}</body></html>", site)
        actual = d.get("country_of_origin")
        if actual is not None:
            # 去方向控制符与空白再比
            norm = actual.replace("\u200e", "").replace("\u200f", "").strip()
        else:
            norm = None

        if expected is None:
            ok = norm in (None, "")
        else:
            ok = norm == expected if expect_pass_quality else (norm == expected)
            # 对已知单格缺陷：若含标签前缀则 FAIL
            if not expect_pass_quality:
                ok = norm == expected  # 仍按严格预期，失败即保留证据

        # CASE 标签小写：当前解析靠关键字 "Country of Origin" 大小写敏感？
        if name == "CASE" and not ok:
            # 记录实际行为
            pass

        if name == "BIDI" and norm and "China" in norm:
            ok = norm == "China" or norm.strip() == "China"

        if name == "MULTI":
            ok = norm in ("China", "USA") and "Country" not in (norm or "")

        if name == "SPACES":
            ok = norm == "China"

        col.add(CaseResult(
            f"COO-{name}", "产地", "详情解析",
            "PASS" if ok else "FAIL",
            expected, actual,
            detail=f"site={site} expect_pass_quality={expect_pass_quality}",
            severity="P2" if not ok else "",
        ))
