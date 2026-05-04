# test_breadcrumb.py — 验证通过产品页面包屑发现隐藏类目节点ID
# 流程：新品榜取ASIN → 访问产品页 → 提取面包屑 → 打印节点ID路径
# 运行：python test_breadcrumb.py

import re
import time
import random
import requests
from bs4 import BeautifulSoup
from config import HEADERS, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX

TEST_URL = "https://www.amazon.com/gp/new-releases/kitchen/"


def get_asins_from_new_releases(url: str, limit: int = 5) -> list[str]:
    """从新品榜页面提取 ASIN，用于后续进入产品页抓面包屑。"""
    resp = requests.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(resp.text, "html.parser")

    asins = []
    # ASIN 藏在产品链接里，格式 /dp/XXXXXXXXXX
    for a in soup.select("a[href*='/dp/']"):
        match = re.search(r"/dp/([A-Z0-9]{10})", a.get("href", ""))
        if match:
            asin = match.group(1)
            if asin not in asins:
                asins.append(asin)
        if len(asins) >= limit:
            break

    return asins


def get_breadcrumb(asin: str) -> list[dict]:
    """
    访问产品页，提取面包屑导航。
    返回: [{"name": "Kitchen & Dining", "node_id": "284507"}, ...]
    """
    url = f"https://www.amazon.com/dp/{asin}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    soup = BeautifulSoup(resp.text, "html.parser")

    breadcrumb = []

    # 面包屑容器
    crumb_div = soup.select_one("#wayfinding-breadcrumbs_feature_div")
    if not crumb_div:
        return breadcrumb

    for a in crumb_div.select("a"):
        name = a.get_text(strip=True)
        href = a.get("href", "")
        node_match = re.search(r"node=(\d+)", href)
        node_id = node_match.group(1) if node_match else None
        if name:
            breadcrumb.append({"name": name, "node_id": node_id, "href": href})

    return breadcrumb


if __name__ == "__main__":
    print(f"[1] 从新品榜提取 ASIN: {TEST_URL}")
    asins = get_asins_from_new_releases(TEST_URL, limit=5)
    print(f"    找到 {len(asins)} 个 ASIN: {asins}\n")

    for asin in asins:
        print(f"[2] 抓取产品页面包屑: {asin}")
        crumbs = get_breadcrumb(asin)
        if crumbs:
            path = " > ".join(c["name"] for c in crumbs)
            print(f"    路径: {path}")
            for c in crumbs:
                print(f"    节点: {c['node_id']:>12}  {c['name']}")
        else:
            print("    未找到面包屑（可能触发验证码或结构不同）")
        print()
        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))
