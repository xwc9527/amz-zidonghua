# test_pagination.py — 第0步：验证新品榜能翻到第几页，目标覆盖排名100-1500
# 运行方式: python test_pagination.py
# 输出: 打印每页状态，确认可用页数上限

import requests
import time
import random
from bs4 import BeautifulSoup
from config import HEADERS, TEST_CATEGORIES, REQUEST_DELAY_MIN, REQUEST_DELAY_MAX

def fetch_page(category_slug: str, page: int) -> tuple[int, str]:
    """
    抓取指定类目新品榜的第N页。
    返回 (HTTP状态码, 页面HTML)
    """
    url = f"https://www.amazon.com/gp/new-releases/{category_slug}/?pg={page}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    return resp.status_code, resp.text


def parse_product_count(html: str) -> int:
    """从页面 HTML 中提取产品卡片数量，判断该页是否有效数据。"""
    soup = BeautifulSoup(html, "html.parser")
    # 新品榜产品卡片的容器 class（需根据实际页面确认）
    items = soup.select("div.p13n-sc-uncoverable-faceout, li.zg-item-immersion")
    return len(items)


def test_category(level: str, name: str, slug: str):
    print(f"\n{'='*50}")
    print(f"[{level}] {name}  →  slug: {slug}")
    print(f"{'='*50}")

    for page in [1, 2, 5, 10, 15]:
        status, html = fetch_page(slug, page)
        count = parse_product_count(html) if status == 200 else 0
        print(f"  第{page:>2}页 | HTTP {status} | 产品数: {count}")

        if status != 200 or count == 0:
            print(f"  ↑ 第{page}页无数据，停止该类目测试")
            break

        time.sleep(random.uniform(REQUEST_DELAY_MIN, REQUEST_DELAY_MAX))


if __name__ == "__main__":
    for level, name, slug in TEST_CATEGORIES:
        test_category(level, name, slug)

    print("\n[完成] 根据以上结果确认可用最大页数，更新 config.py 的 NEW_RELEASES_PAGE_END")
