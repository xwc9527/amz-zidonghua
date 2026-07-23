"""Independent experiment: bounded multi-process sidebar parsing.

This module deliberately has no crawler/database lifecycle code.  It can be
benchmarked and discarded without changing ``fetch_subtree.py``.
"""

from __future__ import annotations

import re
import threading
from concurrent.futures import Future, ProcessPoolExecutor

from bs4 import BeautifulSoup


def normalize_url(url: str) -> str:
    url = url.split("?")[0]
    url = re.sub(r"/ref=.*$", "/", url)
    if not url.endswith("/"):
        url += "/"
    return url


def extract_node_id(url: str) -> str | None:
    match = re.search(r"/(\d{3,})", url)
    return match.group(1) if match else None


def extract_slug(url: str) -> str | None:
    match = re.search(
        r"/gp/(?:new-releases|bestsellers|movers-and-shakers|most-wished-for|most-gifted)/([a-z][a-z0-9-]+)",
        url,
    )
    if match:
        return match.group(1)
    match = re.search(r"/zg(?:bs|ns)/([a-z][a-z0-9-]+)", url)
    return match.group(1) if match else None


def parse_sidebar_payload(markup: str, page_url: str, domain: str) -> list[dict]:
    """Process-safe equivalent of fetch_subtree.parse_sidebar_children."""
    soup = BeautifulSoup(markup, "html.parser")
    root = soup.select_one("ul[class*='zg-browse-root']")
    if not root:
        return []

    selected = root.select_one("[class*='zg-selected']")
    if selected:
        current_li = selected.find_parent("li")
        next_li = current_li.find_next_sibling("li") if current_li else None
        container = next_li.select_one("ul[class*='zg-browse-group']") if next_li else None
    else:
        groups = root.select("ul[class*='zg-browse-group']")
        container = groups[-1] if groups else None
    if not container:
        return []

    seen = set()
    results = []
    for li in container.find_all("li", recursive=False):
        if "browse-up" in " ".join(li.get("class", [])):
            continue
        anchor = (
            li.select_one("a[href*='/gp/']")
            or li.select_one("a[href*='/zgbs/']")
            or li.select_one("a[href*='/zgns/']")
            or li.select_one("a[href]")
        )
        if not anchor:
            continue
        name = anchor.get_text(strip=True).replace("\xa0", " ").replace("​", "").strip()
        href = anchor.get("href", "")
        if not name or not href or name.isdigit():
            continue
        if href.startswith("/"):
            href = domain + href
        href = normalize_url(href)
        if href in seen:
            continue
        seen.add(href)
        results.append({
            "name": name,
            "url": href,
            "node_id": extract_node_id(href),
            "slug": extract_slug(href) or extract_slug(page_url) or "",
        })
    return results


class BoundedParsePipeline:
    """A process pool with a hard cap on submitted-but-unfinished pages."""

    def __init__(self, workers: int, max_pending: int):
        if workers < 1 or max_pending < workers:
            raise ValueError("workers must be >=1 and max_pending must be >= workers")
        self._executor = ProcessPoolExecutor(max_workers=workers)
        self._slots = threading.BoundedSemaphore(max_pending)
        self._closed = False
        self._state_lock = threading.Lock()

    def submit(self, markup: str, page_url: str, domain: str) -> Future:
        with self._state_lock:
            if self._closed:
                raise RuntimeError("parse pipeline is closed")
        self._slots.acquire()
        try:
            future = self._executor.submit(parse_sidebar_payload, markup, page_url, domain)
        except BaseException:
            self._slots.release()
            raise
        future.add_done_callback(lambda _future: self._slots.release())
        return future

    def shutdown(self, *, wait: bool = True, cancel_futures: bool = False) -> None:
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
        self._executor.shutdown(wait=wait, cancel_futures=cancel_futures)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.shutdown(wait=True, cancel_futures=exc_type is not None)
