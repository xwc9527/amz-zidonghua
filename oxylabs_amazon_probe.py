"""One-request Oxylabs Amazon Search smoke test. No changes to existing AMZ crawler.

Docs:
https://developers.oxylabs.io/api-reference/dedicated-scrapers/commerce-scrapers
https://developers.oxylabs.io/products/web-api/scrape

Usage:
    OXYLABS_WEB_API_KEY=... python oxylabs_amazon_probe.py \
        --keyword 'behind couch table' --domain com --page 1
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ENDPOINT = "https://webapi.oxylabs.io/v1/scrape/amazon/search"


class ProbeError(Exception):
    pass


def build_payload(keyword: str, domain: str = "com", page: int = 1) -> dict:
    if not keyword.strip():
        raise ProbeError("keyword must not be empty")
    if not domain.strip():
        raise ProbeError("domain must not be empty")
    if page < 1:
        raise ProbeError("page must be >= 1")
    return {
        "query": keyword.strip(),
        "domain": domain.strip(),
        "start_page": page,
        "pages": 1,  # Strictly one chargeable page per smoke run.
        "output": ["json"],  # Built-in Amazon parser; no AI parsing surcharge.
    }


def fetch_once(payload: dict, api_key: str, *, timeout: int = 150) -> dict:
    if not api_key:
        raise ProbeError("OXYLABS_WEB_API_KEY is not set; no paid request was sent")
    req = Request(
        ENDPOINT,
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            http_status = resp.status
    except HTTPError as exc:
        # No credential, token or full response is logged on failure.
        try:
            data = json.loads(exc.read().decode("utf-8"))
            request_id = (data.get("metadata") or {}).get("request_id", "") if isinstance(data, dict) else ""
            message = (data.get("message") or data.get("error") or "") if isinstance(data, dict) else ""
        except (ValueError, UnicodeError):
            request_id, message = "", ""
        raise ProbeError(f"Oxylabs HTTP {exc.code}; request_id={request_id}; error={str(message)[:200]}") from None
    except (URLError, TimeoutError, OSError) as exc:
        raise ProbeError(f"Oxylabs transport error: {type(exc).__name__}") from None
    if http_status != 200:
        raise ProbeError(f"unexpected HTTP status {http_status}")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeError):
        raise ProbeError("HTTP 200 but invalid JSON response") from None
    if not isinstance(data, dict):
        raise ProbeError("HTTP 200 but response envelope is not an object")
    return data


def _parsed_blocks(data: dict) -> list[dict]:
    outputs = data.get("results")
    if not isinstance(outputs, list) or not outputs:
        raise ProbeError("no results[] in Oxylabs envelope")
    parsed = []
    for result in outputs:
        if not isinstance(result, dict):
            continue
        obj = result.get("json")
        if isinstance(obj, str):
            try:
                obj = json.loads(obj)
            except ValueError:
                pass
        if isinstance(obj, dict):
            parsed.append(obj)
    if not parsed:
        raise ProbeError("results[] exists but no parsed results[].json; cannot claim success")
    return parsed


def parse_products(data: dict) -> tuple[list[dict], dict]:
    """Accept documented Web API envelope + Oxylabs official Amazon organic format.

    Fail explicitly on unrecognized shape; never mistake empty parsing for success.
    """
    parsed_blocks = _parsed_blocks(data)
    products: list[dict] = []
    found_organic = False
    for parsed in parsed_blocks:
        section = parsed.get("results")
        # Oxylabs official Amazon search example has content.results.organic.
        if isinstance(section, dict) and isinstance(section.get("organic"), list):
            organic = section["organic"]
        elif isinstance(parsed.get("organic"), list):
            organic = parsed["organic"]
        else:
            continue
        found_organic = True
        for row in organic:
            if not isinstance(row, dict):
                continue
            asin = str(row.get("asin") or "").strip()
            title = str(row.get("title") or "").strip()
            image_url = row.get("url_image") or row.get("image_url") or ""
            price = row.get("price")
            # Exclude missing ASIN entries; report incomplete fields separately.
            if len(asin) != 10 or not asin.isalnum():
                continue
            products.append({
                "asin": asin,
                "title": title,
                "price": price,
                "image_url": image_url,
                "rating": row.get("rating"),
                "review_count": row.get("reviews_count"),
                "sponsored": bool(row.get("is_sponsored", False)),
            })
    if not found_organic:
        keys = [sorted(p.keys()) for p in parsed_blocks]
        raise ProbeError(f"unknown Amazon parsed schema; parsed top-level keys={keys}")
    # Dedupe by ASIN within one page. Keep first occurrence.
    unique: dict[str, dict] = {}
    for product in products:
        unique.setdefault(product["asin"], product)
    products = list(unique.values())
    complete = sum(bool(p["title"] and p["image_url"] and p["price"] is not None) for p in products)
    meta = data.get("metadata") or {}
    report = {
        "request_id": meta.get("request_id", "") if isinstance(meta, dict) else "",
        "unique_asins": len(products),
        "complete_core_fields": complete,
        "missing_core_fields": len(products) - complete,
        "pass": complete > 0,
    }
    return products, report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Probe one Amazon keyword page through Oxylabs")
    parser.add_argument("--keyword", default="behind couch table")
    parser.add_argument("--domain", default="com")
    parser.add_argument("--page", type=int, default=1)
    args = parser.parse_args(argv)
    try:
        payload = build_payload(args.keyword, args.domain, args.page)
        data = fetch_once(payload, os.getenv("OXYLABS_WEB_API_KEY", ""))
        products, report = parse_products(data)
        result = {
            "query": payload["query"],
            "domain": payload["domain"],
            "page": payload["start_page"],
            **report,
            "sample_products": products[:5],
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0 if report["pass"] else 2
    except ProbeError as exc:
        print(f"FAILED: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
