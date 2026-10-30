"""Independent keyword discovery API using the verified Oxylabs probe."""
from __future__ import annotations

from datetime import datetime, timezone
import math
import os
import re
import time
from urllib.parse import parse_qs, urlparse

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from config import get_marketplace
import oxylabs_amazon_probe as oxylabs

router = APIRouter()
MAX_PAGE_TASKS = 18
ASIN_RE = re.compile(r"[A-Z0-9]{10}\Z")


def validate_search(body: dict) -> dict:
    """Validate the complete paid workload before sending any request."""
    if set(body) - {"site", "keywords", "start_page", "pages"}:
        raise ValueError("Only site, keywords, start_page and pages are supported")
    site = body.get("site")
    if not isinstance(site, str) or not site.strip():
        raise ValueError("site is required")
    site = site.strip().upper()
    market = get_marketplace(site)
    raw_keywords = body.get("keywords")
    if not isinstance(raw_keywords, list) or not raw_keywords:
        raise ValueError("keywords must be a non-empty list of strings")
    keywords = []
    for keyword in raw_keywords:
        if not isinstance(keyword, str) or not keyword.strip() or len(keyword.strip()) > 2048:
            raise ValueError("Each keyword must contain 1 to 2048 characters")
        keyword = keyword.strip()
        if keyword not in keywords:
            keywords.append(keyword)
    start_page, pages = body.get("start_page", 1), body.get("pages", 1)
    if type(start_page) is not int or start_page < 1:
        raise ValueError("start_page must be an integer >= 1")
    if type(pages) is not int or pages < 1:
        raise ValueError("pages must be an integer >= 1")
    if len(keywords) * pages > MAX_PAGE_TASKS:
        raise ValueError(f"A search may collect at most {MAX_PAGE_TASKS} keyword-page tasks")
    domain = urlparse(market["domain"]).hostname.removeprefix("www.amazon.")
    return {"site": site, "keywords": keywords, "start_page": start_page, "pages": pages,
            "domain": domain, "market": market}


def normalize_price(raw):
    if raw is None:
        return None, "missing"
    if not isinstance(raw, (int, float)) or isinstance(raw, bool) or not math.isfinite(raw):
        return None, "invalid"
    if raw == 0:
        return None, "zero"
    if raw < 0:
        return None, "invalid"
    return raw, "valid"


def _image_valid(url):
    if not isinstance(url, str):
        return False
    parsed = urlparse(url)
    return parsed.scheme in {"https", "http"} and bool(parsed.hostname)


def _normalize_product(item, *, search, source):
    raw = item.get("price")
    price, price_status = normalize_price(raw)
    name = item["title"]
    image = item.get("image_url") or ""
    complete = bool(name and _image_valid(image) and price is not None)
    # Match the existing products/run-cache names. Search rank is not a BSR.
    return {
        "site": search["site"], "asin": item["asin"], "name": name, "title": name,
        "price": price, "price_value": price,
        "price_raw": raw if not isinstance(raw, float) or math.isfinite(raw) else str(raw),
        "price_status": price_status, "price_valid": price is not None,
        "currency": search["market"]["currency_code"],
        "image_url": image,
        "product_url": f'{search["market"]["domain"]}/dp/{item["asin"]}',
        "rating": item.get("rating"), "review_count": item.get("review_count"),
        "sponsored": item.get("sponsored", False),
        "list_type": "keyword-search", "node_id": "", "category_name": "",
        "detail_scraped": 0, "detail_status": "pending",
        "scraped_at": source["scraped_at"], "core_fields_complete": complete,
        "sources": [{**source, "price_raw": raw if not isinstance(raw, float) or math.isfinite(raw) else str(raw), "price_status": price_status,
                     "sponsored": item.get("sponsored", False)}],
    }


def _verify_page(data, search, keyword, page):
    """Use response states and returned identities, not HTTP 200 alone."""
    if data.get("state") != "done":
        raise oxylabs.ProbeError("Oxylabs response state is not done")
    outputs = data.get("results")
    if not isinstance(outputs, list) or not outputs:
        raise oxylabs.ProbeError("Oxylabs response has no page results")
    statuses = [(r.get("metadata") or {}).get("statuses", {}) for r in outputs if isinstance(r, dict)]
    if not statuses or any((s.get("http_request") or {}).get("code") != 200 or
                           (s.get("json_parse") or {}).get("tag") != "PARSE_SUCCESS" for s in statuses):
        raise oxylabs.ProbeError("Oxylabs source request or Amazon JSON parsing failed")
    blocks = oxylabs._parsed_blocks(data)
    host = urlparse(search["market"]["domain"]).hostname
    raw_count = 0
    search_url = None
    for block in blocks:
        url = urlparse(block.get("url") or "")
        query = parse_qs(url.query)
        if (block.get("query") != keyword or block.get("page") != page or url.hostname != host or
                query.get("k") != [keyword] or query.get("page") != [str(page)]):
            raise oxylabs.ProbeError("Oxylabs response keyword, marketplace or page does not match the request")
        search_url = block["url"]
        section = block.get("results", block)
        if isinstance(section, dict) and isinstance(section.get("organic"), list):
            raw_count += len(section["organic"])
    return statuses, search_url, raw_count


def discover_products(search: dict, api_key: str) -> dict:
    started = time.perf_counter()
    unique = {}
    page_results = []
    observations = 0
    for keyword in search["keywords"]:
        for page in range(search["start_page"], search["start_page"] + search["pages"]):
            page_started = time.perf_counter()
            result = {"keyword": keyword, "page": page, "status": "error", "http_status": None,
                      "api_parse_success": False, "data_valid": False, "request_id": None,
                      "raw_product_count": 0, "valid_asin_count": 0, "usable_product_count": 0,
                      "error": None}
            try:
                # One single-page call per task, with no automatic paid retry.
                data = oxylabs.fetch_once(oxylabs.build_payload(keyword, search["domain"], page), api_key)
                result["http_status"] = 200
                result["request_id"] = (data.get("metadata") or {}).get("request_id")
                statuses, search_url, count = _verify_page(data, search, keyword, page)
                result.update(api_parse_success=True, api_statuses=statuses, search_url=search_url,
                              raw_product_count=count)
                items, _ = oxylabs.parse_products(data)
                source = {"keyword": keyword, "page": page, "request_id": result["request_id"],
                          "search_url": search_url, "scraped_at": datetime.now(timezone.utc).isoformat()}
                for item in items:
                    if not ASIN_RE.fullmatch(item["asin"]):
                        continue
                    candidate = _normalize_product(item, search=search, source=source)
                    result["valid_asin_count"] += 1
                    result["usable_product_count"] += candidate["core_fields_complete"]
                    observations += 1
                    existing = unique.get(item["asin"])
                    if existing is None:
                        unique[item["asin"]] = candidate
                    else:
                        sources = existing["sources"] + candidate["sources"]
                        # Prefer a complete observed record over an incomplete one;
                        # never fabricate a record by mixing unrelated observations.
                        if candidate["core_fields_complete"] and not existing["core_fields_complete"]:
                            unique[item["asin"]] = candidate
                        unique[item["asin"]]["sources"] = sources
                result["data_valid"] = result["valid_asin_count"] > 0
                result["status"] = "ok" if result["data_valid"] else "empty"
            except oxylabs.ProbeError as exc:
                result["error"] = str(exc).replace(api_key, "[REDACTED]")
            result["elapsed_seconds"] = round(time.perf_counter() - page_started, 3)
            page_results.append(result)
    products = list(unique.values())
    failures = sum(p["status"] == "error" for p in page_results)
    status = "partial" if failures and products else "error" if failures else "ok" if products else "empty"
    return {
        "status": status, "provider": "oxylabs", "site": search["site"], "domain": search["domain"],
        "keywords": search["keywords"], "start_page": search["start_page"], "pages": search["pages"],
        "summary": {
            "requested_page_tasks": len(page_results), "successful_page_tasks": sum(p["status"] == "ok" for p in page_results),
            "empty_page_tasks": sum(p["status"] == "empty" for p in page_results), "failed_page_tasks": failures,
            "raw_product_count": sum(p["raw_product_count"] for p in page_results),
            "valid_asin_observations": observations, "duplicate_asin_observations": observations - len(products),
            "unique_asins": len(products), "usable_products": sum(p["core_fields_complete"] for p in products),
            "invalid_price_products": sum(not p["price_valid"] for p in products),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        },
        "page_results": page_results, "products": products,
    }


@router.post("/api/v2/keyword_search", tags=["Keyword discovery"])
def keyword_search(body: dict):
    """Discover Amazon products. Body: site, keywords[], start_page=1, pages=1.

    Blocks at most 18 page tasks in a FastAPI worker thread. Returns candidates
    without changing category crawls, active run caches, favorites or databases.
    """
    try:
        search = validate_search(body)
    except ValueError as exc:
        return JSONResponse({"status": "error", "error_code": "INVALID_KEYWORD_SEARCH", "msg": str(exc)}, status_code=400)
    api_key = os.getenv("OXYLABS_WEB_API_KEY", "").strip()
    if not api_key:
        return JSONResponse({"status": "error", "error_code": "OXYLABS_NOT_CONFIGURED",
                             "msg": "OXYLABS_WEB_API_KEY is not configured on the server"}, status_code=503)
    result = discover_products(search, api_key)
    code = 207 if result["status"] == "partial" else 502 if result["status"] == "error" else 200
    return JSONResponse(result, status_code=code)
