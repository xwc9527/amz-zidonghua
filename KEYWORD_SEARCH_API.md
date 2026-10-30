# Keyword product discovery

`POST /api/v2/keyword_search` is registered in the existing `api_server:app`.
It calls Oxylabs in real time using the already verified
`oxylabs_amazon_probe.build_payload`, `fetch_once`, and `parse_products` functions.
The existing category/list routes, database schemas, active run cache, and
detail crawler are unchanged.

## Run and call

Install the existing `requirements.txt`, configure `OXYLABS_WEB_API_KEY` in the
server's environment, and start the AMZ server as usual. The key is never a
request-body parameter and is not included in responses.

```sh
uvicorn api_server:app --host 127.0.0.1 --port 8081
curl --max-time 1500 http://127.0.0.1:8081/api/v2/keyword_search \
  -H 'Content-Type: application/json' \
  -d '{"site":"US","keywords":["behind couch table","narrow console table","sofa table with outlet"],"start_page":1,"pages":3}'
```

- `site` is required and uses the existing `config.get_marketplace` site codes,
  domains and currency codes. US has been verified live; other configured
  marketplaces still need site-specific live acceptance.
- `keywords` is a nonempty list of nonblank strings, each at most 2048
  characters. Whitespace is trimmed and duplicate keywords are collected once.
- `start_page` and `pages` are positive JSON integers, defaulting to 1.
  Page tasks cover `start_page` through `start_page + pages - 1` for each keyword.
- At most 18 keyword-page tasks are allowed per AMZ request. The whole workload
  is validated before sending any paid request. Unknown body fields are rejected.
- Calls are sequential, one Oxylabs `pages=1` request per page, with the existing
  150-second timeout and no automatic paid retry. The route runs in a FastAPI
  worker thread; callers should allow for the total requested workload duration.

## Response

The JSON envelope contains `status`, `provider`, normalized request parameters,
`summary`, `page_results`, and `products`.

Each page result carries its keyword, page, Request ID, elapsed time, API parsing
statuses, source URL, counts, data-valid flag, and error. HTTP 200 alone is not
accepted: the source request and JSON parser must succeed, and the returned
keyword, page and Amazon host/URL parameters must match the request.

The product collection is deduplicated by ASIN within the selected marketplace,
across all requested keywords and pages. All occurrences remain in `sources`.
The first observed record is retained, except that a later complete observed
record replaces an incomplete one. Fields from different offers are not mixed.

| AMZ product field | Source / meaning |
| --- | --- |
| `asin` | Oxylabs `asin`, validated as 10 uppercase letters/digits |
| `name`, `title` | Oxylabs `title`; `name` matches the existing AMZ product structure |
| `price`, `price_value` | Oxylabs numeric price, only when finite and greater than zero; otherwise JSON `null` |
| `price_raw` | Original price, preserving zero/missing/invalid values for review |
| `price_status` | `valid`, `zero`, `missing`, or `invalid` |
| `price_valid` | Whether `price` is a usable positive price |
| `currency`, `site` | Existing marketplace configuration |
| `image_url` | Existing probe mapping from `url_image` / `image_url` |
| `product_url` | Configured Amazon marketplace domain + `/dp/{asin}` |
| `rating`, `review_count` | Existing probe mapping from `rating` and `reviews_count` |
| `sponsored` | Existing probe's `is_sponsored` mapping; preserved per occurrence |
| `list_type` | `keyword-search`; not a category/bestseller list |
| `node_id`, `category_name` | Empty because keyword discovery does not establish a category |
| `detail_scraped`, `detail_status` | `0`, `pending`; search data does not imply detail enrichment |
| `scraped_at` | UTC timestamp of the selected product observation |
| `core_fields_complete` | Valid ASIN, nonempty title, usable positive price and valid HTTP(S) image URL |
| `sources[]` | Keyword, page, Request ID, search URL, timestamp, raw price/status and sponsored flag for every occurrence |

`summary.unique_asins` counts discoverable ASINs, including products needing
price enrichment. `summary.usable_products` counts products with all four core
fields complete. Do not treat zero as a free product price. No search position
is mapped to BSR. Missing detail-only fields are not invented.

Returned product dictionaries can be passed to the existing product/run-cache
processing functions; this endpoint itself does not start a crawl, activate a
new cache generation, persist products, or claim that detail processing ran.
Keep the response's `sources` when a caller passes products to subsequent stages,
because the existing cache schema does not store keyword provenance.

## Result and error semantics

| HTTP | `status` | Meaning |
| --- | --- | --- |
| 200 | `ok` | Every page parsed successfully and ASIN candidates were returned; inspect `usable_products` for complete products |
| 200 | `empty` | All pages parsed successfully but no valid ASIN was returned; not a successful product-data acceptance result |
| 207 | `partial` | Some pages failed, with remaining products and each page error retained |
| 502 | `error` | Page errors occurred and no product can be returned |
| 503 | `error` | Server has no `OXYLABS_WEB_API_KEY`; no paid call sent |
| 400 | `error` | Invalid supported-body values or workload; no paid call sent |
| 422 | framework validation | Body is missing or not a JSON object |

Do not retry the whole request automatically after a partial result: that would
repeat successful paid pages. Page errors are explicit and can be reviewed first.
An all-zero-price page may return ASIN candidates while `usable_product_count`
is zero. This is discovery data, not evidence of complete or enriched products.

## Verification

Offline tests cover validation before spending, missing credentials, the actual
AMZ route registration, field mapping, multi-keyword/page deduplication and
provenance, zero/missing/invalid prices, empty data, failed parsing, mismatched
pages, partial failure without retries, and credential redaction.

Live acceptance starts the existing `api_server:app` in an isolated SQLite
environment, sends an HTTP POST to its new route, and records the actual Oxylabs
responses. `TESTING=1` only isolates paths and disables the unrelated proxy daemon;
it does not mock or bypass Oxylabs. The delivery contains the actual AMZ request,
response, upstream evidence, field review workbook, and acceptance report.

This short acceptance is not a claim of long-term stability or verified account
billing. Reference: [Oxylabs Amazon search parameters](https://developers.oxylabs.io/api-reference/dedicated-scrapers/commerce-scrapers).

Live AMZ HTTP acceptance on 2026-10-30 used the three example keywords and pages
1–3. The AMZ request returned HTTP 200 in 44.277 seconds. Its 9 real upstream
requests all succeeded, yielding 432 observations and 287 distinct ASINs.
284 products had all four core fields; 3 products had zero raw prices and were
returned with `price=null` and `price_status=zero`. No paid retry was performed.
