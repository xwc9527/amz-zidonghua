"""Run the production fixed-proxy detail path against an isolated ASIN cache."""

from __future__ import annotations

import json
import os
import queue
import sqlite3
import statistics
import threading
import time as std_time
from collections import Counter
from pathlib import Path


required = (
    "AMZ_RUN_CACHE_FILE",
    "AMZ_FETCH_PRODUCTS_AUDIT_LOG",
    "AMZ_FETCH_PRODUCTS_LOG",
    "AMZ_CHECKPOINT_DIR",
    "AMZ_RUN_ID",
)
missing = [name for name in required if not os.environ.get(name)]
if missing or os.environ.get("TESTING") != "1":
    raise SystemExit(f"isolated TESTING=1 environment required: {missing}")

import config

config.assert_testing_paths_safe()
import fetch_products
from proxy_worker import AttemptAuditor


TARGET = max(1, int(os.environ.get("BENCH_DETAIL_TARGET", "10000")))
STREAMS = max(1, int(os.environ.get("PRODUCT_STREAMS_PER_PROXY", "3")))
MAX_ATTEMPTS = max(1, int(os.environ.get("PRODUCT_TASK_MAX_ATTEMPTS", "3")))
DELAY = float(os.environ.get("BENCH_DETAIL_DELAY", "0.3"))
USE_PARSE_POOL = os.environ.get("BENCH_USE_PARSE_POOL", "1") == "1"


class IsolatedPool(fetch_products.ProxyPool):
    def __init__(self):
        super().__init__()
        self.feedback_enabled = False


class PacingMonitor:
    def __init__(self):
        self.local = threading.local()
        self.lock = threading.Lock()
        self.sleeps = []

    def set_context(self, worker, proxy_key, asin):
        self.local.worker = worker
        self.local.proxy_key = proxy_key
        self.local.asin = asin
        self.local.base = None

    def set_base(self, value):
        self.local.base = float(value)

    def record_sleep(self, requested, actual):
        base = getattr(self.local, "base", None)
        event = {
            "worker": getattr(self.local, "worker", -1),
            "proxy_key": getattr(self.local, "proxy_key", ""),
            "asin": getattr(self.local, "asin", ""),
            "base_sec": base,
            "jitter_sec": (
                max(0.0, float(requested) - base) if base is not None else None
            ),
            "requested_sec": float(requested),
            "actual_sec": float(actual),
        }
        self.local.base = None
        with self.lock:
            self.sleeps.append(event)


class TimeProxy:
    def __init__(self, real, monitor):
        self.real = real
        self.monitor = monitor

    def sleep(self, seconds):
        started = self.real.monotonic()
        self.real.sleep(seconds)
        self.monitor.record_sleep(seconds, self.real.monotonic() - started)

    def __getattr__(self, name):
        return getattr(self.real, name)


def percentile(values, pct):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    pos = (len(ordered) - 1) * pct
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    return ordered[lo] * (hi - pos) + ordered[hi] * (pos - lo)


def prepare_products():
    path = Path(config.PRODUCT_RUN_CACHE_FILE)
    run_id = fetch_products._RUN_ID
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        available = conn.execute(
            "SELECT COUNT(DISTINCT asin) FROM product_cache"
        ).fetchone()[0]
        if available < TARGET:
            raise SystemExit(f"asset cache has only {available}/{TARGET} ASINs")
        rows = conn.execute(
            """
            SELECT cache_id,asin,node_id,list_type,name,title,price,price_raw,
                   rating,review_count,rank,image_url,product_url,list_total,
                   category_name,category_slug,category_depth
              FROM product_cache
             GROUP BY asin
             ORDER BY cache_id
             LIMIT ?
            """,
            (TARGET,),
        ).fetchall()
        ids = [row["cache_id"] for row in rows]
        conn.executemany(
            """
            UPDATE product_cache
               SET run_id=?, detail_scraped=0, detail_status='pending'
             WHERE cache_id=?
            """,
            [(run_id, cache_id) for cache_id in ids],
        )
        conn.commit()
        return [{k: row[k] for k in row.keys() if k != "cache_id"} for row in rows]
    finally:
        conn.close()


def main():
    fetch_products.ProxyPool = IsolatedPool
    fetch_products.touch_crawl_activity = lambda **_kwargs: None
    fetch_products.export_excel = lambda: None
    products = prepare_products()
    if USE_PARSE_POOL:
        fetch_products._detail_parse_pipeline = fetch_products.BoundedDetailParsePipeline(
            fetch_products.PRODUCT_PARSE_WORKERS,
            fetch_products.PRODUCT_PARSE_MAX_PENDING,
        )
    fetch_products._detail_writer = fetch_products.AsyncBatchWriter(
        fetch_products._write_detail_batch,
        batch_size=fetch_products.PRODUCT_DETAIL_WRITE_BATCH_SIZE,
        flush_interval=fetch_products.PRODUCT_DETAIL_WRITE_FLUSH_SEC,
        max_pending_batches=32,
    )
    pool = IsolatedPool()
    entries = pool.usable_entries_snapshot()
    if len(entries) < 16:
        raise SystemExit(f"requires 16 usable proxies, got {len(entries)}")
    entries = entries[:16]

    pacing = PacingMonitor()
    real_pool_delay = fetch_products._pool_delay

    def monitored_pool_delay(delay, worker_pool=None):
        value = real_pool_delay(delay, worker_pool)
        pacing.set_base(value)
        return value

    fetch_products._pool_delay = monitored_pool_delay
    fetch_products.time = TimeProxy(std_time, pacing)

    tasks = queue.Queue()
    for product in products:
        tasks.put({
            "product": product,
            "attempt": 1,
            "used_proxies": set(),
        })

    worker_count = len(entries) * STREAMS
    barrier = threading.Barrier(worker_count + 1)
    lock = threading.RLock()
    shared_auditor = AttemptAuditor(
        os.environ["AMZ_FETCH_PRODUCTS_AUDIT_LOG"], fetch_products._RUN_ID
    )
    successes = []
    failures = []
    terminal_not_found = []
    proxy_success = Counter()
    worker_success = Counter()
    lane_stats = {}
    completed = 0
    started = 0.0
    stop = threading.Event()

    def record_lane_result(proxy_key, outcome):
        with lock:
            stats = lane_stats.setdefault(
                proxy_key, {"total": 0, "captcha": 0, "paused": False},
            )
            stats["total"] += 1
            if outcome.final_reason == "CAPTCHA":
                stats["captcha"] += 1
            if (
                stats["total"] >= fetch_products.PRODUCT_LANE_CAPTCHA_MIN_SAMPLES
                and stats["captcha"] / stats["total"]
                >= fetch_products.PRODUCT_LANE_CAPTCHA_PAUSE_RATE
            ):
                stats["paused"] = True

    def worker(worker_id, entry):
        nonlocal completed
        client = fetch_products.FixedProxyClient(
            pool, entry, worker_id, auditor=shared_auditor,
            on_result=lambda outcome: record_lane_result(proxy_key, outcome),
        )
        proxy_key = client.proxy_key
        barrier.wait()
        try:
            while not stop.is_set():
                try:
                    task = tasks.get(timeout=0.5)
                except queue.Empty:
                    with lock:
                        if completed >= TARGET:
                            break
                    continue
                if lane_stats.get(proxy_key, {}).get("paused"):
                    tasks.put(task)
                    tasks.task_done()
                    return
                used = set(task["used_proxies"])
                if proxy_key in used and len(used) < len(entries):
                    tasks.put(task)
                    tasks.task_done()
                    std_time.sleep(0.001)
                    continue
                product = task["product"]
                pacing.set_context(worker_id, proxy_key, product["asin"])
                failure = 1
                error = ""
                terminal_asins = set()
                try:
                    failure = fetch_products.enrich_with_details(
                        [product], client, DELAY, {},
                        terminal_asins=terminal_asins,
                    )
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                used.add(proxy_key)
                if product["asin"] in terminal_asins:
                    with lock:
                        terminal_not_found.append(product["asin"])
                        completed += 1
                elif not failure and not error:
                    with lock:
                        successes.append(product["asin"])
                        proxy_success[proxy_key] += 1
                        worker_success[worker_id] += 1
                        completed += 1
                elif task["attempt"] < MAX_ATTEMPTS:
                    tasks.put({
                        "product": product,
                        "attempt": task["attempt"] + 1,
                        "used_proxies": used,
                    })
                else:
                    with lock:
                        failures.append({
                            "asin": product["asin"],
                            "attempts": task["attempt"],
                            "used_proxies": sorted(used),
                            "error": error,
                        })
                        completed += 1
                tasks.task_done()
                with lock:
                    current = completed
                    if current and current % 500 == 0:
                        elapsed = std_time.monotonic() - started
                        print(
                            "DETAIL_PROGRESS "
                            + json.dumps({
                                "completed": current,
                                "success": len(successes),
                                "failed": len(failures),
                                "elapsed_sec": round(elapsed, 2),
                                "success_per_min": round(
                                    len(successes) * 60 / max(0.001, elapsed), 2
                                ),
                            }),
                            flush=True,
                        )
                    if current >= TARGET:
                        stop.set()
        finally:
            client.close()

    threads = []
    for proxy_index, entry in enumerate(entries):
        for offset in range(STREAMS):
            worker_id = proxy_index * STREAMS + offset
            thread = threading.Thread(
                target=worker, args=(worker_id, entry), daemon=True
            )
            thread.start()
            threads.append(thread)

    started = std_time.monotonic()
    barrier.wait()
    for thread in threads:
        thread.join()

    main_failures = list(failures)
    failures.clear()
    healthy_entries = [
        entry for entry in entries
        if not lane_stats.get(
            str(entry.get("exit_ip") or entry.get("node_key") or entry.get("proxy") or ""),
            {},
        ).get("paused")
    ]
    mopup_budget = min(fetch_products.PRODUCT_MOPUP_MAX_ATTEMPTS, len(healthy_entries))
    mopup_tasks = queue.Queue()
    for item in main_failures:
        mopup_tasks.put({
            "product": next(p for p in products if p["asin"] == item["asin"]),
            "attempt": 1,
            "used_proxies": set(),
        })

    def mopup_worker(worker_id, entry):
        client = fetch_products.FixedProxyClient(
            pool, entry, worker_id, auditor=shared_auditor,
        )
        proxy_key = client.proxy_key
        try:
            while True:
                item = mopup_tasks.get()
                try:
                    if item is None:
                        return
                    used = set(item["used_proxies"])
                    if proxy_key in used and len(used) < len(healthy_entries):
                        mopup_tasks.put(item)
                        std_time.sleep(0.001)
                        continue
                    product = item["product"]
                    pacing.set_context(worker_id, proxy_key, product["asin"])
                    terminal_asins = set()
                    error = ""
                    try:
                        failure = fetch_products.enrich_with_details(
                            [product], client, DELAY, {},
                            terminal_asins=terminal_asins,
                        )
                    except Exception as exc:
                        failure = 1
                        error = f"{type(exc).__name__}: {exc}"
                    used.add(proxy_key)
                    if product["asin"] in terminal_asins:
                        with lock:
                            terminal_not_found.append(product["asin"])
                    elif not failure and not error:
                        with lock:
                            successes.append(product["asin"])
                            proxy_success[proxy_key] += 1
                            worker_success[worker_id] += 1
                    elif item["attempt"] < mopup_budget:
                        mopup_tasks.put({
                            "product": product,
                            "attempt": item["attempt"] + 1,
                            "used_proxies": used,
                        })
                    else:
                        with lock:
                            failures.append({
                                "asin": product["asin"],
                                "attempts": item["attempt"],
                                "used_proxies": sorted(used),
                                "error": error,
                            })
                finally:
                    mopup_tasks.task_done()
        finally:
            client.close()

    mopup_threads = []
    for proxy_index, entry in enumerate(healthy_entries):
        for offset in range(STREAMS):
            worker_id = 100000 + proxy_index * STREAMS + offset
            thread = threading.Thread(
                target=mopup_worker, args=(worker_id, entry), daemon=True,
            )
            thread.start()
            mopup_threads.append(thread)
    mopup_tasks.join()
    for _ in mopup_threads:
        mopup_tasks.put(None)
    for thread in mopup_threads:
        thread.join()

    fetch_products._detail_writer.close()
    fetch_products._detail_writer = None
    if fetch_products._detail_parse_pipeline is not None:
        fetch_products._detail_parse_pipeline.shutdown(
            wait=True, cancel_futures=False,
        )
        fetch_products._detail_parse_pipeline = None
    elapsed = std_time.monotonic() - started
    pool.stop_live_reload()

    conn = sqlite3.connect(config.PRODUCT_RUN_CACHE_FILE)
    try:
        detail_rows = conn.execute(
            "SELECT COUNT(DISTINCT asin) FROM product_cache "
            "WHERE run_id=? AND detail_scraped=1",
            (fetch_products._RUN_ID,),
        ).fetchone()[0]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    finally:
        conn.close()

    audit = [
        json.loads(line)
        for line in Path(os.environ["AMZ_FETCH_PRODUCTS_AUDIT_LOG"])
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    detail_audit = [event for event in audit if event.get("phase") == "DETAIL"]
    jitters = [
        event["jitter_sec"]
        for event in pacing.sleeps
        if event["jitter_sec"] is not None
    ]
    wave = []
    for start_index in range(0, len(detail_audit), 500):
        chunk = detail_audit[start_index:start_index + 500]
        if not chunk:
            continue
        wave.append({
            "attempts": f"{start_index + 1}-{start_index + len(chunk)}",
            "success_rate": round(
                sum(event.get("result") == "SUCCESS" for event in chunk)
                / len(chunk),
                4,
            ),
            "mean_latency_ms": round(
                statistics.fmean(float(event.get("elapsed_ms") or 0) for event in chunk),
                2,
            ),
        })

    summary = {
        "target": TARGET,
        "success": len(successes),
        "terminal_not_found": len(terminal_not_found),
        "mainphase_failures": len(main_failures),
        "mopup_recovered": len(main_failures) - len(failures),
        "final_failures": len(failures),
        "unique_success_asins": len(set(successes)),
        "detail_rows": detail_rows,
        "elapsed_sec": round(elapsed, 3),
        "success_per_min": round(len(successes) * 60 / elapsed, 2),
        "per_proxy_per_min": round(
            len(successes) * 60 / elapsed / len(entries), 2
        ),
        "http_attempts": len(detail_audit),
        "attempt_success_rate": round(
            sum(event.get("result") == "SUCCESS" for event in detail_audit)
            / max(1, len(detail_audit)),
            4,
        ),
        "proxies": len(entries),
        "streams_per_proxy": STREAMS,
        "workers": worker_count,
        "active_proxies": len(proxy_success),
        "healthy_proxies": len(healthy_entries),
        "lane_stats": lane_stats,
        "proxy_success": dict(proxy_success),
        "worker_success": dict(worker_success),
        "pacing": {
            "sleep_count": len(pacing.sleeps),
            "coverage_per_attempt": round(
                len(pacing.sleeps) / max(1, len(detail_audit)), 4
            ),
            "jitter_ms": {
                "min": round(min(jitters) * 1000, 2),
                "mean": round(statistics.fmean(jitters) * 1000, 2),
                "p50": round(percentile(jitters, 0.50) * 1000, 2),
                "p95": round(percentile(jitters, 0.95) * 1000, 2),
                "max": round(max(jitters) * 1000, 2),
            },
        },
        "wave_per_500_attempts": wave,
        "integrity": integrity,
        "failures": failures,
        "production_feedback_enabled": False,
    }
    output = (
        Path(os.environ["AMZ_CHECKPOINT_DIR"])
        / "fixed_10000_detail_summary.json"
    )
    output.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print("FIXED_10000_SUMMARY " + json.dumps(summary, ensure_ascii=False), flush=True)

    if (
        len(successes) + len(terminal_not_found) != TARGET
        or failures
        or detail_rows != len(successes)
        or len(set(successes)) != len(successes)
        or len(proxy_success) != len(entries)
        or integrity != "ok"
    ):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
