"""Read-only end-to-end benchmark for subtree_pipeline_experiment."""

from __future__ import annotations

import collections
import concurrent.futures
import json
import os
import sqlite3
import statistics
import threading
import time

import requests

import config
import fetch_subtree as production
from subtree_pipeline_experiment import BoundedParsePipeline


ACTIVE_IPS = 16
STREAMS_PER_IP = 2
PARSE_PROCESSES = 8
MAX_PENDING_PAGES = 256
DURATION_SECONDS = 60
EQUIVALENCE_PAGES = 100


def load_entries():
    with open(config.PROXY_POOL_FILE, encoding="utf-8") as handle:
        payload = json.load(handle)
    entries = payload.get("entries", []) if isinstance(payload, dict) else payload
    entries = [entry for entry in entries if isinstance(entry, dict) and entry.get("proxy")]
    if len(entries) < ACTIVE_IPS:
        raise RuntimeError(f"need {ACTIVE_IPS} active proxies, found {len(entries)}")
    return entries[:ACTIVE_IPS]


def load_rows():
    connection = sqlite3.connect(f"file:{config.DB_FILE}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT slug,node_id FROM categories "
            "WHERE site='US' AND node_id IS NOT NULL AND slug!='' LIMIT 3000"
        ).fetchall()
    finally:
        connection.close()
    if len(rows) < ACTIVE_IPS * STREAMS_PER_IP:
        raise RuntimeError("insufficient benchmark URLs")
    return rows


def make_sessions(entry, identity):
    user_agent = production.USER_AGENTS[identity % len(production.USER_AGENTS)]
    sessions = []
    for _ in production.CHART_PREFIXES:
        session = requests.Session()
        session.headers.update({
            **production.HEADERS,
            "User-Agent": user_agent,
            "Accept-Language": production._LANG,
        })
        session.proxies.update({"http": entry["proxy"], "https": entry["proxy"]})
        sessions.append(session)
    return sessions


def chart_urls(slug, node_id):
    return [
        production.normalize_url(f"{production._DOMAIN}{prefix}{slug}/{node_id}/")
        for prefix in production.CHART_PREFIXES
    ]


def fetch_batch(sessions, urls, executor):
    def fetch_one(pair):
        session, url = pair
        markup, reason = production._safe_get(session, url)
        return url, markup, reason

    return list(executor.map(fetch_one, zip(sessions, urls)))


def validate_equivalence(entries, rows, pipeline):
    sessions = make_sessions(entries[0], 0)
    mismatches = []
    compared = 0
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            row_index = 0
            while compared < EQUIVALENCE_PAGES:
                slug, node_id = rows[row_index]
                row_index += 1
                fetched = fetch_batch(sessions, chart_urls(slug, node_id), executor)
                for url, markup, reason in fetched:
                    if not markup:
                        raise RuntimeError(f"equivalence fetch failed: {reason} {url}")
                    future = pipeline.submit(markup, url, production._DOMAIN)
                    expected = production.parse_sidebar_children(markup, url)
                    actual = future.result(timeout=30)
                    compared += 1
                    if actual != expected:
                        mismatches.append((url, expected, actual))
                    if compared >= EQUIVALENCE_PAGES:
                        break
    finally:
        for session in sessions:
            session.close()
    if mismatches:
        raise RuntimeError(f"parser mismatch: {mismatches[0]!r}")
    return compared


def run_benchmark(entries, rows, pipeline):
    work_items = [
        (proxy_index, stream_index, entry)
        for proxy_index, entry in enumerate(entries)
        for stream_index in range(STREAMS_PER_IP)
    ]
    start_gate = threading.Barrier(len(work_items))
    deadline = [0.0]
    metrics_lock = threading.Lock()
    metrics = {
        "network_nodes": 0,
        "parsed_nodes": 0,
        "requests": 0,
        "ok": 0,
        "parse_errors": [],
        "children": 0,
    }
    per_ip_parsed = collections.Counter()

    def producer(worker_id, proxy_index, stream_index, entry):
        sessions = make_sessions(entry, proxy_index)
        row_position = worker_id
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as fetch_executor:
                start_gate.wait()
                if worker_id == 0:
                    deadline[0] = time.monotonic() + DURATION_SECONDS
                while deadline[0] == 0.0:
                    time.sleep(0.001)

                while time.monotonic() < deadline[0]:
                    slug, node_id = rows[row_position % len(rows)]
                    row_position += len(work_items)
                    fetched = fetch_batch(sessions, chart_urls(slug, node_id), fetch_executor)
                    successes = [(url, markup) for url, markup, _reason in fetched if markup]
                    with metrics_lock:
                        metrics["requests"] += len(fetched)
                        metrics["ok"] += len(successes)
                    if len(successes) != len(production.CHART_PREFIXES):
                        continue
                    with metrics_lock:
                        metrics["network_nodes"] += 1

                    tracker = {"remaining": len(successes), "failed": False, "children": 0}
                    tracker_lock = threading.Lock()

                    def parsed_callback(
                        future,
                        tracker=tracker,
                        tracker_lock=tracker_lock,
                        proxy_index=proxy_index,
                    ):
                        try:
                            children = future.result()
                        except BaseException as exc:
                            children = []
                            with tracker_lock:
                                tracker["failed"] = True
                            with metrics_lock:
                                metrics["parse_errors"].append(repr(exc))
                        finished = False
                        with tracker_lock:
                            tracker["remaining"] -= 1
                            tracker["children"] += len(children)
                            finished = tracker["remaining"] == 0
                            failed = tracker["failed"]
                            child_count = tracker["children"]
                        if finished and not failed and time.monotonic() <= deadline[0]:
                            with metrics_lock:
                                metrics["parsed_nodes"] += 1
                                metrics["children"] += child_count
                                per_ip_parsed[proxy_index] += 1

                    for url, markup in successes:
                        pipeline.submit(markup, url, production._DOMAIN).add_done_callback(parsed_callback)
        finally:
            for session in sessions:
                session.close()

    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(work_items)) as executor:
        futures = [
            executor.submit(producer, worker_id, proxy_index, stream_index, entry)
            for worker_id, (proxy_index, stream_index, entry) in enumerate(work_items)
        ]
        for future in futures:
            future.result()
    producer_elapsed = time.monotonic() - started
    pipeline.shutdown(wait=True)

    per_ip_rates = [per_ip_parsed[index] / DURATION_SECONDS * 60 for index in range(len(entries))]
    summary = {
        "active_ips": len(entries),
        "streams_per_ip": STREAMS_PER_IP,
        "parse_processes": PARSE_PROCESSES,
        "duration_sec": DURATION_SECONDS,
        "producer_elapsed_sec": round(producer_elapsed, 2),
        "network_nodes": metrics["network_nodes"],
        "parsed_nodes_within_window": metrics["parsed_nodes"],
        "requests": metrics["requests"],
        "ok": metrics["ok"],
        "failed": metrics["requests"] - metrics["ok"],
        "parse_errors": len(metrics["parse_errors"]),
        "pool_node_rpm": round(metrics["parsed_nodes"] / DURATION_SECONDS * 60, 2),
        "per_ip_node_rpm_min": round(min(per_ip_rates), 2),
        "per_ip_node_rpm_median": round(statistics.median(per_ip_rates), 2),
        "per_ip_node_rpm_mean": round(statistics.mean(per_ip_rates), 2),
        "per_ip_node_rpm_max": round(max(per_ip_rates), 2),
        "children_checksum": metrics["children"],
    }
    return summary


def main():
    entries = load_entries()
    rows = load_rows()
    pipeline = BoundedParsePipeline(PARSE_PROCESSES, MAX_PENDING_PAGES)
    compared = validate_equivalence(entries, rows, pipeline)
    print("EQUIVALENCE", {"pages": compared, "mismatches": 0}, flush=True)
    summary = run_benchmark(entries, rows, pipeline)
    print("SUMMARY", summary, flush=True)
    if summary["parse_errors"]:
        raise SystemExit(2)
    if summary["per_ip_node_rpm_mean"] < 65 or summary["pool_node_rpm"] < 1000:
        raise SystemExit(3)


if __name__ == "__main__":
    main()
