"""Fire two simultaneous start requests at an isolated API instance."""
from __future__ import annotations

import argparse
import json
import threading
import urllib.request


def post(url: str, payload: dict) -> dict:
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.loads(response.read().decode("utf-8"))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:8082")
    args = parser.parse_args()
    payload = {
        "chart": "nr",
        "site": "US",
        "roots": ["home-garden"],
        "lists": ["new-releases"],
        "scope_mode": "tree",
        "include_descendants": False,
        "max_pages": 1,
        "price_min": 777777,
    }
    barrier = threading.Barrier(3)
    results: list[dict] = []
    errors: list[str] = []

    def run() -> None:
        barrier.wait()
        try:
            results.append(post(args.base + "/api/v2/start_products", payload))
        except Exception as exc:  # evidence, not a silent worker failure
            errors.append(repr(exc))

    threads = [threading.Thread(target=run) for _ in range(2)]
    for thread in threads:
        thread.start()
    barrier.wait()
    for thread in threads:
        thread.join()
    print(json.dumps({"results": results, "errors": errors}, ensure_ascii=False, indent=2))
    return 0 if len(results) == 2 and not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
