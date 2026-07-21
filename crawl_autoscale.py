"""抓取侧 worker 随活池规模动态扩容。"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from queue import Empty, Queue
from typing import Any, Callable

from config import PROXY_MAX_CRAWL_WORKERS, PROXY_WORKER_SCALE_INTERVAL_SEC

log = logging.getLogger("crawl_autoscale")

WorkerFn = Callable[..., None]


def run_autoscaled_queue(
    worker_fn: WorkerFn,
    task_q: Queue,
    pool: Any,
    *,
    initial_workers: int,
    max_workers: int | None = None,
    scale_interval: float | None = None,
    worker_args: tuple = (),
    log_prefix: str = "autoscale",
) -> None:
    """用共享队列跑可扩容的 worker 池。

    - 启动时按 initial_workers（通常=当前可用代理数）起 worker
    - 期间若 pool.usable_count 增长且队列仍有任务，动态追加 worker
    - worker 在 Empty 超时后退出；扩容检查间隔内新 worker 仍可领到剩余任务
    """
    cap = max(1, int(max_workers or PROXY_MAX_CRAWL_WORKERS))
    start_n = max(1, min(int(initial_workers or 1), cap))
    interval = float(scale_interval if scale_interval is not None else PROXY_WORKER_SCALE_INTERVAL_SEC)

    futs: list[Future] = []
    next_id = 0
    lock = threading.Lock()

    def _spawn(executor: ThreadPoolExecutor) -> Future:
        nonlocal next_id
        with lock:
            wid = next_id
            next_id += 1
        log.info("[%s] spawn worker=%d (active_target≈%d)", log_prefix, wid, next_id)
        return executor.submit(worker_fn, wid, task_q, pool, *worker_args)

    with ThreadPoolExecutor(max_workers=cap) as exe:
        for _ in range(start_n):
            futs.append(_spawn(exe))

        while True:
            alive = [f for f in futs if not f.done()]
            if not alive:
                # 全部 worker 已退出：无论是队列耗尽的正常收尾，还是有 worker
                # 抛出异常（例如代理池跌破最低阈值），都不能继续空转等待一个
                # 可能永远不会到来的扩容信号——立刻跳出，交给下面的 result()
                # 统一收尾/冒泡异常，保住"跌破阈值就安全暂停"的语义。
                break
            queued = task_q.qsize()
            # 扩容：可用代理数 > 当前存活 worker，且队列还有活
            try:
                usable = int(getattr(pool, "usable_count", 0) or 0)
            except Exception:
                usable = 0
            with lock:
                spawned = next_id
            if queued > 0 and usable > len(alive) and spawned < cap:
                # 一次最多补到 min(usable, cap)
                to_add = min(usable, cap) - spawned
                for _ in range(max(0, to_add)):
                    if next_id >= cap:
                        break
                    futs.append(_spawn(exe))
            # 短暂等待；worker Empty 超时通常 3s，间隔略长以便它们有机会退出/领任务
            time.sleep(max(1.0, interval))

        # 收尾：确保异常冒泡
        for f in futs:
            f.result()
