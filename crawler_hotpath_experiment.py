"""Independent hot-path helpers for crawler experiments.

Nothing in production imports this module.  It provides bounded asynchronous
batch writes, asynchronous logging, and an explicit pacing policy so each
optimization can be benchmarked without changing crawler semantics.
"""

from __future__ import annotations

import queue
import random
import threading
import time
from dataclasses import dataclass
from typing import Callable, Generic, Iterable, TypeVar


T = TypeVar("T")


class AsyncBatchWriter(Generic[T]):
    """Single-writer batching with backpressure and failure propagation."""

    _STOP = object()

    def __init__(
        self,
        write_batch: Callable[[list[T]], int],
        *,
        batch_size: int = 1000,
        flush_interval: float = 1.0,
        max_pending_batches: int = 32,
    ):
        if batch_size < 1 or flush_interval <= 0 or max_pending_batches < 1:
            raise ValueError("invalid async writer configuration")
        self._write_batch = write_batch
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._queue: queue.Queue = queue.Queue(max_pending_batches)
        self._lock = threading.Lock()
        self._error: BaseException | None = None
        self._closed = False
        self.submitted = 0
        self.written = 0
        self.batches = 0
        self._thread = threading.Thread(target=self._run, name="async-batch-writer", daemon=True)
        self._thread.start()

    def submit_many(self, items: Iterable[T]) -> int:
        batch = list(items)
        if not batch:
            return 0
        self._raise_if_failed()
        with self._lock:
            if self._closed:
                raise RuntimeError("async writer is closed")
            self.submitted += len(batch)
        self._queue.put(batch)
        self._raise_if_failed()
        return len(batch)

    def _run(self):
        pending: list[T] = []
        deadline = time.monotonic() + self._flush_interval
        try:
            while True:
                timeout = max(0.0, deadline - time.monotonic())
                try:
                    item = self._queue.get(timeout=timeout)
                except queue.Empty:
                    item = None
                if item is self._STOP:
                    self._queue.task_done()
                    if pending:
                        self._commit(pending)
                    return
                if item is not None:
                    pending.extend(item)
                    self._queue.task_done()
                now = time.monotonic()
                if pending and (len(pending) >= self._batch_size or now >= deadline):
                    self._commit(pending)
                    pending = []
                    deadline = now + self._flush_interval
                elif now >= deadline:
                    deadline = now + self._flush_interval
        except BaseException as exc:
            with self._lock:
                self._error = exc
            # Release queue.join callers even after a writer failure.
            while True:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                self._queue.task_done()
                if item is self._STOP:
                    break

    def _commit(self, batch: list[T]):
        count = self._write_batch(list(batch))
        with self._lock:
            self.written += int(count)
            self.batches += 1

    def _raise_if_failed(self):
        with self._lock:
            error = self._error
        if error is not None:
            raise RuntimeError("async writer failed") from error

    def flush(self):
        self._queue.join()
        # Queue empty does not guarantee the writer's local pending list was
        # committed, so close() is the hard durability gate.
        self._raise_if_failed()

    def close(self):
        with self._lock:
            already_closed = self._closed
            self._closed = True
        if already_closed:
            self._raise_if_failed()
            return
        self._queue.put(self._STOP)
        self._thread.join()
        self._raise_if_failed()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


class AsyncLogSink:
    """Non-blocking crawler log queue with a single output thread."""

    _STOP = object()

    def __init__(self, emit: Callable[[str], None], *, max_pending: int = 20000):
        self._emit = emit
        self._queue: queue.Queue = queue.Queue(max_pending)
        self._closed = False
        self.lines = 0
        self._thread = threading.Thread(target=self._run, name="async-log-sink", daemon=True)
        self._thread.start()

    def print(self, *values, sep=" ", end="\n", flush=False):
        if self._closed:
            raise RuntimeError("async log sink is closed")
        self._queue.put(sep.join(str(value) for value in values) + ("" if end == "\n" else end))

    def _run(self):
        while True:
            item = self._queue.get()
            try:
                if item is self._STOP:
                    return
                self._emit(item)
                self.lines += 1
            finally:
                self._queue.task_done()

    def close(self):
        if self._closed:
            return
        self._closed = True
        self._queue.put(self._STOP)
        self._thread.join()


@dataclass(frozen=True)
class PacingPolicy:
    """Explicit experiment policy; production jitter remains untouched."""

    mode: str = "baseline"
    low: float = 0.3
    high: float = 0.8

    def delay(self, *, recent_failure_rate: float = 0.0) -> float:
        if self.mode == "baseline":
            return random.uniform(self.low, self.high)
        if self.mode == "off":
            return 0.0
        if self.mode == "adaptive":
            # Keep jitter when the exit is showing pressure; healthy exits do
            # not pay a fixed tax. Thresholds are experimental, not production.
            if recent_failure_rate >= 0.01:
                return random.uniform(self.low, self.high)
            return 0.0
        raise ValueError(f"unknown pacing mode: {self.mode}")
