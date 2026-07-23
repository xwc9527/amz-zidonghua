import threading
import time
import unittest

from crawler_hotpath_experiment import AsyncBatchWriter, AsyncLogSink, PacingPolicy


class TestAsyncBatchWriter(unittest.TestCase):
    def test_batches_and_closes_durably(self):
        stored = []
        with AsyncBatchWriter(lambda batch: stored.extend(batch) or len(batch), batch_size=3) as writer:
            self.assertEqual(writer.submit_many([1, 2]), 2)
            self.assertEqual(writer.submit_many([3, 4]), 2)
        self.assertEqual(stored, [1, 2, 3, 4])
        self.assertEqual(writer.written, 4)

    def test_writer_failure_reaches_close(self):
        def fail(_batch):
            raise OSError("disk full")

        writer = AsyncBatchWriter(fail, batch_size=1)
        writer.submit_many([1])
        time.sleep(0.05)
        with self.assertRaisesRegex(RuntimeError, "failed"):
            writer.close()


class TestAsyncLogSink(unittest.TestCase):
    def test_all_lines_drain_before_close(self):
        lines = []
        sink = AsyncLogSink(lines.append)
        for index in range(20):
            sink.print("line", index)
        sink.close()
        self.assertEqual(len(lines), 20)
        self.assertEqual(sink.lines, 20)


class TestPacingPolicy(unittest.TestCase):
    def test_modes_are_explicit(self):
        self.assertEqual(PacingPolicy("off").delay(), 0.0)
        self.assertEqual(PacingPolicy("adaptive").delay(recent_failure_rate=0), 0.0)
        self.assertGreaterEqual(PacingPolicy("baseline").delay(), 0.3)
        self.assertGreaterEqual(PacingPolicy("adaptive").delay(recent_failure_rate=0.1), 0.3)


if __name__ == "__main__":
    unittest.main()
