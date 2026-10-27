"""LRU, TTL, persistence, and thread-safety scenarios for MicroCache."""

from __future__ import annotations

import os
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cache import MicroCache


class Clock(object):
    def __init__(self):
        self.now = 1_000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class MicroCacheTests(unittest.TestCase):
    def setUp(self):
        self.caches = []

    def tearDown(self):
        for cache in self.caches:
            cache.close()

    def make(self, capacity, clock=None, sweep_interval=None):
        cache = MicroCache(capacity, time_func=clock, sweep_interval=sweep_interval)
        self.caches.append(cache)
        return cache

    def test_lru_evicts_least_recently_used(self):
        cache = self.make(2)
        cache.set("a", 1)
        cache.set("b", 2)
        self.assertEqual(cache.get("a"), 1)

        cache.set("c", 3)

        self.assertIsNone(cache.get("b"))
        self.assertEqual(cache.get("a"), 1)
        self.assertEqual(cache.get("c"), 3)
        self.assertEqual(len(cache), 2)
        self.assertEqual(cache.stats()["evictions"], 1)

    def test_update_refreshes_recency_without_growing(self):
        cache = self.make(2)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("a", 10)
        cache.set("c", 3)

        self.assertEqual(cache.get("a"), 10)
        self.assertIsNone(cache.get("b"))
        self.assertEqual(cache.get("c"), 3)
        self.assertEqual(len(cache), 2)

    def test_ttl_expires_on_read(self):
        clock = Clock()
        cache = self.make(4, clock=clock)
        cache.set("session", "ok", ttl=5)
        cache.set("fast", "ms", ttl_ms=1500)

        clock.advance(1.4)
        self.assertEqual(cache.get("session"), "ok")
        self.assertEqual(cache.get("fast"), "ms")

        clock.advance(0.2)
        self.assertIsNone(cache.get("fast"))
        self.assertNotIn("fast", cache)
        self.assertEqual(cache.get("session"), "ok")

        clock.advance(5)
        self.assertIsNone(cache.get("session"))
        self.assertEqual(cache.stats()["expirations"], 2)
        self.assertEqual(len(cache), 0)

    def test_daemon_sweeper_removes_expired_keys(self):
        clock = Clock()
        cache = self.make(4, clock=clock, sweep_interval=0.05)
        cache.set("temp", 1, ttl=1)
        self.assertEqual(len(cache), 1)

        clock.advance(2)
        deadline = time.time() + 2
        while time.time() < deadline and len(cache) != 0:
            time.sleep(0.05)

        self.assertEqual(len(cache), 0)
        self.assertGreaterEqual(cache.stats()["expirations"], 1)
        self.assertIsNone(cache.get("temp"))

    def test_concurrent_writers_never_exceed_capacity(self):
        capacity = 20
        cache = self.make(capacity)
        errors = []
        observed = []

        def worker(worker_id):
            try:
                for step in range(400):
                    key = "%d-%d" % (worker_id, step % 15)
                    cache.set(key, step)
                    cache.get(key)
                    if step % 20 == 0:
                        observed.append(len(cache))
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(index,)) for index in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertTrue(observed)
        self.assertTrue(all(0 <= size <= capacity for size in observed))
        self.assertLessEqual(len(cache), capacity)
        self.assertGreater(cache.stats()["evictions"], 0)

    def test_snapshot_restores_values_ttl_and_lru_order(self):
        clock = Clock()
        expired = self.make(2, clock=clock)
        expired.set("gone", 1, ttl=1)
        expired.set("stay", 2)
        clock.advance(1)

        cache = self.make(3, clock=clock)
        cache.set("a", {"n": 1})
        cache.set("b", [2, 3], ttl=10)
        cache.set("c", "keep")

        with tempfile.TemporaryDirectory() as directory:
            expired_path = os.path.join(directory, "expired.json")
            path = os.path.join(directory, "snap.json")
            expired.save_to_disk(expired_path)
            cache.save_to_disk(path)

            skipped = self.make(2, clock=clock)
            skipped.load_from_disk(expired_path)
            restored = self.make(1, clock=clock)
            restored.load_from_disk(path)

        self.assertNotIn("gone", skipped)
        self.assertEqual(skipped.get("stay"), 2)

        self.assertEqual(restored.capacity, 3)
        self.assertIn("a", restored)
        self.assertIn("b", restored)
        self.assertIn("c", restored)
        restored.set("d", 4)
        self.assertNotIn("a", restored)
        self.assertEqual(restored.get("b"), [2, 3])
        self.assertEqual(restored.get("c"), "keep")

        clock.advance(9)
        self.assertEqual(restored.get("b"), [2, 3])
        clock.advance(1.1)
        self.assertIsNone(restored.get("b"))
        self.assertEqual(restored.get("c"), "keep")

    def test_invalid_arguments_are_rejected(self):
        with self.assertRaises(ValueError):
            MicroCache(0)
        with self.assertRaises(ValueError):
            MicroCache(-3)
        cache = self.make(1)
        with self.assertRaises(ValueError):
            cache.set("a", 1, ttl=0)
        with self.assertRaises(ValueError):
            cache.set("a", 1, ttl=-1)
        with self.assertRaises(ValueError):
            cache.set("a", 1, ttl=1, ttl_ms=1)
        cache.set("a", None)
        self.assertIn("a", cache)
        self.assertIsNone(cache.get("a"))


if __name__ == "__main__":
    unittest.main()
