#!/usr/bin/env python3
"""Mini stress test. Prints operations per second for set, get, and a mix."""

from __future__ import annotations

import time

from cache import MicroCache


OPERATIONS = 100_000
CAPACITY = 10_000
HOT_KEYS = 2_000


def timed(label, count, action):
    started = time.perf_counter()
    action(count)
    elapsed = time.perf_counter() - started
    rate = count / elapsed if elapsed else float("inf")
    print("%-6s %10.0f ops/sec   (%.3fs)" % (label, rate, elapsed))
    return rate


def main():
    cache = MicroCache(CAPACITY, sweep_interval=None)
    try:
        print("MicroCache benchmark")
        print("capacity=%d  operations=%d  hot_keys=%d" % (CAPACITY, OPERATIONS, HOT_KEYS))
        print()

        def fill(count):
            for index in range(count):
                cache.set(index % HOT_KEYS, index)

        def read(count):
            for index in range(count):
                cache.get(index % HOT_KEYS)

        def mixed(count):
            for index in range(count):
                key = index % HOT_KEYS
                if index % 5 == 0:
                    cache.set(key, index)
                else:
                    cache.get(key)

        timed("set", OPERATIONS, fill)
        timed("get", OPERATIONS, read)
        timed("mixed", OPERATIONS, mixed)
        print()
        stats = cache.stats()
        print(
            "size=%d  hits=%d  misses=%d  evictions=%d"
            % (stats["size"], stats["hits"], stats["misses"], stats["evictions"])
        )
    finally:
        cache.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
