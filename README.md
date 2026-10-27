# Micro-Cache Store

![Python](https://img.shields.io/badge/python-3.8%2B-3776AB?logo=python&logoColor=white)
![Structure](https://img.shields.io/badge/structure-hash%20map%20%2B%20DLL-0F766E)
![Eviction](https://img.shields.io/badge/eviction-LRU-555555)
![Dependencies](https://img.shields.io/badge/dependencies-zero-success)
![License](https://img.shields.io/badge/license-MIT-blue)

An in-memory key-value cache with LRU eviction, per-key TTL, a background sweeper, and JSON snapshots. The standard library is the whole dependency list: `json`, `os`, `threading`, and `time`.

`capacity` is a count of stored keys. It is a slot budget, so a large string and a small integer cost one slot each.

## Architecture

Each entry lives in two places:

- A hash map from key to node, for average O(1) lookup.
- A doubly linked list with sentinel head and tail, for usage order.

The node just after the head is most recently used. The node just before the tail is least recently used. Insert, move-to-front, and unlink are pointer updates, so they stay O(1) after the hash lookup.

```text
head <-> [MRU] <-> ... <-> [LRU] <-> tail
```

`threading.RLock` wraps every public operation. The sweeper takes the same lock, so a `get` can run while the daemon is inside `purge` without two threads splicing the same node. The lock is re-entrant because snapshot load calls the unlocked setter while it already holds the lock.

## Complexity

| Operation | Time | Notes |
| --- | --- | --- |
| `get` hit | O(1) average | Hash lookup, then move to MRU |
| `get` miss / expired | O(1) average | Expired hits are unlinked immediately |
| `set` insert | O(1) average | Evicts the LRU node when the map is full |
| `set` update | O(1) average | Replaces value and TTL, then moves to MRU |
| `delete` | O(1) average | |
| `key in cache` | O(1) average | Drops an expired key and does not refresh LRU |
| `len(cache)` | O(1) | Counts stored nodes, including TTL that has elapsed but not yet been swept |
| sweep | O(n) | Walks the list once per interval |
| `save_to_disk` / `load_from_disk` | O(n) | One pass, plus eviction if the snapshot is larger than capacity |

## TTL

Pass `ttl` in seconds or `ttl_ms` in milliseconds. Pass one of them. A non-positive TTL raises `ValueError`. Omit both and the key lives until it is evicted, deleted, or cleared.

Expiry is checked in two places:

- **Lazy.** `get` and `in` drop a key whose deadline has passed and count an expiration.
- **Daemon.** A thread named `micro-cache-sweeper` walks the list every `sweep_interval` seconds (default `0.5`). Pass `sweep_interval=None` to disable it. `close()` stops the thread and is safe to call more than once.

Deadlines use `time.monotonic`. Tests pass `time_func` so they can advance a fake clock without sleeping. `len` can still include a key whose TTL has elapsed until the next read or sweep.

```python
from cache import MicroCache

cache = MicroCache(capacity=1000)
cache.set("session", {"user": "ada"}, ttl=30)
cache.set("burst", 1, ttl_ms=1500)
cache.get("session")          # refreshes LRU order
"session" in cache            # True, and does not refresh LRU
cache.close()
```

A stored `None` is a real value. `get` returns `None` for both a stored `None` and a miss when `default` is `None`. Use `key in cache` to tell them apart.

## Snapshots

`save_to_disk(path)` writes a version-1 JSON file and replaces the destination with `os.replace`, so a crash mid-write leaves the previous file in place. Expired keys are omitted. Each item stores `ttl_remaining` in seconds, measured at save time, so a later process does not need this process's monotonic clock.

Items are written least-recently-used first. `load_from_disk` validates the file before it clears the cache, then inserts in that order, so the last item becomes MRU again. The snapshot's `capacity` replaces the cache's capacity. Values must be JSON-serializable.

```python
cache.set("a", 1)
cache.set("b", 2, ttl=60)
cache.save_to_disk("snapshot.json")

restored = MicroCache(capacity=1)
restored.load_from_disk("snapshot.json")
```

## Threads

Writers and readers share one `RLock`. Capacity is checked and the LRU node is unlinked in the same critical section, so concurrent `set` calls cannot grow the map past `capacity`.

## Tests and benchmark

```bash
cd 7-micro-cache-store
python -m unittest test_cache.py -v
python benchmark.py
```

The tests cover LRU eviction, recency on update, lazy TTL, the daemon sweeper, a capacity ceiling under eight concurrent writers, snapshot order and remaining TTL, and rejected arguments.

`benchmark.py` runs 100,000 sets, 100,000 gets, and 100,000 mixed operations against 2,000 hot keys in a cache of 10,000 slots, with the sweeper off. One local run:

```text
MicroCache benchmark
capacity=10000  operations=100000  hot_keys=2000

set       2793117 ops/sec   (0.036s)
get       2944103 ops/sec   (0.034s)
mixed     2732569 ops/sec   (0.037s)

size=2000  hits=180000  misses=0  evictions=0
```

Those hot keys fit in capacity, so this run measures the hit path. Eviction is covered by the unit tests. Expect the ops/sec figure to move with the machine.
