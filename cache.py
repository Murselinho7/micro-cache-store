#!/usr/bin/env python3
"""In-memory key-value cache with LRU eviction, TTL, and JSON snapshots.

A hash map gives average O(1) lookup. A doubly linked list keeps usage order
so moving a key to most-recently-used, and dropping the least-recently-used
key, are both O(1). Expired keys are removed on access and by a daemon thread.

The stored value count is what `capacity` limits. It is not a byte budget.
"""

from __future__ import annotations

import json
import os
import threading
import time


class _Node(object):
    __slots__ = ("key", "value", "expires_at", "prev", "next")

    def __init__(self, key=None, value=None, expires_at=None):
        self.key = key
        self.value = value
        self.expires_at = expires_at
        self.prev = None
        self.next = None


class MicroCache(object):
    """Thread-safe LRU cache.

    ``capacity`` is the maximum number of stored keys.
    ``time_func`` defaults to ``time.monotonic``. Tests inject a fake clock.
    ``sweep_interval`` is the daemon period in seconds. ``None`` disables it.
    """

    def __init__(self, capacity, time_func=None, sweep_interval=0.5):
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("capacity must be a positive integer")
        if sweep_interval is not None and sweep_interval <= 0:
            raise ValueError("sweep_interval must be positive or None")
        self.capacity = capacity
        self._now = time_func or time.monotonic
        self._sweep_interval = sweep_interval
        self._data = {}
        self._lock = threading.RLock()
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._expirations = 0
        self._head = _Node()
        self._tail = _Node()
        self._head.next = self._tail
        self._tail.prev = self._head
        self._stop = threading.Event()
        self._thread = None
        if sweep_interval is not None:
            self._thread = threading.Thread(
                target=self._sweep_loop,
                name="micro-cache-sweeper",
                daemon=True,
            )
            self._thread.start()

    def set(self, key, value, ttl=None, ttl_ms=None):
        """Store ``value`` under ``key``.

        ``ttl`` is seconds. ``ttl_ms`` is milliseconds. Pass at most one.
        Updating an existing key refreshes its position and replaces its TTL.
        """
        expires_at = self._expiry_deadline(ttl, ttl_ms)
        with self._lock:
            self._set_unlocked(key, value, expires_at)

    def get(self, key, default=None):
        """Return the value, or ``default`` if the key is missing or expired.

        A hit moves the key to most-recently-used. A stored ``None`` is
        indistinguishable from a miss when ``default`` is ``None``; use
        ``key in cache`` to tell them apart. Membership does not refresh LRU.
        """
        with self._lock:
            node = self._data.get(key)
            if node is None:
                self._misses += 1
                return default
            if self._expired(node):
                self._drop(node, expired=True)
                self._misses += 1
                return default
            self._touch(node)
            self._hits += 1
            return node.value

    def delete(self, key):
        """Remove ``key``. Return True when it was stored."""
        with self._lock:
            node = self._data.get(key)
            if node is None:
                return False
            self._drop(node, expired=False)
            return True

    def clear(self):
        with self._lock:
            self._clear_unlocked()

    def __contains__(self, key):
        with self._lock:
            node = self._data.get(key)
            if node is None:
                return False
            if self._expired(node):
                self._drop(node, expired=True)
                return False
            return True

    def __len__(self):
        """Stored entries, including ones whose TTL has elapsed but not yet been swept."""
        with self._lock:
            return len(self._data)

    def stats(self):
        with self._lock:
            return {
                "capacity": self.capacity,
                "size": len(self._data),
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "expirations": self._expirations,
            }

    def save_to_disk(self, filepath):
        """Write a JSON snapshot. Expired keys are omitted.

        Each item records the TTL remaining at save time, so a later load does
        not depend on the monotonic clock of this process. Items are ordered
        least-recently-used first.
        """
        with self._lock:
            now = self._now()
            items = []
            node = self._tail.prev
            while node is not self._head:
                if not self._expired(node, now):
                    remaining = None
                    if node.expires_at is not None:
                        remaining = node.expires_at - now
                    items.append(
                        {
                            "key": node.key,
                            "value": node.value,
                            "ttl_remaining": remaining,
                        }
                    )
                node = node.prev
            payload = {
                "version": 1,
                "capacity": self.capacity,
                "items": items,
            }
        self._write_json(filepath, payload)

    def load_from_disk(self, filepath):
        """Replace the cache contents with a snapshot from ``save_to_disk``.

        The snapshot's capacity becomes this cache's capacity. Items are
        inserted least-recently-used first, so the previous order is restored.
        A snapshot larger than the saved capacity still obeys eviction.
        """
        with open(filepath, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict) or payload.get("version") != 1:
            raise ValueError("unsupported snapshot version")
        items = payload.get("items")
        if not isinstance(items, list):
            raise ValueError("snapshot items must be a list")
        capacity = payload.get("capacity", self.capacity)
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError("snapshot capacity must be a positive integer")
        prepared = []
        for item in items:
            if not isinstance(item, dict) or "key" not in item or "value" not in item:
                raise ValueError("snapshot item must include key and value")
            remaining = item.get("ttl_remaining")
            if remaining is not None:
                remaining = float(remaining)
                if remaining <= 0:
                    continue
            prepared.append((item["key"], item["value"], remaining))
        with self._lock:
            self._clear_unlocked()
            self.capacity = capacity
            now = self._now()
            for key, value, remaining in prepared:
                expires_at = None if remaining is None else now + remaining
                self._set_unlocked(key, value, expires_at)

    def close(self):
        """Stop the sweeper thread. Safe to call more than once."""
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2)
        self._thread = None

    def _expiry_deadline(self, ttl, ttl_ms):
        if ttl is not None and ttl_ms is not None:
            raise ValueError("pass ttl or ttl_ms, not both")
        if ttl_ms is not None:
            ttl = float(ttl_ms) / 1000.0
        if ttl is None:
            return None
        ttl = float(ttl)
        if ttl <= 0:
            raise ValueError("ttl must be positive")
        return self._now() + ttl

    def _expired(self, node, now=None):
        if node.expires_at is None:
            return False
        if now is None:
            now = self._now()
        return now >= node.expires_at

    def _set_unlocked(self, key, value, expires_at):
        node = self._data.get(key)
        if node is not None:
            node.value = value
            node.expires_at = expires_at
            self._touch(node)
            return
        if len(self._data) >= self.capacity:
            self._pop_lru()
        node = _Node(key, value, expires_at)
        self._data[key] = node
        self._attach_mru(node)

    def _pop_lru(self):
        node = self._tail.prev
        if node is self._head:
            return None
        self._drop(node, expired=False)
        self._evictions += 1
        return node

    def _drop(self, node, expired):
        self._detach(node)
        self._data.pop(node.key, None)
        if expired:
            self._expirations += 1

    def _touch(self, node):
        self._detach(node)
        self._attach_mru(node)

    def _detach(self, node):
        node.prev.next = node.next
        node.next.prev = node.prev
        node.prev = None
        node.next = None

    def _attach_mru(self, node):
        first = self._head.next
        node.prev = self._head
        node.next = first
        first.prev = node
        self._head.next = node

    def _clear_unlocked(self):
        self._data.clear()
        self._head.next = self._tail
        self._tail.prev = self._head

    def _purge_expired_unlocked(self):
        now = self._now()
        node = self._head.next
        while node is not self._tail:
            nxt = node.next
            if self._expired(node, now):
                self._drop(node, expired=True)
            node = nxt

    def _sweep_loop(self):
        while not self._stop.wait(self._sweep_interval):
            try:
                with self._lock:
                    self._purge_expired_unlocked()
            except Exception:
                continue

    @staticmethod
    def _write_json(filepath, payload):
        directory = os.path.dirname(os.path.abspath(filepath))
        if directory:
            os.makedirs(directory, exist_ok=True)
        temporary = filepath + ".tmp"
        try:
            with open(temporary, "w", encoding="utf-8", newline="\n") as handle:
                json.dump(payload, handle, indent=2, allow_nan=False, ensure_ascii=False)
                handle.write("\n")
            os.replace(temporary, filepath)
        except Exception:
            if os.path.exists(temporary):
                try:
                    os.remove(temporary)
                except OSError:
                    pass
            raise
