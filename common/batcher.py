from __future__ import annotations
from collections import defaultdict
import os
import threading
import time
from typing import Callable, Dict, List, Any

from common.state_persistence import StatePersistence

class PerClientBatcher:
    """Crash-safe batch accumulator reusable by any component (joiner, data-controller, …).

    Parameters
    ----------
    producer : RabbitMQ (or compatible) publisher instance
        Must expose a `.publish(bytes)` method.
    encode_fn : Callable[[List[Any], int], bytes]
        Function that turns a list of protobuf records *and* a numeric key into
        a serialized batch.
    max_items : int, optional
        Flush threshold. Defaults to 100 items.
    backup_dir : str, optional
        Directory for snapshot files. Defaults to "/backup" (same volume used
        throughout the project).
    serializer : str, optional
        "json" or "pickle".  Pickle is used by default because we store raw
        protobufs.
    namespace : str, optional
        Namespace for snapshot files. Defaults to "batch".
    """

    def __init__(
        self,
        producer,
        encode_fn: Callable[[List[Any], int], bytes],
        *,
        max_items: int = 100,
        backup_dir: str = "/backup",
        serializer: str = "pickle",
        namespace: str = "batch",
    ) -> None:
        self._producer = producer
        self._encode_fn = encode_fn
        self._max_items = max(max_items, 1)
        self._backup_dir = backup_dir
        self._serializer = serializer
        self._namespace = namespace

        self._buffers: Dict[str, List[Any]] = defaultdict(list)
        self._state_helpers: Dict[str, StatePersistence] = {}
        self._last_flush: Dict[str, float] = defaultdict(lambda: time.monotonic())
        self._flush_count: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

        self._restore_existing_batches()

    def add(self, item: Any, key: str) -> None:
        """Append *item* to the batch for *key* and persist immediately."""
        with self._lock:
            buf = self._buffers[key]
            buf.append(item)

            if len(buf) >= self._max_items:
                self._flush_locked(key)

    def flush_key(self, key: str) -> None:
        with self._lock:
            self._flush_locked(key)

    def flush_all(self) -> None:
        with self._lock:
            for k in list(self._buffers.keys()):
                self._flush_locked(k)

    def _flush_locked(self, key: str) -> None:
        buf = self._buffers.get(key)
        if not buf:
            return

        batch_bytes = self._encode_fn(buf, int(key))
        self._producer.publish(batch_bytes)  # may raise – caller must handle

        # Success: clear memory & on-disk snapshot
        buf.clear()
        self._get_state_helper(key).save([])
        self._last_flush[key] = time.monotonic()
        self._flush_count[key] += 1

    def _get_state_helper(self, key: str) -> StatePersistence:
        helper = self._state_helpers.get(key)
        if helper is None:
            os.makedirs(self._backup_dir, exist_ok=True)
            fname = f"{self._namespace}_{key}.pkl" if self._serializer == "pickle" else f"{self._namespace}_{key}.json"
            helper = StatePersistence(fname, directory=self._backup_dir, serializer=self._serializer)
            self._state_helpers[key] = helper
        return helper

    def _restore_existing_batches(self) -> None:
        if not os.path.isdir(self._backup_dir):
            return
        for fname in os.listdir(self._backup_dir):
            if not fname.startswith(f"{self._namespace}_"):
                continue
            key = fname[len(self._namespace)+1:].split(".")[0]
            helper = StatePersistence(fname, directory=self._backup_dir, serializer=self._serializer)
            items: List[Any] = helper.load(default_factory=list)
            if items:
                self._buffers[key].extend(items)
                self._state_helpers[key] = helper

    def clear(self, key: str) -> None:
        """Remove buffered data and truncate on-disk snapshot for *key*."""
        with self._lock:
            self._buffers.pop(key, None)
            helper = self._state_helpers.pop(key, None)
            if helper is None:
                # Helper may not exist if clear() is called before any data arrived.
                try:
                    helper = self._get_state_helper(key)
                except Exception:
                    helper = None
            if helper is not None:
                helper.clear()

    def snapshot_key(self, key: str) -> None:
        """Persist the current buffer for *key* exactly once.

        Call this at the end of a RabbitMQ delivery when the batch has NOT
        reached `max_items`.  It guarantees crash-replay safety with a single
        fsync per delivery
        """
        with self._lock:
            buf = self._buffers.get(key)
            if buf is None:
                return
            self._get_state_helper(key).save(buf)

    def snapshot_all(self) -> None:
        with self._lock:
            for key in self._buffers:
                self._get_state_helper(key).save(self._buffers[key])

    def flushes(self, key: str) -> int:
        """Return how many batches have been flushed for *key*."""
        return self._flush_count.get(key, 0)

    def pop_flushes(self, key: str) -> int:
        """Return and clear the flushed-batch counter for *key*."""
        return self._flush_count.pop(key, 0) 