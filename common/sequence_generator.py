from __future__ import annotations

"""Crash-safe sequence-number generator (copied from joiner.common.sequence_generator).
This duplicate allows legacy `from common.sequence_generator import SequenceGenerator`
imports to remain functional after we turned *joiner* into a standalone package.
"""

from typing import Dict
from threading import Lock

from common.state_persistence import StatePersistence

class SequenceGenerator:
    """Generates incremental integers per client and persists them."""

    def __init__(
        self,
        replica_id: int,
        replicas_count: int,
        *,
        namespace: str | None = None,
        filename: str | None = None,
        backup_dir: str = "/backup",
    ) -> None:

        if not 1 <= replica_id <= replicas_count:
            raise ValueError("replica_id must be in range [1, replicas_count]")

        self._rid = replica_id - 1  # zero-based for arithmetic progression
        self._R = replicas_count

        if filename is None:
            ns = namespace or "joiner"
            filename = f"{ns}_seq_{replica_id}.json"

        self._state = StatePersistence(filename, directory=backup_dir, serializer="json")
        self._lock = Lock()
        raw: Dict[str, int] = self._state.load(default_factory=dict)
        self._counters: Dict[str, int] = {str(k): int(v) for k, v in raw.items()}

    # ------------------------------------------------------------------
    def next(self, client_id: str) -> int:
        """Return current sequence number for *client_id* and increment."""
        with self._lock:
            cid = str(client_id)
            seq = self._counters.get(cid, 0)
            self._counters[cid] = seq + 1
            self._state.save(self._counters)
            return self._rid + self._R * seq

    def current(self, client_id: str) -> int:
        """Return *global* sequence to be assigned next (without increment)."""
        return self._rid + self._R * self._counters.get(str(client_id), 0)

    def clear(self, client_id: str) -> None:
        """Reset counter for *client_id* (after finished)."""
        with self._lock:
            self._counters.pop(str(client_id), None)

            if self._counters:
                self._state.save(self._counters)
            else:
                self._state.clear() 