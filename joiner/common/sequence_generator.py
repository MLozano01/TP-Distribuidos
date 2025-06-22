from __future__ import annotations

"""Light-weight, crash-safe sequence-number generator used by Joiner replicas.

The class keeps *one* atomic counter per client and persists the dictionary
as JSON through the existing `StatePersistence` helper so that numbers remain
monotonic across restarts.
"""

from typing import Dict
from threading import Lock

from common.state_persistence import StatePersistence


# ---------------------------------------------------------------------------
# Public helper
# ---------------------------------------------------------------------------
# The generator combines a *local* counter with the replica-stride formula so
# that parallel joiner replicas never emit the same sequence-number:
#
#     SN = replica_id  +  replicas_count × counter
#
# With replica_id ∈ [0, replicas_count-1].  Each replica therefore produces a
# distinct arithmetic progression (0,2,4 … / 1,3,5 …) and the union is a
# gap-free global sequence seen by the reducer.
# ---------------------------------------------------------------------------


class SequenceGenerator:
    """Generates incremental integers per *client_id* and persists them.
    """

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

        self._rid = int(replica_id - 1)  # convert to 0-based for formula
        self._R = int(replicas_count)

        if filename is None:
            ns = namespace or "joiner"
            filename = f"{ns}_seq_{replica_id}.json"

        self._state = StatePersistence(filename, directory=backup_dir, serializer="json")
        self._lock = Lock()
        raw: Dict[str, int] = self._state.load(default_factory=dict)
        # Ensure all keys are str and values int
        self._counters: Dict[str, int] = {str(k): int(v) for k, v in raw.items()}

    # ------------------------------------------------------------------
    def next(self, client_id: str) -> int:
        """Return the current sequence number for *client_id* and increment."""
        with self._lock:
            cid = str(client_id)
            seq = self._counters.get(cid, 0)
            self._counters[cid] = seq + 1
            self._state.save(self._counters)
            return self._rid + self._R * seq

    def current(self, client_id: str) -> int:
        """Return *global* sequence that will be assigned next (without increment)."""
        local = self._counters.get(str(client_id), 0)
        return self._rid + self._R * local

    def clear(self, client_id: str) -> None:
        """Reset counter for *client_id* (e.g. after EOF fully processed)."""
        with self._lock:
            cid = str(client_id)
            if cid in self._counters:
                del self._counters[cid]
                self._state.save(self._counters)

    # Convenience -----------------------------------------------------
    def local_current(self, client_id: str) -> int:
        """Return local counter value (mainly for tests)."""
        return self._counters.get(str(client_id), 0) 