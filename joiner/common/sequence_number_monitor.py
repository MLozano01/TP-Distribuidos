from __future__ import annotations

import threading
import logging
from typing import Dict, Set

from common.state_persistence import StatePersistence

class SequenceNumberMonitor:
    """Thread-safe helper that tracks *other-stream* sequence numbers per client.

    It encapsulates:
      • in-memory deduplication via a per-client *set* of seen numbers,
      • durable persistence to disk (using the provided ``StatePersistence``), and
      • clean-up after a client finishes.
    """

    def __init__(self, state_manager: StatePersistence) -> None:
        self._state_manager = state_manager
        backup = state_manager.load_saved_secuence_number_data()
        # Use sets for O(1) membership checks.
        self._batches_seen: Dict[str, Set[str]] = {
            cid: set(seq_list) for cid, seq_list in backup.items()
        }
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def is_duplicate(self, client_id: str, seq_num: str) -> bool:
        """Return ``True`` iff *seq_num* was already recorded for *client_id*."""
        with self._lock:
            return seq_num in self._batches_seen.get(client_id, set())

    def record(self, client_id: str, seq_num: str) -> None:
        """Persist *seq_num* for *client_id* (in-memory **and** on disk)."""
        with self._lock:
            self._batches_seen.setdefault(client_id, set()).add(seq_num)
            try:
                self._state_manager.save_secuence_number_data(seq_num, client_id)
            except Exception as exc:
                logging.error(
                    "[SeqMonitor] Failed to persist sequence number %s for client %s: %s",
                    seq_num,
                    client_id,
                    exc,
                )
                raise

    def clear_client(self, client_id: str) -> None:
        """Remove all stored information for *client_id* (memory + disk)."""
        with self._lock:
            self._batches_seen.pop(client_id, None)
            try:
                self._state_manager.clean_client(client_id)
            except Exception as exc:
                logging.error(
                    "[SeqMonitor] Error cleaning sequence number file for client %s: %s",
                    client_id,
                    exc,
                ) 
                raise