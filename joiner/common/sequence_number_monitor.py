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
        self._lock = threading.Lock()
        self._state_manager = state_manager
        self._batches_seen = {}
        self.intizalize()

    def intizalize(self):
        with self._lock:
            backup = self._state_manager.load_saved_secuence_number_data()
            logging.info(f"Loaded Backup {backup}")
            self._batches_seen = {
            cid: set(seq_list) for cid, seq_list in backup.items()
            }
            logging.info(f"Loaded _batches_seen {self._batches_seen}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def is_duplicate(self, client_id: str, seq_num: str) -> bool:
        """Return ``True`` iff *seq_num* was already recorded for *client_id*."""
        with self._lock:
            logging.info(f"Checking _batches_seen from credits consumer: {self._batches_seen}")
            return str(seq_num) in self._batches_seen.get(client_id, set())
        
    def get_num_unique(self, client_id):
        with self._lock:
            return len(self._batches_seen.get(client_id, set()))

    def record(self, client_id: str, seq_num: str) -> None:
        """Persist *seq_num* for *client_id* (in-memory **and** on disk)."""
        with self._lock:
            self._batches_seen.setdefault(client_id, set()).add(str(seq_num))
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
                pass
            except Exception as exc:
                logging.error(
                    "[SeqMonitor] Error cleaning sequence number file for client %s: %s",
                    client_id,
                    exc,
                ) 
                raise