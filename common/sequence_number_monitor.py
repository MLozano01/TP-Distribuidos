from __future__ import annotations

import threading
import logging
from typing import Dict, Set

from common.state_persistence import StatePersistence

class SequenceNumberMonitor:
    """Thread-safe helper that tracks sequence numbers per client (copied from
    *joiner.common.sequence_number_monitor* so that top-level `common.*` imports
    keep working after package restructuring)."""

    def __init__(self, state_manager: StatePersistence) -> None:
        self._state_manager = state_manager
        backup = state_manager.load_saved_secuence_number_data()
        self._batches_seen: Dict[str, Set[str]] = {cid: set(lst) for cid, lst in backup.items()}
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def is_duplicate(self, client_id: str, seq_num: str) -> bool:
        with self._lock:
            return seq_num in self._batches_seen.get(client_id, set())

    def record(self, client_id: str, seq_num: str) -> None:
        with self._lock:
            self._batches_seen.setdefault(client_id, set()).add(seq_num)
            try:
                self._state_manager.save_secuence_number_data(seq_num, client_id)
            except Exception as exc:
                logging.error("[SeqMonitor] Failed to persist sequence num %s for client %s: %s", seq_num, client_id, exc)
                raise

    def clear_client(self, client_id: str) -> None:
        with self._lock:
            self._batches_seen.pop(client_id, None)
            try:
                self._state_manager.clean_client(client_id)
            except Exception as exc:
                logging.error("[SeqMonitor] Error cleaning client %s: %s", client_id, exc)
                raise 