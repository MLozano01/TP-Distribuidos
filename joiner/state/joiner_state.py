import threading
import logging
from typing import Any, Dict, Set, Optional

from common.state_persistence import StatePersistence

class JoinerState:
    """Thread-safe state container for the *Joiner* node.

    If a ``StatePersistence`` instance is provided the state is automatically
    saved *atomically* after each mutating operation and restored on start-up.
    """

    def __init__(self, persistence: Optional[StatePersistence] = None):
        # Public attributes need to be pickle/JSON serialisable if they are to
        # be persisted.
        self.movies_buffer: Dict[str, Dict[int, Any]] = {}
        self.unmatched_other_buffer: Dict[str, Dict[int, Any]] = {}
        self.movies_eof_received: Set[str] = set()

        self._lock = threading.Lock()
        self._persistence = persistence

        # Attempt to restore previous state if a persistence manager is given.
        if self._persistence is not None:
            persisted = self._persistence.load(default_factory=dict)
            if persisted:
                self.movies_buffer = persisted.get("movies_buffer", {})
                self.unmatched_other_buffer = persisted.get("unmatched_other_buffer", {})
                # Stored as list for JSON compatibility when using pickle we
                # can load a set directly – but handling consistently is fine.
                self.movies_eof_received = set(persisted.get("movies_eof_received", []))

        logging.info("JoinerState initialized (restored=%s).", bool(self._persistence))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _persist(self):
        if self._persistence is None:
            return
        snapshot = {
            "movies_buffer": self.movies_buffer,
            "unmatched_other_buffer": self.unmatched_other_buffer,
            "movies_eof_received": list(self.movies_eof_received),
        }
        self._persistence.save(snapshot)

    def add_movie_and_process_unmatched(self, client_id, movie):
        with self._lock:
            if client_id not in self.movies_buffer:
                self.movies_buffer[client_id] = {}
            self.movies_buffer[client_id][movie.id] = movie

            unmatched_data = []
            if client_id in self.unmatched_other_buffer:
                if movie.id in self.unmatched_other_buffer[client_id]:
                    unmatched_data = self.unmatched_other_buffer[client_id].pop(movie.id)
                    if not self.unmatched_other_buffer[client_id]:
                        del self.unmatched_other_buffer[client_id]
            # Persist after mutation
            self._persist()
        return unmatched_data

    def get_movie(self, client_id, movie_id):
        with self._lock:
            if client_id in self.movies_buffer:
                return self.movies_buffer[client_id].get(movie_id)
            return None

    def add_unmatched_other(self, client_id, movie_id, data):
        with self._lock:
            if client_id not in self.unmatched_other_buffer:
                self.unmatched_other_buffer[client_id] = {}
            
            if movie_id not in self.unmatched_other_buffer[client_id]:
                self.unmatched_other_buffer[client_id][movie_id] = []
            
            self.unmatched_other_buffer[client_id][movie_id].append(data)
            # Persist after mutation
            self._persist()

    def set_movies_eof(self, client_id):
        with self._lock:
            self.movies_eof_received.add(client_id)
            logging.info(f"Movies EOF for client {client_id} received. Cleaning up unmatched ratings.")
            
            if client_id in self.unmatched_other_buffer:
                movies_for_client = self.movies_buffer.get(client_id, {})
                unmatched_ratings_for_client = self.unmatched_other_buffer[client_id]
                
                movie_ids_to_purge = [
                    movie_id for movie_id in unmatched_ratings_for_client
                    if movie_id not in movies_for_client
                ]
                
                if movie_ids_to_purge:
                    logging.info(f"Purging {len(movie_ids_to_purge)} unmatched ratings for client {client_id} that have no matching movie.")
                    for movie_id in movie_ids_to_purge:
                        del unmatched_ratings_for_client[movie_id]

                if not unmatched_ratings_for_client:
                    del self.unmatched_other_buffer[client_id]

            # Persist after mutation
            self._persist()

    def has_movies_eof(self, client_id):
        return client_id in self.movies_eof_received

    def clear_client_state(self, client_id):
        """
        Clears all state associated with a client ID.
        This could be called, for example, after the "other" stream's EOF is received.
        """
        with self._lock:
            self.movies_buffer.pop(client_id, None)
            self.unmatched_other_buffer.pop(client_id, None)
            self.movies_eof_received.discard(client_id)
            logging.info(f"Cleared all state for client {client_id}") 
            # Persist after mutation
            self._persist() 