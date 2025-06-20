import threading
import logging
from typing import Dict, List, Any

class JoinerState:
    """Thread-safe container for all mutable state required by the Joiner node.

    The class keeps three internal data structures, all guarded by the same
    lock to guarantee atomicity across related structures:

    1. _movies_data:      {client_id: {movie_id: movie_pb}}
    2. _other_data:       {client_id: {movie_id: [other_pb, ...]}}
    3. _eof_trackers:     {client_id: {"movies": bool, "other": bool}}

    Every public mutating method MUST acquire the lock. Helper readers that
    return a simple value are *not* locked to avoid unnecessary contention –
    those methods should be treated as best-effort snapshots.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._movies_data: Dict[str, Dict[int, Any]] = {}
        self._other_data: Dict[str, Dict[int, List[Any]]] = {}
        self._eof_trackers: Dict[str, Dict[str, bool]] = {}
        logging.debug("JoinerState initialised")

    # ---------------------------------------------------------------------
    # Movie helpers
    # ---------------------------------------------------------------------
    def add_movie(self, client_id: str, movie_pb) -> List[Any]:
        """Add a movie to the buffer and return any previously buffered
        records from the *other* stream that match this movie.
        """
        with self._lock:
            self._movies_data.setdefault(client_id, {})[movie_pb.id] = movie_pb
            # Pop and return any buffered other data for the same movie.
            other_for_client = self._other_data.get(client_id, {})
            unmatched = other_for_client.pop(movie_pb.id, [])
            # House-keeping – remove empty nested dicts.
            if client_id in self._other_data and not self._other_data[client_id]:
                del self._other_data[client_id]
            return list(unmatched)  # Shallow copy, callers may mutate.

    def get_movie(self, client_id: str, movie_id: int):
        return self._movies_data.get(client_id, {}).get(movie_id)

    # ---------------------------------------------------------------------
    # Other-stream helpers
    # ---------------------------------------------------------------------
    def buffer_other(self, client_id: str, movie_id: int, other_pb) -> None:
        """Buffers an *other* record that arrived before its movie."""
        with self._lock:
            self._other_data.setdefault(client_id, {}).setdefault(movie_id, []).append(other_pb)

    def get_buffered_other(self, client_id: str, movie_id: int) -> List[Any]:
        """Return (but DO NOT remove) buffered *other* data for a movie."""
        return self._other_data.get(client_id, {}).get(movie_id, [])

    # ---------------------------------------------------------------------
    # EOF tracking helpers
    # ---------------------------------------------------------------------
    def set_stream_eof(self, client_id: str, stream: str) -> None:
        """Persist that *stream* ("movies" | "other") has sent EOF for client."""
        if stream not in ("movies", "other"):
            raise ValueError("stream must be 'movies' or 'other'")
        with self._lock:
            self._eof_trackers.setdefault(client_id, {"movies": False, "other": False})[stream] = True
            logging.debug(f"EOF for stream '{stream}' received ‑ client {client_id}")

    def has_eof(self, client_id: str, stream: str) -> bool:
        return self._eof_trackers.get(client_id, {}).get(stream, False)

    def has_both_eof(self, client_id: str) -> bool:
        tracker = self._eof_trackers.get(client_id)
        return bool(tracker and tracker["movies"] and tracker["other"])

    # ---------------------------------------------------------------------
    # Clean-up helpers
    # ---------------------------------------------------------------------
    def purge_orphan_other_after_movie_eof(self, client_id: str) -> None:
        """Discard buffered *other* data that can never be matched now that the
        movies stream has ended for *client_id*.
        """
        with self._lock:
            movies_ids = set(self._movies_data.get(client_id, {}).keys())
            other_for_client = self._other_data.get(client_id)
            if not other_for_client:
                return
            orphan_ids = [mid for mid in other_for_client.keys() if mid not in movies_ids]
            for mid in orphan_ids:
                del other_for_client[mid]
            if not other_for_client:
                del self._other_data[client_id]
            logging.debug(f"Purged {len(orphan_ids)} orphan other-records for client {client_id}")

    def remove_client_data(self, client_id: str) -> None:
        """Remove *all* cached data & trackers for *client_id* to free memory."""
        with self._lock:
            self._movies_data.pop(client_id, None)
            self._other_data.pop(client_id, None)
            self._eof_trackers.pop(client_id, None)
            logging.info(f"State fully cleared for client {client_id}") 