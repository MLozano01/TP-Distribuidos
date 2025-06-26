import threading
import logging
import os
from typing import Dict, List, Any, Tuple

from common.state_persistence import StatePersistence

class JoinerState:
    """Thread-safe container for all mutable state required by the Joiner node.

    The class keeps three internal data structures, all guarded by the same
    lock to guarantee atomicity across related structures:

    1. _movies_data:      {client_id: {movie_id: str}}   # maps id -> title
    2. _other_data:       {client_id: {movie_id: [other_record, ...]}}
    3. _eof_trackers:     {client_id: {"movies": bool, "other": bool}}

    Every public mutating method MUST acquire the lock. Helper readers that
    **do not mutate state** purposefully skip the lock to minimise contention,
    therefore they return a *best-effort snapshot* of the underlying data.

    IMPORTANT:
        • The returned objects MUST be treated as **immutable** by the caller.
        • Never mutate or store direct references for later mutation. Doing so
          would introduce data-races because another thread could update the
          internal dictionaries concurrently.

    If a caller needs to iterate over / transform the data, they should copy
    it first (e.g. ``list(... )`` or ``dict(... )``) while still holding no
    locks in this class.
    """

    def __init__(
        self,
        backup_file: str,
        node_tag: str,
        base_dir: str,
    ) -> None:
        """Create a new *in-memory* state container and – if available –
        restore any snapshots found on disk.
        """
        self._base_dir = base_dir
        self._node_tag = str(node_tag)
        self._state_manager = StatePersistence(
            backup_file,
            node_info=node_tag,
            serializer="json",
        )

        # Lazy registry of per-client ``StatePersistence`` helpers.
        self._client_state_managers: Dict[str, StatePersistence] = {}

        persisted_raw = (
            self._restore_from_disk() if self._state_manager is not None else {}
        )

        (
            self._movies_data,
            self._other_data,
            self._eof_trackers,
            others_proc,
            movies_proc,
        ) = self._normalise_snapshots(persisted_raw)

        self._processed_counts: dict[str, int] = others_proc          # OTHER stream
        self._pc_managers: dict[str, StatePersistence] = {}

        self._movies_processed: dict[str, int] = movies_proc          # MOVIES stream
        self._mp_managers: dict[str, StatePersistence] = {}

        for cid in set(self._movies_data) | set(self._other_data) | set(self._eof_trackers):
            mgr = StatePersistence(
                f"processed_{self._node_tag}_client_{cid}.txt",
                directory=self._base_dir,
                serializer=StatePersistence._JSON,
            )
            self._pc_managers[cid] = mgr
            try:
                self._processed_counts[cid] = int(mgr.load(lambda: 0))
            except Exception:
                self._processed_counts[cid] = 0

        restored_movies = sum(len(m) for m in self._movies_data.values())
        logging.info(
            "[JoinerState] Restored snapshot – movies=%s clients=%s",
            restored_movies,
            len(self._movies_data),
        )

        # Synchronisation primitive MUST be created *after* loading to avoid
        # using it inside helpers inadvertently.
        self._lock = threading.Lock()
        logging.debug("JoinerState initialised (persistent=%s)", bool(self._state_manager))

    def _restore_from_disk(self) -> Dict[str, Dict]:
        """Scan the backup directory and merge per-client snapshots.
        """

        movies: Dict[str, Dict[int, Any]] = {}
        other: Dict[str, Dict[int, List[Any]]] = {}
        eofs: Dict[str, Dict[str, bool]] = {}
        processed: Dict[str, int] = {}

        base_dir = self._base_dir
        base_file = getattr(self._state_manager, "_file_name", "joiner_state.json")
        base_name = base_file.rsplit(".", 1)[0]

        try:
            for fname in os.listdir(base_dir):
                if not (fname.startswith(f"{base_name}_") and fname.endswith(".json")):
                    continue
                client_id = fname[len(base_name) + 1 : -5]  # strip prefix/suffix
                mgr = StatePersistence(fname, directory=base_dir, serializer="json")
                self._client_state_managers[client_id] = mgr

                try:
                    snap = mgr.load(default_factory=dict)
                except Exception as exc:
                    logging.error("[JoinerState] Failed loading snapshot for client %s: %s", client_id, exc)
                    continue

                movies.update(snap.get("movies_data", {}))
                other.update(snap.get("other_data", {}))
                eofs.update(snap.get("eof_trackers", {}))
                processed.update({str(k): int(v) for k, v in snap.get("processed_counts", {}).items()})
        except FileNotFoundError:
            # Backup directory missing – no prior state, not an error.
            pass
        except Exception as exc:
            logging.error("[JoinerState] Error scanning backup directory: %s", exc)

        return {
            "movies_data": movies,
            "other_data": other,
            "eof_trackers": eofs,
            "processed_counts": processed,
        }

    def _normalise_snapshots(
        self, persisted: Dict[str, Dict]
    ) -> Tuple[
        Dict[str, Dict[int, str]],
        Dict[str, Dict[int, List[Any]]],
        Dict[str, Dict[str, bool]],
        Dict[str, int],
        Dict[str, int],
    ]:
        """Convert raw JSON dictionaries (str keys) into strongly-typed maps.
        This helper ensures **all** keys are of the expected type so that the
        rest of the codebase can rely on `int` for movie IDs and `str` for
        client IDs.
        """

        # --- Movies -----------------------------------------------------
        movies_typed: Dict[str, Dict[int, str]] = {}
        raw_movies = persisted.get("movies_data", {})
        for raw_cid, movies in raw_movies.items():
            cid = str(raw_cid)
            typed_map: Dict[int, str] = {}
            for mid_str, title in movies.items():
                try:
                    typed_map[int(mid_str)] = str(title)
                except (ValueError, TypeError):
                    # Skip malformed id/title pairs but keep going.
                    logging.debug("[JoinerState] Malformed movie entry cid=%s id=%s", cid, mid_str)
            if typed_map:
                movies_typed[cid] = typed_map

        # --- Other ------------------------------------------------------
        other_typed: Dict[str, Dict[int, List[Any]]] = {}
        raw_other = persisted.get("other_data", {})
        for raw_cid, movie_map in raw_other.items():
            cid = str(raw_cid)
            typed_movie_map: Dict[int, List[Any]] = {}
            for mid_str, vals in movie_map.items():
                try:
                    typed_movie_map[int(mid_str)] = list(vals)
                except (ValueError, TypeError):
                    logging.debug("[JoinerState] Malformed other entry cid=%s id=%s", cid, mid_str)
            if typed_movie_map:
                other_typed[cid] = typed_movie_map

        # --- EOF trackers ----------------------------------------------
        eofs_typed: Dict[str, Dict[str, bool]] = {
            str(cid): dict(tracker) for cid, tracker in persisted.get("eof_trackers", {}).items()
        }

        # --- Processed counts -----------------------------------------
        proc_typed: Dict[str, int] = {}
        raw_proc = persisted.get("processed_counts", {})
        for raw_cid, val in raw_proc.items():
            try:
                proc_typed[str(raw_cid)] = int(val)
            except (ValueError, TypeError):
                continue

        # --- Movies processed (new) -----------------------------------------
        movies_proc_typed: Dict[str, int] = {}
        raw_movies_proc = persisted.get("movies_processed", {})
        for raw_cid, val in raw_movies_proc.items():
            try:
                movies_proc_typed[str(raw_cid)] = int(val)
            except (ValueError, TypeError):
                continue

        return movies_typed, other_typed, eofs_typed, proc_typed, movies_proc_typed

    def add_movie(self, client_id: str, movie_pb) -> List[Any]:
        """Add a movie (protobuf) to the buffer, storing only minimal fields.

        Returns any previously buffered records from the *other* stream that
        match this movie.
        """
        with self._lock:
            self._movies_data.setdefault(client_id, {})[movie_pb.id] = movie_pb.title
            # Ensure EOF tracker exists for this client.
            self._eof_trackers.setdefault(client_id, {"movies": False, "other": False})
            # Pop and return any buffered other data for the same movie.
            other_for_client = self._other_data.get(client_id, {})
            unmatched = other_for_client.pop(movie_pb.id, [])
            # House-keeping – remove empty nested dicts.
            if client_id in self._other_data and not self._other_data[client_id]:
                del self._other_data[client_id]
            # Return an *immutable* view so callers cannot mutate internal state.
            return tuple(unmatched)

    def get_movie(self, client_id: str, movie_id: int):
        """Return movie title (str) or None."""
        return self._movies_data.get(client_id, {}).get(movie_id)

    def buffer_other(self, client_id: str, movie_id: int, other_pb) -> None:
        """Buffers an *other* record that arrived before its movie."""
        with self._lock:
            movie_buff = self._other_data.setdefault(client_id, {})
            movie_buff.setdefault(movie_id, [])
            if isinstance(other_pb, list):
                movie_buff[movie_id].extend(other_pb)
            else:
                movie_buff[movie_id].append(other_pb)

    def get_buffered_other(self, client_id: str, movie_id: int) -> List[Any]:
        """Return (but DO NOT remove) buffered *other* data for a movie."""
        return self._other_data.get(client_id, {}).get(movie_id, [])

    def set_stream_eof(self, client_id: str, stream: str) -> None:
        """Persist that *stream* ("movies" | "other") has sent EOF for client."""
        if stream not in ("movies", "other"):
            raise ValueError("stream must be 'movies' or 'other'")
        with self._lock:
            self._eof_trackers.setdefault(client_id, {"movies": False, "other": False})[stream] = True
            logging.debug(f"EOF for stream '{stream}' received ‑ client {client_id}")
            self._persist(client_id)

    def has_eof(self, client_id: str, stream: str) -> bool:
        return self._eof_trackers.get(client_id, {}).get(stream, False)

    def has_both_eof(self, client_id: str) -> bool:
        tracker = self._eof_trackers.get(client_id)
        return bool(tracker and tracker.get("movies") and tracker.get("other"))

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
            self._persist(client_id)

    def remove_client_data(self, client_id: str) -> None:
        """Remove *all* cached data & trackers for *client_id* to free memory."""
        with self._lock:
            self._movies_data.pop(client_id, None)
            self._other_data.pop(client_id, None)
            self._eof_trackers.pop(client_id, None)
            self._processed_counts.pop(client_id, None)
            self._movies_processed.pop(client_id, None)
            logging.info(f"Clearing state for client {client_id}")

            # Remove on-disk snapshot for the client (if any).
            mgr = self._client_state_managers.pop(client_id, None)
            if mgr is not None:
                mgr.clear()
            pc_mgr = self._pc_managers.pop(client_id, None)
            if pc_mgr is not None:
                pc_mgr.clear()

            mp_mgr = self._mp_managers.pop(client_id, None)
            if mp_mgr is not None:
                mp_mgr.clear()

    def _persist(self, client_id: str) -> None:
        """Persist the state snapshot for *client_id* only."""
        if self._state_manager is None:
            return

        try:
            snap = {
                "movies_data": {client_id: self._movies_data.get(client_id, {})},
                "other_data": {client_id: self._other_data.get(client_id, {})},
                "eof_trackers": {client_id: self._eof_trackers.get(client_id, {})},
            }
            self._get_client_manager(client_id).save(snap)

            # Cleanup: remove snapshot files for clients no longer present in memory.
            active_ids = set(self._movies_data) | set(self._other_data) | set(self._eof_trackers)
            for cid in list(self._client_state_managers.keys()):
                if cid not in active_ids:
                    self._client_state_managers[cid].clear()
                    self._client_state_managers.pop(cid, None)
        except Exception as exc:
            logging.error("[JoinerState] Error while persisting per-client state: %s", exc)
            raise

    def _get_client_manager(self, client_id: str) -> "StatePersistence":
        mgr = self._client_state_managers.get(client_id)
        if mgr is None:
            # Build a dedicated persistence helper for the client.
            base_dir = self._base_dir
            base_file = getattr(self._state_manager, "_file_name", "joiner_state.json")
            base_name = base_file.rsplit(".", 1)[0]
            filename = f"{base_name}_{client_id}.json"
            mgr = StatePersistence(filename, directory=base_dir, serializer="json")
            self._client_state_managers[client_id] = mgr
        return mgr

    def persist_client(self, client_id: str) -> None:
        """Persist state for *client_id* immediately, holding the internal lock
        to avoid concurrent writes that could mutate dictionaries while
        they are being serialised ("dictionary changed size during
        iteration").
        """
        with self._lock:
            self._persist(client_id)

    def increment_processed(self, client_id: str, count: int = 1) -> None:
        """Increment processed counter for client by *count*. This method doest not persist"""
        new_val = self._processed_counts.get(client_id, 0) + int(count)
        self._processed_counts[client_id] = new_val

        mgr = self._pc_managers.get(client_id)
        if mgr is None and self._state_manager is not None:
            mgr = StatePersistence(
                f"processed_{self._node_tag}_client_{client_id}.txt",
                directory=self._base_dir,
                serializer=StatePersistence._JSON,
            )
            self._pc_managers[client_id] = mgr
        if mgr is not None:
            mgr.save(new_val)

    def get_processed_count(self, client_id: str) -> int:
        return self._processed_counts.get(client_id, 0)

    # ------------------------------------------------------------------
    # Movies processed helpers
    # ------------------------------------------------------------------
    def increment_movies_processed(self, client_id: str, count: int = 1) -> None:
        new_val = self._movies_processed.get(client_id, 0) + int(count)
        self._movies_processed[client_id] = new_val

        logging.debug(
            "[JoinerState] movies_processed increment – client=%s +%s → %s",
            client_id,
            count,
            new_val,
        )

        mgr = self._mp_managers.get(client_id)
        if mgr is None:
            mgr = StatePersistence(
                f"processed_movies_{self._node_tag}_client_{client_id}.txt",
                directory=self._base_dir,
                serializer=StatePersistence._JSON,
            )
            self._mp_managers[client_id] = mgr
        mgr.save(new_val)

    def get_movies_processed(self, client_id: str) -> int:
        return self._movies_processed.get(client_id, 0) 