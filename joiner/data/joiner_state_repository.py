from __future__ import annotations

from typing import Any, List

from ..state.joiner_state import JoinerState
from ..domain.state_repository import StateRepository
from common.sequence_number_monitor import SequenceNumberMonitor
from common.state_persistence import StatePersistence


class JoinerStateRepository(StateRepository):
    """Repository that forwards every call to an underlying *JoinerState*.

    No additional behaviour is introduced for now – this class merely acts as
    a seam that will allow us to swap the persistence mechanism (e.g. move to
    a proper DB) without touching the business logic.
    """


    def __init__(self, joiner_state: JoinerState, node_tag: str):
        self._state = joiner_state

        self._seq_monitors: dict[str, SequenceNumberMonitor] = {}
        self._node_tag = str(node_tag)

        def _make_monitor(stream: str):
            fname = f"{stream}_seq_{self._node_tag}.json"
            sp = StatePersistence(fname, directory="/backup", serializer="json", node_info=f"{stream}_{self._node_tag}")
            return SequenceNumberMonitor(sp)

        # Pre-create for movies stream; others will be lazy
        self._seq_monitors["movies"] = _make_monitor("movies")
        self._monitor_factory = _make_monitor

    # ------------------------------------------------------------------
    # Movies helpers
    # ------------------------------------------------------------------
    def add_movie(self, client_id: str, movie_pb) -> List[Any]:
        return self._state.add_movie(client_id, movie_pb)

    def get_movie(self, client_id: str, movie_id: int):
        return self._state.get_movie(client_id, movie_id)

    # ------------------------------------------------------------------
    # Other-stream helpers
    # ------------------------------------------------------------------
    def buffer_other(self, client_id: str, movie_id: int, other_pb) -> None:
        self._state.buffer_other(client_id, movie_id, other_pb)

    def get_buffered_other(self, client_id: str, movie_id: int) -> List[Any]:
        return self._state.get_buffered_other(client_id, movie_id)

    # ------------------------------------------------------------------
    # EOF / lifecycle helpers
    # ------------------------------------------------------------------
    def set_stream_eof(self, client_id: str, stream: str) -> None:  # noqa: D401
        self._state.set_stream_eof(client_id, stream)

    def has_eof(self, client_id: str, stream: str) -> bool:  # noqa: D401
        return self._state.has_eof(client_id, stream)

    def has_both_eof(self, client_id: str) -> bool:
        return self._state.has_both_eof(client_id)

    def purge_orphan_other_after_movie_eof(self, client_id: str) -> None:
        self._state.purge_orphan_other_after_movie_eof(client_id)

    # ------------------------------------------------------------------
    # Accounting helpers
    # ------------------------------------------------------------------
    def persist_client(self, client_id: str) -> None:
        self._state.persist_client(client_id)

    def increment_processed(self, client_id: str, count: int = 1) -> None:
        self._state.increment_processed(client_id, count)

    def get_processed_count(self, client_id: str) -> int:
        return self._state.get_processed_count(client_id)

    # Compatibility alias
    get_others_processed_count = get_processed_count

    # Movies processed
    def increment_movies_processed(self, client_id: str, count: int = 1) -> None:
        self._state.increment_movies_processed(client_id, count)

    def get_movies_processed(self, client_id: str) -> int:
        return self._state.get_movies_processed(client_id)

    # ------------------------------------------------------------------
    # Sequence-number helpers
    # ------------------------------------------------------------------
    def _get_monitor(self, stream: str):
        mon = self._seq_monitors.get(stream)
        if mon is None:
            mon = self._monitor_factory(stream)
            self._seq_monitors[stream] = mon
        return mon

    # Movie-stream specific helpers (only 'movies' monitor used currently)
    def is_duplicate_movie_seq(self, client_id: str, seq_num: str) -> bool:
        return self._get_monitor("movies").is_duplicate(client_id, seq_num)

    def record_movie_seq(self, client_id: str, seq_num: str) -> None:
        self._get_monitor("movies").record(client_id, seq_num)

    def clear_movie_seq_client(self, client_id: str) -> None:
        self._get_monitor("movies").clear_client(client_id)

    # ------------------------------------------------------------------
    # Composite helpers
    # ------------------------------------------------------------------
    def clear_all_seq_client(self, client_id: str) -> None:
        for mon in self._seq_monitors.values():
            mon.clear_client(client_id)

    # Overrides base repository method to also clear all sequence-monitor files
    def remove_client_data(self, client_id: str) -> None:
        self._state.remove_client_data(client_id)
        self.clear_all_seq_client(client_id)

    # ------------------------------------------------------------------
    # Generic delegation helper
    # ------------------------------------------------------------------
    def __getattr__(self, name):  # noqa: D401
        return getattr(self._state, name) 