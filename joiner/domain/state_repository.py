from __future__ import annotations

"""Domain layer definition for the Joiner state repository interface.

This abstraction allows us to decouple the business logic (join strategies
and the node orchestration) from the concrete persistence mechanism that
backs the state (JSON files, databases, etc.).  A concrete implementation
is provided in *joiner.data.joiner_state_repository.JoinerStateRepository*.
"""

from abc import ABC, abstractmethod
from typing import Any, List


class StateRepository(ABC):
    """Abstract repository exposing the operations required by the Joiner
    business-logic layer.  It intentionally mirrors the public API of the
    legacy *JoinerState* helper so that the existing logic can remain
    unchanged while gaining the benefits of DI and clear separation of
    concerns.
    """

    # ------------------------------------------------------------------
    # Movies helpers
    # ------------------------------------------------------------------
    @abstractmethod
    def add_movie(self, client_id: str, movie_pb) -> List[Any]:
        """Store *movie_pb* and return any previously buffered records that
        matched the movie (if available)."""

    @abstractmethod
    def get_movie(self, client_id: str, movie_id: int):
        """Return the *title* for *movie_id* if it is already known."""

    # ------------------------------------------------------------------
    # Other-stream helpers
    # ------------------------------------------------------------------
    @abstractmethod
    def buffer_other(self, client_id: str, movie_id: int, other_pb) -> None:
        """Temporarily store an *other* record until its movie arrives."""

    @abstractmethod
    def get_buffered_other(self, client_id: str, movie_id: int) -> List[Any]:
        """Return buffered *other* records for *movie_id* (may be empty)."""

    # ------------------------------------------------------------------
    # EOF / lifecycle helpers
    # ------------------------------------------------------------------
    @abstractmethod
    def set_stream_eof(self, client_id: str, stream: str) -> None:
        pass

    @abstractmethod
    def has_eof(self, client_id: str, stream: str) -> bool:
        pass

    @abstractmethod
    def has_both_eof(self, client_id: str) -> bool:
        pass

    @abstractmethod
    def purge_orphan_other_after_movie_eof(self, client_id: str) -> None:
        pass

    @abstractmethod
    def remove_client_data(self, client_id: str) -> None:
        pass

    # ------------------------------------------------------------------
    # Accounting helpers
    # ------------------------------------------------------------------
    @abstractmethod
    def persist_client(self, client_id: str) -> None:
        pass

    @abstractmethod
    def increment_processed(self, client_id: str, count: int = 1) -> None:
        pass

    @abstractmethod
    def get_processed_count(self, client_id: str) -> int:
        pass

    # ------------------------------------------------------------------
    # Sequence-number deduplication helpers (per stream)
    # ------------------------------------------------------------------
    @abstractmethod
    def is_duplicate_movie_seq(self, client_id: str, seq_num: str) -> bool:
        """Movie-stream deduplication helper."""

    @abstractmethod
    def record_movie_seq(self, client_id: str, seq_num: str) -> None:
        """Persist movie sequence number after successful processing."""

    @abstractmethod
    def clear_movie_seq_client(self, client_id: str) -> None:
        """Clear stored movie sequence numbers for *client_id*."""

    # Generic helper to purge ALL sequence-number data for client (movies + others)
    @abstractmethod
    def clear_all_seq_client(self, client_id: str) -> None:
        pass

    # ------------------------------------------------------------------
    # Generic delegation (optional)
    # ------------------------------------------------------------------
    def __getattr__(self, item):  # noqa: D401 – delegation helper
        """Allow the domain layer to access extra helper methods that might
        not be explicitly declared here without breaking at runtime.  This
        keeps the interface succinct whilst maintaining backward
        compatibility with the existing codebase.
        """
        raise AttributeError(item)

    # Movies processed counters
    @abstractmethod
    def increment_movies_processed(self, client_id: str, count: int = 1) -> None:
        pass

    @abstractmethod
    def get_movies_processed(self, client_id: str) -> int:
        pass

    @abstractmethod
    def get_others_processed_count(self, client_id: str) -> int:
        pass 