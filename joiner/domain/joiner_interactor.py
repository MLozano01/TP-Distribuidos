from __future__ import annotations
from typing import Any, List, Optional
import logging

from protocol.protocol import Protocol
from .state_repository import StateRepository


class JoinerInteractor:
    """Use-case coordinator for both *movies* and *other* streams."""

    def __init__(
        self,
        state: StateRepository,
        join_strategy,
        publisher,
        ring_handler,
        movies_publisher: Optional[Any] = None,
        other_publisher: Optional[Any] = None,
        replica_id: int | str = "0",
        protocol: Optional[Protocol] = None,
    ) -> None:
        self._state = state
        self._join_strategy = join_strategy
        self._publisher = publisher
        self._ring_handler = ring_handler
        self._movies_pub = movies_publisher #EOFS
        self._other_pub = other_publisher #EOFS
        self._protocol = protocol or Protocol()

    # ------------------------------------------------------------------
    # Public API – called by the controller
    # ------------------------------------------------------------------
    def handle_movie_body(self, body: bytes) -> None:
        """Entry-point for raw *Movies* messages coming from the broker."""
        movies_msg = self._protocol.decode_movies_msg(body)
        if not movies_msg:
            logging.warning("[Interactor] Received invalid movie message, could not decode.")
            return

        client_id = str(movies_msg.client_id)

        seq_num = str(getattr(movies_msg, "secuence_number", "0"))
        # Sharded publisher may emit multiple messages with the same sequence number
        # (one per movie id).  Use a composite key <seq>-<movieId> to guarantee
        # idempotency per individual movie record.
        if movies_msg.finished:
            movie_id_component = "FINISHED"
        elif movies_msg.movies:
            movie_id_component = str(movies_msg.movies[0].id)
        else:
            movie_id_component = ""  # empty list marker for discard-only batch
        dup_key = f"{seq_num}-{movie_id_component}"
        
        
        if self._state.is_duplicate_movie_seq(client_id, dup_key):
            logging.info(
                "[Interactor] Dropping duplicate MOVIE message key=%s client=%s", dup_key, client_id
            )
            return
        
        #logging.info(f"[Interactor] Received new MOVIE message seq={seq_num} client={client_id} movie_id={movie_id_component}")

        if movies_msg.finished:
            # Notify the ring handler – it will coordinate the global EOF.
            total_processed_local = self._state.get_movies_processed(client_id)
            highest_sn_local = getattr(self._join_strategy, "_seqgen", None)
            if highest_sn_local is not None:
                try:
                    highest_sn_local = highest_sn_local.current(client_id)
                except Exception:
                    highest_sn_local = 0
            else:
                highest_sn_local = 0

            self._ring_handler.on_local_eof(
                stream="movies",
                client_id=client_id,
                total_processed_local=total_processed_local,
                total_to_process=getattr(movies_msg, "total_to_process", movies_msg.total_to_process),
                highest_sn_local=highest_sn_local,
                publisher=self._movies_pub,
            )
        else:
            if movies_msg.movies:
                self._handle_movie_batch(client_id, movies_msg.movies)
            if hasattr(movies_msg, "discarded_count") and movies_msg.discarded_count:
                self._state.increment_movies_processed(client_id, movies_msg.discarded_count)

        self._state.record_movie_seq(client_id, dup_key)

    def handle_other_body(self, body: bytes) -> None:
        """Entry-point for raw *Other* stream messages (ratings/credits)."""
        
        finished_info = self._join_strategy.process_other_message(
            body, self._state, self._publisher
        )

        if finished_info is None:
            return

        try: # TODO: Test and Delete try?
            finished_client_id, total_to_process = finished_info
        except Exception:
            logging.error(
                "[Interactor] Unexpected finished_info=%s (expected tuple).", finished_info
            )
            return

        processed_local = self._state.get_processed_count(finished_client_id)

        highest_sn_local = getattr(self._join_strategy, "_seqgen", None)
        if highest_sn_local is not None:
            try:
                highest_sn_local = highest_sn_local.current(finished_client_id)
            except Exception:
                highest_sn_local = 0
        else:
            highest_sn_local = 0

        self._ring_handler.on_local_eof(
            stream="other",
            client_id=finished_client_id,
            total_processed_local=processed_local,
            total_to_process=total_to_process,
            highest_sn_local=highest_sn_local,
            publisher=self._other_pub,
        )

    # ------------------------------------------------------------------
    # Internal helpers (business rules)
    # ------------------------------------------------------------------

    def _handle_movie_batch(self, client_id: str, movies) -> None:
        if not movies:
            return

        for movie in movies:
            unmatched = self._state.add_movie(client_id, movie)
            if unmatched:
                self._join_strategy.process_unmatched_data(
                    unmatched,
                    movie.id,
                    movie.title,
                    client_id,
                    self._publisher,
                )
        self._state.persist_client(client_id)
        self._state.increment_movies_processed(client_id, len(movies))
        #logging.info("[Interactor] Incremented %d movies processed for client %s", len(movies), client_id)

    def _check_and_handle_client_finished(self, client_id: str) -> None:
        if not self._state.has_both_eof(client_id):
            return

        logging.info("[Interactor] Both streams finished for client %s. Finalising...", client_id)
        try:
            self._join_strategy.handle_client_finished(
                client_id, self._state, self._publisher
            )
        finally:
            self._state.remove_client_data(client_id) 