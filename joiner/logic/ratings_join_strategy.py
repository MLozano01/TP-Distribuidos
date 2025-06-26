from __future__ import annotations
import logging
import os
from logic.join_strategy import JoinStrategy
from protocol.protocol import Protocol

from common.batcher import PerClientBatcher
from common.sequence_generator import SequenceGenerator
from typing import Optional

class RatingsJoinStrategy(JoinStrategy):
    """Join strategy that combines movie details with rating records."""

    def __init__(self, replica_id: int, replicas_count: int, state_manager):
        super().__init__(state_manager)
        self._replica_id = replica_id
        self._seqgen = SequenceGenerator(replica_id, replicas_count, namespace="ratings")
        self._batcher: PerClientBatcher | None = None

    def process_other_message(self, body, state, producer):
        """Process a RatingsMsg coming from RabbitMQ.

        Returns the `client_id` if the message represents an EOF for that
        client, `None` otherwise.
        """
        ratings_msg = self.protocol.decode_ratings_msg(body)
        if not ratings_msg:
            logging.warning("[RatingsJoinStrategy] Could not decode ratings message.")
            return None

        client_id = str(ratings_msg.client_id)
        seq = ratings_msg.secuence_number
        movie_id_val = ratings_msg.ratings[0].movieId if ratings_msg.ratings else "-"

        seq_num = str(ratings_msg.secuence_number)
        dedup_key = f"{seq_num}-{movie_id_val}"

        if self._seq_monitor.is_duplicate(client_id, dedup_key):
            logging.info(
                f"[RatingsJoinStrategy] Discarding duplicate RATING message key={dedup_key} client={client_id}"
            )
            return None


        if ratings_msg.finished:
            return client_id, ratings_msg.total_to_process

        #logging.info(
        #    f"[RatingsJoinStrategy] Received new RATING client={client_id} seq={seq} movie_id={movie_id_val} items={len(ratings_msg.ratings)}"
        #)
        if ratings_msg.ratings:
            state.increment_processed(client_id, len(ratings_msg.ratings))

        for rating in ratings_msg.ratings:
            rating_value = float(rating.rating)
            movie_title = state.get_movie(client_id, rating.movieId)
            if movie_title:
                self._join_and_batch([rating_value], rating.movieId, movie_title, client_id, producer)
                continue

            # Movie not (yet) available – should we buffer or discard?  If the
            # movies stream already ended, the movie will never arrive.
            if state.has_eof(client_id, "movies"):
                logging.debug(
                    f"[RatingsJoinStrategy] Discarding rating for movie {rating.movieId} "
                    f"(client {client_id}) – movies stream already EOF."
                )
                continue

            state.buffer_other(client_id, rating.movieId, rating_value)

        state.persist_client(client_id)
        self._seq_monitor.record(client_id, dedup_key)
        self._snapshot_if_needed(client_id)
        return None

    def _join_and_batch(self, ratings, movie_id, title, client_id, producer):
        if not ratings:
            return
        rating_val = ratings[0]
        try:
            self._ensure_batcher(producer)

            # Build JoinedRating PB
            from protocol import files_pb2

            rating_pb = files_pb2.JoinedRating(
                movie_id=movie_id,
                title=title,
                rating=rating_val,
                timestamp="",
            )

            self._batcher.add(rating_pb, client_id)
            logging.debug(
                "[RatingsJoinStrategy] queue joined rating – client=%s movie_id=%s rating=%s",
                client_id,
                movie_id,
                rating_val,
            )
        except Exception as exc:
            logging.error(
                f"Failed to batch joined rating – client {client_id} movie {movie_id}: {exc}",
                exc_info=True,
            )
            raise

    def process_unmatched_data(self, unmatched_ratings, movie_id, title, client_id, producer):
        for rating_val in unmatched_ratings:
            self._join_and_batch([rating_val], movie_id, title, client_id, producer)


    def handle_client_finished(self, client_id, state):
        """Both streams are done – flush pending batches and propagate EOF."""
        if self._batcher:
            self._batcher.flush_key(client_id)
            self._batcher.clear(client_id)

        state.remove_client_data(client_id)
        self._seqgen.clear(client_id)

        if hasattr(self, "_seq_monitor") and self._seq_monitor:
            try:
                self._seq_monitor.clear_client(client_id)
            except Exception as exc:
                logging.error("[RatingsJoinStrategy] Error clearing seq monitor for client %s: %s", client_id, exc)

    def _ensure_batcher(self, producer):
        if self._batcher is not None:
            return

        batch_size = int(os.getenv("JOINER_BATCH_SIZE", "100"))

        def _encode_batch(ratings_list, cid):
            from protocol import files_pb2
            seq = self._seqgen.next(str(cid))
            batch = files_pb2.JoinedRatingsBatch(client_id=cid)
            batch.ratings.extend(ratings_list)
            batch.secuence_number = seq
            count = self._batcher.flushes(str(cid)) if self._batcher else 0
            batch.expected_batches = count + 1
            
            logging.info(
                "[RatingsJoinStrategy] Sending batch – client=%s seq=%s items=%s",
                cid,
                seq,
                len(ratings_list),
            )
            return batch.SerializeToString()

        self._batcher = PerClientBatcher(
            producer,
            _encode_batch,
            max_items=batch_size,
            namespace=f"ratings_r{self._replica_id}",
        )

    def _snapshot_if_needed(self, client_id):
        if self._batcher is not None:
            try:
                self._batcher.snapshot_key(client_id)
            except Exception as exc:
                logging.error("Error snapshotting batch for client %s: %s", client_id, exc)
                raise

    def get_flushed_batches(self, client_id: str) -> Optional[int]:
        if self._batcher is None:
            return None
        try:
            return self._batcher.flushes(client_id)
        except Exception:
            return None

    def clear_flushed_batches(self, client_id: str) -> None:
        if self._batcher is None:
            return
        try:
            self._batcher.pop_flushes(client_id)
        except Exception:
            pass
