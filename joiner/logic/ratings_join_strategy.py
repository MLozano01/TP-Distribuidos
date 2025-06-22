from __future__ import annotations
import logging
import os
from logic.join_strategy import JoinStrategy
from protocol.protocol import Protocol
from messaging.messaging_utils import send_finished_signal
from common.batcher import PerClientBatcher
from common.sequence_generator import SequenceGenerator

class RatingsJoinStrategy(JoinStrategy):
    """Join strategy that combines movie details with rating records."""

    def __init__(self, replica_id: int, replicas_count: int):
        super().__init__()
        self._replica_id = replica_id
        self._seqgen = SequenceGenerator(replica_id, replicas_count, namespace="ratings")
        self._batcher: PerClientBatcher | None = None

    # ------------------------------------------------------------------
    # Incoming RATINGS stream
    # ------------------------------------------------------------------
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
        logging.debug(
            f"[RatingsJoinStrategy] client={client_id} finished={ratings_msg.finished} items={len(ratings_msg.ratings)}"
        )

        # ------------------------------------------------------------------
        # Stream finished
        # ------------------------------------------------------------------
        if ratings_msg.finished:
            state.set_stream_eof(client_id, "other")
            # No clean-up yet – wait until both EOFs arrive.
            return client_id

        # ------------------------------------------------------------------
        # Normal data path
        # ------------------------------------------------------------------
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
        self._snapshot_if_needed(client_id)
        return None

    # ------------------------------------------------------------------
    # Helper hooks
    # ------------------------------------------------------------------
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
        except Exception as exc:
            logging.error(
                f"Failed to batch joined rating – client {client_id} movie {movie_id}: {exc}",
                exc_info=True,
            )
            raise

    def process_unmatched_data(self, unmatched_ratings, movie_id, title, client_id, producer):
        for rating_val in unmatched_ratings:
            self._join_and_batch([rating_val], movie_id, title, client_id, producer)

    # ------------------------------------------------------------------
    # EOF hooks
    # ------------------------------------------------------------------
    def handle_movie_eof(self, client_id, state):
        """After movies EOF we purge orphan ratings that will never match."""
        state.purge_orphan_other_after_movie_eof(client_id)

    def handle_client_finished(self, client_id, state, producer):
        """Both streams are done – flush pending batches and propagate EOF."""
        if self._batcher:
            self._batcher.flush_client(client_id)
            self._batcher.clear(client_id)
        last_seq = self._seqgen.current(client_id)
        send_finished_signal(producer, client_id, self.protocol, secuence_number=last_seq)
        self._seqgen.clear(client_id)

    # ----------------------------------------------
    def _ensure_batcher(self, producer):
        if self._batcher is not None:
            return

        batch_size = int(os.getenv("JOINER_BATCH_SIZE", "100"))

        def _encode_batch(ratings_list, cid):
            from protocol import files_pb2
            batch = files_pb2.JoinedRatingsBatch(client_id=cid)
            batch.ratings.extend(ratings_list)
            batch.secuence_number = self._seqgen.next(str(cid))
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
