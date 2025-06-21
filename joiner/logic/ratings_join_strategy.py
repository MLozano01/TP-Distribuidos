import logging
from logic.join_strategy import JoinStrategy
from protocol.protocol import Protocol
from messaging.messaging_utils import send_finished_signal

class RatingsJoinStrategy(JoinStrategy):
    """Join strategy that combines movie details with rating records."""

    def __init__(self):
        super().__init__()

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
                self._join_and_send([rating_value], rating.movieId, movie_title, client_id, producer)
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
        return None

    # ------------------------------------------------------------------
    # Helper hooks
    # ------------------------------------------------------------------
    def _join_and_send(self, ratings, movie_id, title, client_id, producer):
        if not ratings:
            return
        rating_val = ratings[0]
        try:
            msg = self.protocol.encode_joined_rating_msg(
                client_id=int(client_id),
                movie_id=movie_id,
                title=title,
                rating=rating_val,
                timestamp="",
            )
            producer.publish(msg)
        except Exception as exc:
            logging.error(
                f"Failed to emit joined rating – client {client_id} movie {movie_id}: {exc}",
                exc_info=True,
            )
            raise

    def process_unmatched_data(self, unmatched_ratings, movie_id, title, client_id, producer):
        for rating_val in unmatched_ratings:
            self._join_and_send([rating_val], movie_id, title, client_id, producer)

    # ------------------------------------------------------------------
    # EOF hooks
    # ------------------------------------------------------------------
    def handle_movie_eof(self, client_id, state):
        """After movies EOF we purge orphan ratings that will never match."""
        state.purge_orphan_other_after_movie_eof(client_id)

    def handle_client_finished(self, client_id, state, producer):
        """Both streams are done – propagate consolidated EOF downstream."""
        send_finished_signal(producer, client_id, self.protocol)
