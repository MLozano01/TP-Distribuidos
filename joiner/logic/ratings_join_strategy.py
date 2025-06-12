import logging
from logic.join_strategy import JoinStrategy
from protocol.protocol import Protocol
from messaging.messaging_utils import send_finished_signal

class RatingsJoinStrategy(JoinStrategy):
    """
    Joining strategy for movie ratings in a streaming fashion.
    """
    def __init__(self):
        super().__init__()

    def process_other_message(self, body, state, producer):
        ratings_msg = self.protocol.decode_ratings_msg(body)
        if not ratings_msg:
            logging.warning("[Node] Could not decode ratings message.")
            return

        client_id = ratings_msg.client_id
        logging.info(f"[RatingsJoinStrategy] Processing message for client {client_id}. Finished: {ratings_msg.finished}, Items: {len(ratings_msg.ratings)}")

        if ratings_msg.finished:
            logging.info(f"Ratings EOF received for client {client_id}.")
            self.process_other_eof(client_id, state)
            # Forward the EOF signal
            send_finished_signal(producer, client_id, self.protocol)
            return

        for rating in ratings_msg.ratings:
            movie_data = state.get_movie(client_id, rating.movieId)
            if movie_data:
                # Movie is already in our buffer, join and send immediately
                self._join_and_send([rating], movie_data, client_id, producer)
            else:
                # Movie not seen yet. Check if movie stream has ended.
                if state.has_movies_eof(client_id):
                    logging.warning(f"Rating for movie {rating.movieId} for client {client_id} arrived after movies EOF, and movie not found in buffer. Discarding.")
                else:
                    # Movie stream is still active, so buffer the rating.
                    state.add_unmatched_other(client_id, rating.movieId, rating)

    def _join_and_send(self, ratings, movie_data, client_id, producer):
        # We are sending one joined rating at a time, as requested
        # to be more stream-like.
        if not ratings:
            return

        try:
            # For now, we assume the protocol can handle a list of ratings
            # to be joined with one movie.
            # We'll create a new message type for this.
            rating = ratings[0] # The user wants to join as it is received
            msg = self.protocol.encode_joined_rating_msg(
                client_id=client_id,
                movie_id=movie_data.id,
                title=movie_data.title,
                rating=rating.rating,
                timestamp=rating.timestamp
            )
            producer.publish(msg)

        except Exception as e:
            logging.error(f"Error sending joined rating for client {client_id}, movie {movie_data.id}: {e}", exc_info=True)

    def process_unmatched_data(self, unmatched_ratings, movie_data, client_id, producer):
        logging.info(f"Processing {len(unmatched_ratings)} unmatched ratings for movie {movie_data.id}, client {client_id}")
        # In ratings, we still process one by one
        for rating in unmatched_ratings:
            self._join_and_send([rating], movie_data, client_id, producer)

    def process_other_eof(self, client_id, state):
        logging.info(f"Processing ratings EOF for client {client_id}. Clearing all related state.")
        state.clear_client_state(client_id) 