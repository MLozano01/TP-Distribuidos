import logging
from logic.join_strategy import JoinStrategy
from protocol.protocol import Protocol
from messaging.messaging_utils import send_movie_batch, send_finished_signal, BATCH_SIZE

class RatingsJoinStrategy(JoinStrategy):
    """
    Joining strategy for movie ratings.
    """
    def __init__(self):
        self.protocol = Protocol()

    def process_other_message(self, body, state, producer):
        ratings_msg = self.protocol.decode_ratings_msg(body)
        if not ratings_msg:
            logging.warning("[Node] Could not decode ratings message.")
            return

        client_id = ratings_msg.client_id
        logging.info(f"[Node] Processing ratings message for client {client_id}. Finished: {ratings_msg.finished}, Items: {len(ratings_msg.ratings)}")

        if ratings_msg.finished:
            if state.set_other_eof(client_id):
                self.trigger_final_processing(client_id, state, producer)
            return

        for rating in ratings_msg.ratings:
            state.add_other_data(client_id, rating.movieId, (rating.rating, 1), aggregate=True)

    def trigger_final_processing(self, client_id, state, producer):
        logging.info(f"Triggering final processing for RATINGS join, client {client_id}")
        
        movies_buffer, other_buffer = state.pop_buffers_for_processing(client_id)

        if not movies_buffer:
            logging.warning(f"No movie data to process for client {client_id}, ending.")
            send_finished_signal(producer, client_id, self.protocol)
            return

        processed_movies_batch = []
        for movie_id, movie in movies_buffer.items():
            if movie_id in other_buffer:
                total_rating, count = other_buffer[movie_id]
                if count > 0:
                    movie.average_rating = total_rating / count
                    movie.vote_count = count
                    processed_movies_batch.append(movie)
            # Only include movies that actually received ratings; skip otherwise to
            # avoid polluting downstream aggregators with default 0.0 averages.

            if len(processed_movies_batch) >= BATCH_SIZE:
                send_movie_batch(producer, processed_movies_batch, client_id, self.protocol)
                processed_movies_batch = []

        if processed_movies_batch:
            send_movie_batch(producer, processed_movies_batch, client_id, self.protocol)
        
        send_finished_signal(producer, client_id, self.protocol)
        logging.info(f"Finished processing for RATINGS join, client {client_id}") 