import logging
from .publisher import Publisher
from protocol.rabbit_protocol import RabbitMQ

class ShardedPublisher(Publisher):
    def __init__(self, protocol, exchange_snd_ratings, exc_snd_type_ratings, exchange_snd_credits, exc_snd_type_credits):
        self.protocol = protocol
        self.exchange_snd_ratings = exchange_snd_ratings
        self.exc_snd_type_ratings = exc_snd_type_ratings
        self.exchange_snd_credits = exchange_snd_credits
        self.exc_snd_type_credits = exc_snd_type_credits
        self.queue_snd_movies_to_ratings_joiner = None
        self.queue_snd_movies_to_credits_joiner = None

    def setup_queues(self):
        self.queue_snd_movies_to_ratings_joiner = RabbitMQ(self.exchange_snd_ratings, None, "", self.exc_snd_type_ratings)
        self.queue_snd_movies_to_credits_joiner = RabbitMQ(self.exchange_snd_credits, None, "", self.exc_snd_type_credits)
        logging.info(f"Initialized sharded sender to ratings joiner and credits joiner")

    def publish(self, result_list, client_id):
        if not self.queue_snd_movies_to_ratings_joiner or not self.queue_snd_movies_to_credits_joiner:
            logging.error("Cannot publish by movie_id: One or both sender queues (ratings/credits joiner) are not initialized.")
            return

        exchange_ratings = self.queue_snd_movies_to_ratings_joiner.exchange
        exchange_credits = self.queue_snd_movies_to_credits_joiner.exchange
        logging.info(f"Publishing {len(result_list)} filtered messages individually by movie_id to exchanges '{exchange_ratings}' and '{exchange_credits}'.")

        published_count = 0
        for movie in result_list:
            try:
                if not movie.id:
                    logging.warning(f"Skipping movie with missing ID: {movie.title}")
                    continue

                single_movie_batch_bytes = self.protocol.create_movie_list([movie], client_id)
                pub_routing_key = str(movie.id)

                # Publish to RATINGS joiner exchange
                try:
                    self.queue_snd_movies_to_ratings_joiner.publish(single_movie_batch_bytes, routing_key=pub_routing_key)
                except Exception as e_pub_r:
                    logging.error(f"Failed to publish movie ID {movie.id} to RATINGS exchange '{exchange_ratings}': {e_pub_r}")
                    continue # If one fails, don't try the other for this movie

                # Publish to CREDITS joiner exchange
                try:
                    self.queue_snd_movies_to_credits_joiner.publish(single_movie_batch_bytes, routing_key=pub_routing_key)
                except Exception as e_pub_c:
                    logging.error(f"Failed to publish movie ID {movie.id} to CREDITS exchange '{exchange_credits}': {e_pub_c}")
                    # Should we try to rollback the ratings publish? For now, we just log and continue
                    continue
                
                published_count += 1

            except Exception as e_inner:
                logging.error(f"Error processing movie ID {movie.id} before publishing: {e_inner}", exc_info=True)

        logging.info(f"Finished publishing {published_count}/{len(result_list)} messages individually to both exchanges.")

    def publish_finished_signal(self, msg):
        self.queue_snd_movies_to_ratings_joiner.publish(self.protocol.create_movie_finished_msg(msg.client_id))
        self.queue_snd_movies_to_credits_joiner.publish(self.protocol.create_movie_finished_msg(msg.client_id))
        logging.info(f"Published movie finished signal for client {msg.client_id} to both joiners.")

    def close(self):
        if self.queue_snd_movies_to_ratings_joiner:
            try:
                self.queue_snd_movies_to_ratings_joiner.close_channel()
            except Exception as e:
                logging.error(f"Error closing ratings sender channel: {e}")

        if self.queue_snd_movies_to_credits_joiner:
            try:
                self.queue_snd_movies_to_credits_joiner.close_channel()
            except Exception as e:
                logging.error(f"Error closing credits sender channel: {e}") 