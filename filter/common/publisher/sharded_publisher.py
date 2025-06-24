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

    def publish(self, result_list, client_id, secuence_number):
        if not self.queue_snd_movies_to_ratings_joiner or not self.queue_snd_movies_to_credits_joiner:
            logging.error("Cannot publish by movie_id: One or both sender queues (ratings/credits joiner) are not initialized.")
            return

        exchange_ratings = self.queue_snd_movies_to_ratings_joiner.exchange
        exchange_credits = self.queue_snd_movies_to_credits_joiner.exchange
        # logging.info(f"Publishing {len(result_list)} filtered messages individually by movie_id to exchanges '{exchange_ratings}' and '{exchange_credits}'.")

        total = len(result_list)
        for mv in result_list:
            try:
                batch_bytes = self.protocol.create_movie_list([mv], client_id, secuence_number)
                routing_key = str(mv.id)

                # RATINGS
                self.queue_snd_movies_to_ratings_joiner.publish(batch_bytes, routing_key=routing_key)

                # CREDITS
                self.queue_snd_movies_to_credits_joiner.publish(batch_bytes, routing_key=routing_key)
            except Exception as exc:
                logging.error(
                    f"Error publishing movie id {getattr(mv, 'id', '?')} to exchanges '{exchange_ratings}'/'{exchange_credits}': {exc}"
                )
                raise

        logging.info(f"Published {total} individual movie batches (seq={secuence_number}) to both joiner exchanges.")

    def publish_finished_signal(self, msg):
        msg_to_send = msg.SerializeToString()
        pub_routing_key = str(msg.client_id)

        # Publish finished signal to RATINGS joiner
        self.queue_snd_movies_to_ratings_joiner.publish(msg_to_send, pub_routing_key)

        # Publish finished signal to CREDITS joiner
        self.queue_snd_movies_to_credits_joiner.publish(msg_to_send, pub_routing_key)

        logging.info(
            f"Published movie finished signal for client {msg.client_id} to both joiners with routing_key={pub_routing_key}."
        )

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