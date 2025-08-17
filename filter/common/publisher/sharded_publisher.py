import logging
from .publisher import Publisher
from protocol.rabbit_protocol import RabbitMQ, get_routing

class ShardedPublisher(Publisher):
    def __init__(self, protocol, config):
        self.protocol = protocol
        self.config = config
        self.queues_snd_movies_to_ratings_joiner = {}
        self.queues_snd_movies_to_credits_joiner = {}


    def setup_queues(self):
        
        for i in range(1, self.config["ratings_replicas"]+1):
            self.queues_snd_movies_to_ratings_joiner[f"ratings_{i}"] = RabbitMQ(self.config["exchange_snd_ratings"], f'{self.config["q_name_ratings"]}_{i}', f'{self.config["routing_key_ratings"]}_{i}', self.config["exc_snd_type_ratings"])
        
        for i in range(1, self.config["credits_replicas"]+1):
            self.queues_snd_movies_to_credits_joiner[f"credits_{i}"] = RabbitMQ(self.config["exchange_snd_credits"], f'{self.config["q_name_credits"]}_{i}', f'{self.config["routing_key_credits"]}_{i}', self.config["exc_snd_type_credits"])
        logging.info(f"Initialized sharded sender to ratings joiner and credits joiner")

    def publish(self, result_list, client_id, secuence_number):
        if not self.queues_snd_movies_to_ratings_joiner or not self.queues_snd_movies_to_credits_joiner:
            logging.error("Cannot publish by movie_id: One or both sender queues (ratings/credits joiner) are not initialized.")
            return
        # logging.info(f"Publishing {len(result_list)} filtered messages individually by movie_id to exchanges '{exchange_ratings}' and '{exchange_credits}'.")

        movie_batch_bytes = self.protocol.create_movie_list(result_list, client_id, secuence_number)


        # Publish to RATINGS joiner exchange
        try:
            routing_key_ratings = get_routing(self.config["ratings_replicas"], client_id)
            self.queues_snd_movies_to_ratings_joiner[f"ratings_{routing_key_ratings}"].publish(movie_batch_bytes)
        except Exception as e_pub_r:
            logging.error(f"Failed to publish movie batch to RATINGS: {e_pub_r}")
            raise

        # Publish to CREDITS joiner exchange
        try:
            routing_key_credits = get_routing(self.config["credits_replicas"], client_id)
            self.queues_snd_movies_to_credits_joiner[f"credits_{routing_key_credits}"].publish(movie_batch_bytes)
        except Exception as e_pub_c:
            logging.error(f"Failed to publish movie batch to CREDITS exchange: {e_pub_c}")
            raise
        
        logging.info(f"Finished publishing messages to both exchanges.")

    def publish_finished_signal(self, msg):
        msg_to_send = msg.SerializeToString()

        # Publish finished signal to RATINGS joiner
        routing_key_ratings = get_routing(self.config["ratings_replicas"], msg.client_id)
        self.queues_snd_movies_to_ratings_joiner[f"ratings_{routing_key_ratings}"].publish(msg_to_send)

        # Publish finished signal to CREDITS joiner
        routing_key_credits = get_routing(self.config["credits_replicas"], msg.client_id)
        self.queues_snd_movies_to_credits_joiner[f"credits_{routing_key_credits}"].publish(msg_to_send)

        logging.info(
            f"Published movie finished signal for client {msg.client_id} to both joiners with routing_key_ratings={routing_key_ratings} and routing_key_credits={routing_key_credits}."
        )

    def close(self):
        if self.queues_snd_movies_to_ratings_joiner:
            try:
                self.queues_snd_movies_to_ratings_joiner.close_channel()
            except Exception as e:
                logging.error(f"Error closing ratings sender channel: {e}")

        if self.queues_snd_movies_to_credits_joiner:
            try:
                self.queuessnd_movies_to_credits_joiner.close_channel()
            except Exception as e:
                logging.error(f"Error closing credits sender channel: {e}") 