import logging
from .publisher import Publisher
from protocol.rabbit_protocol import RabbitMQ

class SinglePublisher(Publisher):
    def __init__(self, protocol, exchange_snd, queue_snd_name, routing_snd_key, exc_snd_type):
        self.protocol = protocol
        self.exchange_snd = exchange_snd
        self.queue_snd_name = queue_snd_name
        self.routing_snd_key = routing_snd_key
        self.exc_snd_type = exc_snd_type
        self.queue_snd_movies = None

    def setup_queues(self):
        self.queue_snd_movies = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
        logging.info(f"Initialized single sender: exchange={self.queue_snd_movies.exchange}, type={self.queue_snd_movies.exc_type}, key={self.queue_snd_movies.key}")

    def publish(self, result, client_id):
        if self.queue_snd_movies:
            logging.info(f"Publishing batch of {len(result)} filtered messages with routing key: '{self.queue_snd_movies.key}' to exchange '{self.queue_snd_movies.exchange}' ({self.queue_snd_movies.exc_type}).")
            self.queue_snd_movies.publish(self.protocol.create_movie_list(result, client_id))
        else:
            logging.error("Single sender queue not initialized for non-sharded publish.")

    def publish_finished_signal(self, msg):
        msg_to_send = msg.SerializeToString()
        self.queue_snd_movies.publish(msg_to_send)
        logging.info(f"Published movie finished signal to {self.queue_snd_movies.exchange}")

    def close(self):
        if self.queue_snd_movies:
            try:
                self.queue_snd_movies.close_channel()
            except Exception as e:
                logging.error(f"Error closing single sender channel: {e}") 