from protocol.rabbit_protocol import RabbitMQ
from common.aux import parse_filter_funct
import logging
import json
from protocol.protocol import Protocol, MOVIES_FILE_CODE, FileType
from protocol import files_pb2


class Filter:
    def __init__(self, **kwargs):
        self.protocol = Protocol()
        self.queue_rcv = None # For receiving movies data + finished signal
        self.queue_snd_movies_to_ratings_joiner = None
        self.queue_snd_movies_to_credits_joiner = None
        self.queue_snd_movies = None  # For publishing movies data
        self.finished_filter_arg_step_publisher = None # for notifying joiners

        for key, value in kwargs.items():
            setattr(self, key, value)

        # Ensure publish_to_joiners attribute exists, default to False if somehow missing
        if not hasattr(self, 'publish_to_joiners'):
            logging.warning("'publish_to_joiners' not found in config, defaulting to False.")
            self.publish_to_joiners = False

        self.is_alive = True

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type)

        # Setup DATA sender(s) based on publish_to_joiners flag
        if self.publish_to_joiners:
            self.queue_snd_movies_to_ratings_joiner = RabbitMQ(self.exchange_snd_ratings, None, None, self.exc_snd_type_ratings)
            logging.info(f"Initialized ratings sender: exchange={self.queue_snd_movies_to_ratings_joiner.exchange}, type={self.queue_snd_movies_to_ratings_joiner.exc_type}")

            self.queue_snd_movies_to_credits_joiner = RabbitMQ(self.exchange_snd_credits, None, None, self.exc_snd_type_credits)
            logging.info(f"Initialized credits sender: exchange={self.queue_snd_movies_to_credits_joiner.exchange}, type={self.queue_snd_movies_to_credits_joiner.exc_type}")

            self.finished_filter_arg_step_publisher = RabbitMQ("filter_arg_step_finished", None, "", "fanout")

        else:
            self.queue_snd_movies = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
            logging.info(f"Initialized single sender: exchange={self.queue_snd_movies.exchange}, type={self.queue_snd_movies.exc_type}, key={self.queue_snd_movies.key}")
        

    def run(self):
        """Start the filter to consume messages from the queue."""
        self._settle_queues()
        if not self.queue_rcv: # Check if receiver queue settled
            logging.error("Receiver queue not initialized. Filter cannot run.")
            return
        self.queue_rcv.consume(self.callback)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.info(f"Received message, with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)
            
        if decoded_msg.finished:
            logging.info("Received MOVIES finished signal from server on data channel.")
            self._publish_movie_finished_signal(decoded_msg)
            return
        self.filter(decoded_msg)

    def filter(self, decoded_msg):
        try:
            result = parse_filter_funct(decoded_msg, self.filter_by, self.file_name)
            
            if result:
                if self.publish_to_joiners:
                    self._publish_individually_by_movie_id(result, self.protocol)
                else:
                    if hasattr(self, 'queue_snd_movies') and self.queue_snd_movies:
                            logging.info(f"Publishing batch of {len(result)} filtered '{self.file_name}' messages with routing key: '{self.queue_snd_movies.key}' to exchange '{self.queue_snd_movies.exchange}' ({self.queue_snd_movies.exc_type}).")
                            self.queue_snd_movies.publish(self.protocol.create_movie_list(result))
                    else:
                            logging.error("Single sender queue not initialized for non-sharded publish.")
            else:
                logging.info(f"No {self.file_name} matched the filter criteria.")

        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return

    def _publish_individually_by_movie_id(self, result_list, protocol):
        """Helper function to publish filtered movies individually to BOTH exchanges."""
        # Check if both senders were initialized successfully using correct attribute names
        if not hasattr(self, 'queue_snd_movies_to_ratings_joiner') or not self.queue_snd_movies_to_ratings_joiner or \
           not hasattr(self, 'queue_snd_movies_to_credits_joiner') or not self.queue_snd_movies_to_credits_joiner:
             logging.error("Cannot publish by movie_id: One or both sender queues (ratings/credits joiner) are not initialized.")
             return

        exchange_ratings = self.queue_snd_movies_to_ratings_joiner.exchange
        exchange_credits = self.queue_snd_movies_to_credits_joiner.exchange
        logging.info(f"Publishing {len(result_list)} filtered '{self.file_name}' messages individually by movie_id to exchanges '{exchange_ratings}' and '{exchange_credits}'.")

        published_count = 0
        for movie in result_list:
            try:
                if not movie.id:
                    logging.warning(f"Skipping movie with missing ID: {movie.title}")
                    continue

                single_movie_batch_bytes = protocol.create_movie_list([movie])
                pub_routing_key = str(movie.id) # Explicit routing key

                published_to_ratings = False
                published_to_credits = False

                # Publish to RATINGS joiner exchange
                try:
                    self.queue_snd_movies_to_ratings_joiner.publish(single_movie_batch_bytes, routing_key=pub_routing_key)
                    published_to_ratings = True
                except Exception as e_pub_r:
                     logging.error(f"Failed to publish movie ID {movie.id} to RATINGS exchange '{exchange_ratings}': {e_pub_r}")

                # Publish to CREDITS joiner exchange
                try:
                     self.queue_snd_movies_to_credits_joiner.publish(single_movie_batch_bytes, routing_key=pub_routing_key)
                     published_to_credits = True
                except Exception as e_pub_c:
                     logging.error(f"Failed to publish movie ID {movie.id} to CREDITS exchange '{exchange_credits}': {e_pub_c}")

                if published_to_ratings and published_to_credits:
                    published_count += 1 # Count only if sent to both

            except Exception as e_inner:
                logging.error(f"Error processing movie ID {movie.id} before publishing: {e_inner}", exc_info=True)

        logging.info(f"Finished publishing {published_count}/{len(result_list)} messages individually to both exchanges.")


    def _publish_movie_finished_signal(self, msg):
        """Publishes the movie finished signal. If its to joiners it publishis to a specific fanout exchange, otherwise it publishes to the default key of the single sender queue."""
        if self.publish_to_joiners:
            self.finished_filter_arg_step_publisher.publish(self.protocol.create_finished_message_for_joiners(FileType.MOVIES))
            logging.info(f"Published movie finished signal to {self.finished_filter_arg_step_publisher.exchange}")
        else:
            self.queue_snd_movies.publish(msg.SerializeToString())
            logging.info(f"Published movie finished signal to {self.queue_snd_movies.exchange}")

    def end_filter(self):
        """End the filter and close the queue."""
        if self.queue_rcv:
            try: self.queue_rcv.close_channel()
            except Exception as e: logging.error(f"Error closing receiver channel: {e}")

        if hasattr(self, 'queue_snd_ratings') and self.queue_snd_movies_to_ratings_joiner:
            try: self.queue_snd_movies_to_ratings_joiner.close_channel()
            except Exception as e: logging.error(f"Error closing ratings sender channel: {e}")

        if hasattr(self, 'queue_snd_credits') and self.queue_snd_movies_to_credits_joiner:
            try: self.queue_snd_movies_to_credits_joiner.close_channel()
            except Exception as e: logging.error(f"Error closing credits sender channel: {e}")

        if hasattr(self, 'queue_snd_single') and self.queue_snd_single:
             try: self.queue_snd_single.close_channel()
             except Exception as e: logging.error(f"Error closing single sender channel: {e}")

        # Close control consumer and publisher
        if self.movies_control_publisher:
            try: self.movies_control_publisher.close_channel()
            except Exception as e: logging.error(f"Error closing movie control publisher channel: {e}")

        self.is_alive = False
        logging.info("Filter Stopped")