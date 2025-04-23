from protocol.rabbit_protocol import RabbitMQ
from common.aux import parse_filter_funct
import logging
import json
from protocol.protocol import Protocol


class Filter:
    def __init__(self, **kwargs):
        self.queue_rcv = None
        self.queue_snd_ratings = None # Sender for ratings joiners
        self.queue_snd_credits = None # Sender for credits joiners
        self.queue_snd_single = None  # Sender for non-sharded case

        for key, value in kwargs.items():
            setattr(self, key, value)

        # Ensure publish_by_movie_id attribute exists, default to False if somehow missing
        if not hasattr(self, 'publish_by_movie_id'):
            logging.warning("'publish_by_movie_id' not found in config, defaulting to False.")
            self.publish_by_movie_id = False

        self.is_alive = True

    def _settle_queues(self):
        host = getattr(self, 'rabbit_host', 'rabbitmq') # Use getattr with default

        # Initialize Receiver queue (check attributes exist)
        try:
            self.queue_rcv = RabbitMQ(
                self.exchange_rcv,
                self.queue_rcv_name,
                self.routing_rcv_key,
                self.exc_rcv_type
            )
            logging.info("Initialized receiver queue.")
        except AttributeError as e:
            logging.error(f"Missing receiver configuration attribute: {e}")
        except Exception as e:
            logging.error(f"Error initializing receiver queue: {e}")

        # Setup sender(s) based on publish_by_movie_id flag
        if self.publish_by_movie_id:
            # Initialize sender for RATINGS joiners
            try:
                self.queue_snd_ratings = RabbitMQ(
                    self.exchange_snd_ratings,
                    None, # Queue name not needed for publisher
                    None, # Default routing key not needed for publisher when overridden
                    self.exc_snd_type_ratings
                )
                logging.info(f"Initialized ratings sender: exchange={self.queue_snd_ratings.exchange}, type={self.queue_snd_ratings.exc_type}")
            except AttributeError as e:
                 logging.error(f"Missing config attribute for ratings sender: {e}")
            except Exception as e:
                 logging.error(f"Error initializing ratings sender: {e}")

            # Initialize sender for CREDITS joiners
            try:
                self.queue_snd_credits = RabbitMQ(
                    self.exchange_snd_credits,
                    None,
                    None,
                    self.exc_snd_type_credits
                )
                logging.info(f"Initialized credits sender: exchange={self.queue_snd_credits.exchange}, type={self.queue_snd_credits.exc_type}")
            except AttributeError as e:
                 logging.error(f"Missing config attribute for credits sender: {e}")
            except Exception as e:
                 logging.error(f"Error initializing credits sender: {e}")
        else:
            # Setup single sender if not publishing by movie id
            try:
                 self.queue_snd_single = RabbitMQ(
                     self.exchange_snd,
                     self.queue_snd_name,
                     self.routing_snd_key,
                     self.exc_snd_type
                 )
                 logging.info(f"Initialized single sender: exchange={self.queue_snd_single.exchange}, type={self.queue_snd_single.exc_type}, key={self.queue_snd_single.key}")
            except AttributeError as e:
                 logging.error(f"Missing config attribute for single sender: {e}")
            except Exception as e:
                 logging.error(f"Error initializing single sender: {e}")
        
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
        self.filter(body)

    def _publish_individually_by_movie_id(self, result_list, protocol):
        """Helper function to publish filtered movies individually to BOTH exchanges."""
        # Check if both senders were initialized successfully
        if not hasattr(self, 'queue_snd_ratings') or not self.queue_snd_ratings or \
           not hasattr(self, 'queue_snd_credits') or not self.queue_snd_credits:
             logging.error("Cannot publish by movie_id: One or both sender queues are not initialized.")
             return

        exchange_ratings = self.queue_snd_ratings.exchange
        exchange_credits = self.queue_snd_credits.exchange
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
                    self.queue_snd_ratings.publish(single_movie_batch_bytes, routing_key=pub_routing_key)
                    published_to_ratings = True
                except Exception as e_pub_r:
                     logging.error(f"Failed to publish movie ID {movie.id} to RATINGS exchange '{exchange_ratings}': {e_pub_r}")

                # Publish to CREDITS joiner exchange
                try:
                     self.queue_snd_credits.publish(single_movie_batch_bytes, routing_key=pub_routing_key)
                     published_to_credits = True
                except Exception as e_pub_c:
                     logging.error(f"Failed to publish movie ID {movie.id} to CREDITS exchange '{exchange_credits}': {e_pub_c}")

                if published_to_ratings and published_to_credits:
                    published_count += 1 # Count only if sent to both

            except Exception as e_inner:
                logging.error(f"Error processing movie ID {movie.id} before publishing: {e_inner}", exc_info=True)

        logging.info(f"Finished publishing {published_count}/{len(result_list)} messages individually to both exchanges.")

    def filter(self, data):
        try:
            protocol = Protocol()
            decoded_msg = protocol.decode_movies_msg(data)
            result = parse_filter_funct(decoded_msg, self.filter_by, self.file_name)
            
            if result:
                if self.publish_by_movie_id:
                    self._publish_individually_by_movie_id(result, protocol)
                else:
                    # Publish batch to single destination using default key
                    if hasattr(self, 'queue_snd_single') and self.queue_snd_single:
                         logging.info(f"Publishing batch of {len(result)} filtered '{self.file_name}' messages with routing key: '{self.queue_snd_single.key}' to exchange '{self.queue_snd_single.exchange}' ({self.queue_snd_single.exc_type}).")
                         self.queue_snd_single.publish(protocol.create_movie_list(result)) # Use default key
                    else:
                         logging.error("Single sender queue not initialized for non-sharded publish.")
            else:
                logging.info(f"No {self.file_name} matched the filter criteria.")

        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}")
            return
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return

    def end_filter(self):
        """End the filter and close the queue."""
        if self.queue_rcv:
            try: self.queue_rcv.close_channel()
            except Exception as e: logging.error(f"Error closing receiver channel: {e}")

        # Close all potential sender channels safely
        if hasattr(self, 'queue_snd_ratings') and self.queue_snd_ratings:
            try: self.queue_snd_ratings.close_channel()
            except Exception as e: logging.error(f"Error closing ratings sender channel: {e}")

        if hasattr(self, 'queue_snd_credits') and self.queue_snd_credits:
            try: self.queue_snd_credits.close_channel()
            except Exception as e: logging.error(f"Error closing credits sender channel: {e}")

        if hasattr(self, 'queue_snd_single') and self.queue_snd_single:
             try: self.queue_snd_single.close_channel()
             except Exception as e: logging.error(f"Error closing single sender channel: {e}")

        self.is_alive = False
        logging.info("Filter Stopped")