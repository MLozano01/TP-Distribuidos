from protocol.rabbit_protocol import RabbitMQ
from common.aux import parse_filter_funct
import logging
import json
from protocol.protocol import Protocol


class Filter:
    def __init__(self, **kwargs):
        self.queue_rcv = None
        self.queue_snd = None
        self.publish_by_movie_id = kwargs.get('publish_by_movie_id', False)
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.is_alive = True

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type)
        self.queue_snd = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
        
    def run(self):
        """Start the filter to consume messages from the queue."""
        self._settle_queues()
        self.queue_rcv.consume(self.callback)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.info(f"Received message, with routing key: {method.routing_key}")
        self.filter(body)

    def _publish_individually_by_movie_id(self, result_list, protocol):
        """Helper function to publish filtered movies individually using movie_id as routing key."""
        logging.info(f"Publishing {len(result_list)} filtered '{self.file_name}' messages individually by movie_id to exchange '{self.exchange_snd}' ({self.exc_snd_type}).")
        published_count = 0
        for movie in result_list:
            try:
                if not movie.id:
                    logging.warning(f"Skipping movie with missing ID: {movie.title}")
                    continue

                single_movie_batch_bytes = protocol.create_movie_list([movie])
                pub_routing_key = str(movie.id) # Explicit routing key
                # Publish single movie with explicit routing key
                self.queue_snd.publish(single_movie_batch_bytes, routing_key=pub_routing_key)
                published_count += 1
            except Exception as e_pub:
                logging.error(f"Failed to publish movie ID {movie.id}: {e_pub}", exc_info=True)
        logging.info(f"Finished publishing {published_count}/{len(result_list)} messages individually.")

    def filter(self, data):
        try:
            protocol = Protocol()
            decoded_msg = protocol.decode_movies_msg(data)
            result = parse_filter_funct(decoded_msg, self.filter_by, self.file_name)
            
            if result:
                if self.publish_by_movie_id:
                    self._publish_individually_by_movie_id(result, protocol)
                else:
                    logging.info(f"Publishing batch of {len(result)} filtered '{self.file_name}' messages with routing key: '{self.routing_snd_key}' to exchange '{self.exchange_snd}' ({self.exc_snd_type}).")
                    batch_bytes = protocol.create_movie_list(result)
                    self.queue_snd.publish(batch_bytes) 
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
            self.queue_rcv.close_channel()
        if self.queue_snd:
            self.queue_snd.close_channel()
        self.is_alive = False
        logging.info("Filter Stopped")