
from protocol.rabbit_protocol import RabbitMQ
import logging
import json

class Filter:
    def __init__(self, filters, **kwargs):
        self.queue = None
        self.filters = filters
        for key, value in kwargs.items():
            setattr(self, key, value)

    def settle_queues(self):
        pass
        
    def start(self):
        """Start the filter to consume messages from the queue."""
        self.queue = RabbitMQ(self.exchange, self.queue_name, self.routing_key, self.exc_type)
        self.queue.consume(self.callback)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.info(f"Received message")
        result = {}
        try:
            data = json.loads(body)
            for movie in data['movies']:

                for filter in self.filters.values():
                    filter_info = filter[0].split("_")
                    movie_year = movie[filter_info[0]].split("-")[0]

                    if int(movie_year) > int(filter_info[1]):
                        result[movie['title']] = movie
            
            if result:
                logging.info(f"Filtered movies: {result}")
                self.queue.publish(json.dumps(result))
            else:
                logging.info("No movies matched the filter criteria.")

        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}")
            return
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return
        

    def filter(self, data):
        pass

    def apply_filter(self, data):
        pass

    def end_filter(self):
        """End the filter and close the queue."""
        if self.queue:
            self.queue.close_channel()
            logging.info("Filter Stopped")