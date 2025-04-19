
from protocol.rabbit_protocol import RabbitMQ
import logging
import json

class Filter:
    def __init__(self, filters, **kwargs):
        self.queue = None
        self.filters = filters
        for key, value in kwargs.items():
            setattr(self, key, value)

    def start(self):
        """Start the filter to consume messages from the queue."""
        self.queue = RabbitMQ(self.exchange, self.queue_name, self.routing_key, self.exc_type)
        self.queue.consume(self.callback)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.info("Received message")
        self.filter(body.decode())

        

    def filter(self, data):
        result = {}
        try:
            data = json.loads(data)
            
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}")
            return
        for movie in data["movies"]:
            print(movie["releaseDate"])

    def apply_filter(self, data):
        pass

    def end_filter(self):
        """End the filter and close the queue."""
        if self.queue:
            self.queue.close_channel()
            logging.info("Filter Stopped")