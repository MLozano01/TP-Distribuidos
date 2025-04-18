
from protocol.rabbit_protocol import RabbitMQ
import logging

class Filter:
    def __init__(self, **kwargs):
        self.queue = None
        for key, value in kwargs.items():
            setattr(self, key, value)

    def start(self):
        """Start the filter to consume messages from the queue."""

        self.queue = RabbitMQ(self.exchange, self.queue_name, self.routing_key, self.exc_type)
        self.queue.consume(self.callback)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.info(f"Received message: {body.decode()}")
        # message = body.decode()

    def filter_by(data):
        pass

    def end_filter(self):
        """End the filter and close the queue."""
        if self.queue:
            self.queue.close_channel()
            logging.info("Filter Stopped")