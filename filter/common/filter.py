
from protocol.protocol import RabbitMQ
import logging

class Filter:
    def __init__(self, **kwargs):
        self.queue = None
        for key, value in kwargs.items():
            setattr(self, key, value)

    def start(self):
        """Start the filter to consume messages from the queue."""
        self.queue = RabbitMQ(self.exchange, self.queue_name, self.routing_key, self.type)
        self.queue.consume(self.callback)

    def callback(ch, method, properties, body):
        """Callback function to process messages."""
        logging.info(f"Received message: {body.decode()}")

    def filter_by(data):
        pass