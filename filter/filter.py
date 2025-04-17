
import pika
from protocol.protocol import RabbitMQ
import logging

class Filter:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def start(self):
        """Start the filter to consume messages from the queue."""


    def callback(ch, method, properties, body):
        """Callback function to process messages."""
        print(f"Received message: {body.decode()}")


    def filter_by(data):
        pass