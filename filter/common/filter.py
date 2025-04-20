
from protocol.rabbit_protocol import RabbitMQ
from common.aux import parse_filter_funct
import logging
import json


class Filter:
    def __init__(self, **kwargs):
        self.queue_rcv = None
        self.queue_snd = None
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type)
        self.queue_snd = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
        
    def start(self):
        """Start the filter to consume messages from the queue."""
        self._settle_queues()
        self.queue_rcv.consume(self.callback)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.info(f"Received message")
        self.filter(body)

    def filter(self, data):
        result = {}
        try:
            data = json.loads(data)

            result = parse_filter_funct(data, self.filter_by, self.file_name)
            
            if result:
                self.queue_snd.publish(json.dumps(result))
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
        logging.info("Filter Stopped")