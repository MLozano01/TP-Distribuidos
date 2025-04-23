
from protocol.rabbit_protocol import RabbitMQ
from common.aux import parse_reduce_funct
import logging
import json
from protocol.protocol import Protocol


class Reducer:
    def __init__(self, **kwargs):
        self.queue_rcv = None
        self.queue_snd = None
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.is_alive = True
        self.partial_result = {}

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type)
        self.queue_snd = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
        
    def run(self):
        """Start the reduce to consume messages from the queue."""
        self._settle_queues()
        self.queue_rcv.consume(self.callback)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.info(f"Received message, with routing key: {method.routing_key}")
        self.reducer(body)

    def reducer(self, data):
        try:
            protocol = Protocol()

            self.partial_result = parse_reduce_funct(protocol.decode_aggr_batch(data), self.reduce_by, self.partial_result)

            logging.info(f"Partial result: {self.partial_result}")

            smth = parse_reduce_funct("", "calc_avg", self.partial_result)

            logging.info(f"Partial Average: {smth}")

            if smth:
                self.queue_snd.publish(protocol.create_movie_list(smth))
            else:
                logging.info(f"No  matched the reduce criteria.")

        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}")
            return
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return

    def end_reduce(self):
        """End the reduce and close the queue."""
        if self.queue_rcv:
            self.queue_rcv.close_channel()
        if self.queue_snd:
            self.queue_snd.close_channel()
        self.is_alive = False
        logging.info("reduce Stopped")