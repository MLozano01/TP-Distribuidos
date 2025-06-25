from protocol.rabbit_protocol import RabbitMQ
from common.aux import parse_filter_funct
import logging
from protocol.protocol import Protocol
from queue import Empty
from multiprocessing import Process, Value, Event
from .publisher.publisher import Publisher
import signal

logging.getLogger("pika").setLevel(logging.ERROR)
logging.getLogger("RabbitMQ").setLevel(logging.ERROR)

START = False
DONE = True

class Filter:
    def __init__(self, publisher: Publisher, **kwargs):
        self.protocol = Protocol()
        self.queue_rcv = None
        self.publisher = publisher

        for key, value in kwargs.items():
            setattr(self, key, value)

        self.stop_event = Event()

        # Setup signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def update_actual_client_id_status(self, client_id, status): 
        self.actual_client_id.value = client_id
        self.actual_status.value = status

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type)
        logging.info(f"Ready receiving queue with Exchange: {self.exchange_rcv}, Name: {self.queue_rcv_name}, Key: {self.routing_rcv_key}")
        self.publisher.setup_queues()

    def run(self):
        """Start the filter to consume messages from the queue."""
        self._settle_queues()
        if not self.queue_rcv: # Check if receiver queue settled
            logging.error("Receiver queue not initialized. Filter cannot run.")
            return
        
        try:
            self.queue_rcv.consume(callback_func=self.callback, stop_event=self.stop_event)
            self._close_publishers()
            logging.info(f"Filter done Consuming and publishers closed")
        except Exception as e:
            logging.error(f"Error in filter consumption: {e}")
            self._close_publishers()

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""

        logging.debug(f"Received message, with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)

        if decoded_msg.finished:
            self.publisher.publish_finished_signal(decoded_msg)
            return

        self.filter(decoded_msg)

    def filter(self, decoded_msg):
        try:
            result = parse_filter_funct(decoded_msg, self.filter_by)
            self.publisher.publish(result, decoded_msg.client_id, decoded_msg.secuence_number)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Do a raise for nack ? 
            return

    def _close_publishers(self):
        """Close publish channels of the filter."""
        if self.publisher:
            self.publisher.close()

    def _handle_shutdown(self, _sig, _frame):
        logging.info("Graceful exit")
        self.stop()

    def stop(self):
        """End the filter and close the queue."""
        logging.info("Stopping filter")
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        logging.info("Filter Stopped")