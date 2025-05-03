from protocol.rabbit_protocol import RabbitMQ
import logging
from multiprocessing import Process



class FilterCommunicator:
    """
    This class is responsible for communicating between different filter components.
    It handles the sending and receiving of messages and data between filters.
    """

    def __init__(self, config, queue):
        self.comm_queue = queue
        self.config = config
        self.filters = {}
        self.queue_communication = None

    def _settle_queues(self):
        """
        Initialize the communication queues based on the configuration.
        """
        self.queue_communication = self.queue_rcv = RabbitMQ(self.exchange_communication, self.queue_communication_name, self.routing_communication_key, self.exc_communication_type)
        logging.info(f"Initialized communication queue: {self.queue_communication.exchange}, type={self.queue_communication.exc_type}")

    def run(self, filter_name, filter_instance):
        """
        Register a filter instance with a unique name.
        """
        

    def manage_inner_communication(self):
        """
        Manage the inner communication between filters.
        """

        if not self.comm_queue:
            logging.error("Communication queue not initialized. Filter cannot run.")
            return
        
        if self.comm_queue:
            self.queue_communication.publish()


    def manage_filter_communication(self):
        """
        Manage the communication between different filters.
        """

        if not self.queue_rcv:
            logging.error("Receiver queue not initialized. Filter cannot run.")
            return

        for i in range(self.config["filter_replicas_count"]):
            self.queue_rcv.consume(self.callback)

    def callback(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        logging.info(f"Received message on communication channel with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)
        
        if decoded_msg.finished:
            logging.info("Received finished signal from server on communication channel.")
            self._publish_movie_finished_signal(decoded_msg)
            return
        self.filter(decoded_msg)