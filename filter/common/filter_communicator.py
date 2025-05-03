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
        self.queue_communication = RabbitMQ(self.exchange_communication, self.queue_communication_name, self.routing_communication_key, self.exc_communication_type)
        logging.info(f"Initialized communication queue: {self.queue_communication.exchange}, type={self.queue_communication.exc_type}")

    def run(self):
        """
        Register a filter instance with a unique name.
        """
        gets_the_finished = Process(target=self.manage_getting_finished, args=())

        gets_the_finished_notification = Process(target=self.manage_getting_finished_notification, args=())

        gets_the_finished.start()
        gets_the_finished_notification.start()

        gets_the_finished.join()
        gets_the_finished_notification.join()
        


    def manage_getting_finished(self):
        """
        Manage the inner communication between filters.
        """
        try:
            data = self.comm_queue.get()

            self.queue_communication.publish(data)

            for i in range(self.config["filter_replicas_count"]):
                self.queue_communication.consume(self.other_callback, "ack_finished")

            self.comm_queue.put(True)

        except Exception as e:
            logging.error(f"Error in managing inner communication: {e}")
            self.comm_queue.put(False)


    def manage_getting_finished_notification(self):
        """
        Manage the communication between different filters.
        """
        try: 
            self.queue_communication.consume(self.callback)
        except Exception as e:
            logging.error(f"Error in managing inner communication: {e}")

    def callback(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        logging.info(f"Received message on communication channel with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)
        
        if decoded_msg.finished:
            logging.info("Received finished signal from server on communication channel.")
            msg = self.protocol.encode_movies_msg(decoded_msg)
            self.queue_communication.publish(msg, routing_key="ack_finished")

        return
    
    def other_callback(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        logging.info("RECEIVED A FILTER ACK")
