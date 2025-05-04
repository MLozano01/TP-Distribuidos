from protocol.rabbit_protocol import RabbitMQ
import logging
from multiprocessing import Process
from protocol.protocol import Protocol
import time

logging.getLogger("pika").setLevel(logging.ERROR)

logger = logging.getLogger("DataControllerCommunicator")

class DataControllerCommunicator:
    """
    This class is responsible for communicating between different data controller components.
    It handles the sending and receiving of messages and data between data controllers.
    """

    def __init__(self, config, queue, type):
        self.comm_queue = queue
        self.config = config
        self.queue_communication = None
        self.protocol = Protocol()
        self.data_controllers_acked = 0
        self.type = type

        logger.info(f"Initialized {self.type} communicator with: {self.config['data_controller_replicas_count']} replicas count")


    def _settle_queues(self):
        """
        Initialize the communication queues based on the configuration.
        """

        name_one = self.config["queue_communication_name"] + "_1"
        name_two = self.config["queue_communication_name"] + "_2"

        key_one = self.config["routing_communication_key"] + "_1"
        key_two = self.config["routing_communication_key"] + "_2"

        self.queue_communication_1 = RabbitMQ(self.config["exchange_communication"], name_one, key_one, self.config["exc_communication_type"])
        self.queue_communication_2 = RabbitMQ(self.config["exchange_communication"], name_two, key_two, self.config["exc_communication_type"])
        logger.debug(f"Initialized communication queues")


    def run(self):
        """
        Register a data controller instance with a unique name.
        """
        self._settle_queues()

        gets_the_finished = Process(target=self.propagate_upstream_finished_to_replicas, args=())
        gets_the_finished_notification = Process(target=self.handle_replicas_finished, args=())

        gets_the_finished.start()
        gets_the_finished_notification.start()

        gets_the_finished.join()
        gets_the_finished_notification.join()
        

    def propagate_upstream_finished_to_replicas(self):
        """
        Manage the inner communication between data controllers.
        """
        try:

            data = self.comm_queue.get()

            logger.info(f"Received {self.type} finished signal from upstream")

            consume_process = Process(target=self._await_replicas_ack, args=())
            consume_process.start()

            time.sleep(0.5)  # Ensure the consumer is ready before publishing

            self.queue_communication_1.publish(data)
            consume_process.join()
            
            logger.info(f"All {self.type} acks of the other replicas received")

        except Exception as e:
            logger.error(f"Error in managing inner communication: {e}")
            self.comm_queue.put(False)

    def _await_replicas_ack(self):
        consumer_queue = RabbitMQ(self.config["exchange_communication"], self.config["queue_communication_name"] + "_2", self.config["routing_communication_key"] + "_2", self.config["exc_communication_type"])
        consumer_queue.consume(self._handle_replica_ack)


    def handle_replicas_finished(self):
        """
        Manage the communication between different data controllers.
        """
        try: 
            consumer_queue = RabbitMQ(self.config["exchange_communication"], self.config["queue_communication_name"] + "_1", self.config["routing_communication_key"] + "_1", self.config["exc_communication_type"])
            consumer_queue.consume(self._on_finished_from_replica_received)
        except Exception as e:
            logger.error(f"Error in managing inner communication: {e}")

    def _on_finished_from_replica_received(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        logger.debug(f"Received message on communication channel with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)
        
        if decoded_msg.finished:
            logger.info(f"Received {self.type} finished from replica data controller. Answering ACK")
            self.queue_communication_2.publish(body)
        return
    
    def _handle_replica_ack(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        logger.debug("RECEIVED A DATA CONTROLLER ACK")
        self.data_controllers_acked += 1
        if self.data_controllers_acked == self.config["data_controller_replicas_count"] - 1:
            logger.info(f"All data controllers acked {self.type}")
            self.comm_queue.put(True)
        else:
            logger.info(f"Data controller {self.type} ack number: {self.data_controllers_acked} received")