from protocol.rabbit_protocol import RabbitMQ
import logging
from multiprocessing import Process
from protocol.protocol import Protocol
import time
import threading
import signal

logging.getLogger("pika").setLevel(logging.ERROR)

logger = logging.getLogger("DataControllerCommunicator")

class DataControllerCommunicator:
    """
    This class is responsible for communicating between different data controller components.
    It handles the sending and receiving of messages and data between data controllers.
    """

    def __init__(self, config, finish_receive_ntc, finish_notify_ntc, finish_receive_ctn, finish_notify_ctn, type):
        self.finish_receive_ntc = finish_receive_ntc
        self.finish_notify_ntc = finish_notify_ntc
        self.finish_receive_ctn = finish_receive_ctn
        self.finish_notify_ctn = finish_notify_ctn
        self.config = config
        self.queue_communication = None
        self.protocol = Protocol()
        self.data_controllers_acked = {}
        self.type = type

        logger.info(f"Initialized {self.type} communicator with: {self.config['data_controller_replicas_count']} replicas count")
        
        # Setup signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        self.continue_running = True

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
        consume_process = Process(target=self._await_replicas_ack, args=())
        consume_process.start()
        while self.continue_running:
            try:
                data = self.finish_receive_ntc.get()
                logger.info(f"Received {self.type} finished signal from upstream")


                time.sleep(0.5)  # Ensure the consumer is ready before publishing

                self.queue_communication_1.publish(data)
                
                logger.info(f"All {self.type} acks of the other replicas received")

            except Exception as e:
                logger.error(f"Error in managing inner communication: {e}")
                self.finish_receive_ctn.put(False)
        consume_process.join()

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
        
        # Decode message based on type
        if self.type == "MOVIES":
            decoded_msg = self.protocol.decode_movies_msg(body)
        elif self.type == "RATINGS":
            decoded_msg = self.protocol.decode_ratings_msg(body)
        elif self.type == "CREDITS":
            decoded_msg = self.protocol.decode_credits_msg(body)
        else:
            logger.error(f"Unknown message type: {self.type}")
            return
        
        if decoded_msg.finished:
            logger.info(f"Received {self.type} finished from replica data controller.")
            self.finish_notify_ctn.put([decoded_msg.client_id, False])

            done_with_client = self.finish_notify_ntc.get()
            logging.info(f"{done_with_client}")
            if done_with_client[1]:
                logging.info(f"Data controller was done with the client {decoded_msg.client_id}")
                self.queue_communication_2.publish(done_with_client[0].to_bytes(2, byteorder='big'))
            else:
                raise Exception("Failed to send finished signal to other filter")
        
    
    def _handle_replica_ack(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        logger.debug("RECEIVED A DATA CONTROLLER ACK")
        client_id = int.from_bytes(body, byteorder='big')

        logging.info(f"Received ack from the client: {client_id}")

        self.data_controllers_acked.setdefault(client_id, 0)
        self.data_controllers_acked[client_id] += 1

        if self.data_controllers_acked[client_id] == self.config["data_controller_replicas_count"]:
            logging.info("All data_controller acked")
            self.finish_receive_ctn.put(True)


    def stop(self):
        """Stop the DataControllerCommunicator and close all connections"""
        logging.info(f"Stopping {self.type} DataControllerCommunicator...")
        self.continue_running = False
        # Close queue communication 1 connection
        if hasattr(self, 'queue_communication_1'):
            try:
                self.queue_communication_1.close_channel()
                logging.info(f"{self.type} queue communication 1 channel closed")
            except Exception as e:
                logging.error(f"Error closing {self.type} queue communication 1 channel: {e}")
        
        # Close queue communication 2 connection
        if hasattr(self, 'queue_communication_2'):
            try:
                self.queue_communication_2.close_channel()
                logging.info(f"{self.type} queue communication 2 channel closed")
            except Exception as e:
                logging.error(f"Error closing {self.type} queue communication 2 channel: {e}")
        
        logging.info(f"{self.type} DataControllerCommunicator stopped successfully")

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"{self.type} DataControllerCommunicator received signal {signum}. Shutting down gracefully...")
        self.stop()
        logger.info(f"{self.type} DataControllerCommunicator shutdown complete.")