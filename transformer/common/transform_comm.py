from protocol.rabbit_protocol import RabbitMQ
import logging
from multiprocessing import Process
from protocol.protocol import Protocol
import time


logging.getLogger("pika").setLevel(logging.ERROR)



class TransformerCommunicator:
    """
    This class is responsible for communicating between different transformer components.
    It handles the sending and receiving of messages and data between transformers.
    """

    def __init__(self, config, queue, transformer):
        self.comm_queue = queue
        self.config = config
        self.queue_communication = None
        self.protocol = Protocol()
        self.transformers_acked = 0
        self.transformer = transformer


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
        logging.info(f"Initialized communication queues")

    def run(self):
        """
        Register a transformer instance with a unique name.
        """
        self._settle_queues()

        gets_the_finished = Process(target=self.manage_getting_finished, args=())

        gets_the_finished_notification = Process(target=self.manage_getting_finished_notification, args=())

        gets_the_finished.start()
        gets_the_finished_notification.start()

        gets_the_finished.join()
        gets_the_finished_notification.join()
        

    def manage_getting_finished(self):
        """
        Manage the inner communication between transformers.
        """
        try:

            data = self.comm_queue.get()

            logging.info(f"Received finished signal from transformer")

            consume_process = Process(target=self._manage_consume_pika, args=())
            consume_process.start()

            time.sleep(0.5)  # Ensure the consumer is ready before publishing

            self.queue_communication_1.publish(data)
            consume_process.join()

            
            logging.info("Finished acking the other transformers")

        except Exception as e:
            logging.error(f"Error in managing inner communication: {e}")
            self.comm_queue.put(False)

    def _manage_consume_pika(self):
        self.queue_communication_2.consume(self.other_callback)
        logging.info("Finished acking the other transformers")


    def manage_getting_finished_notification(self):
        """
        Manage the communication between different transformers.
        """
        try: 
            self.queue_communication_1.consume(self.callback)
        except Exception as e:
            logging.error(f"Error in managing inner communication: {e}")

    def callback(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        logging.info(f"Received message on communication channel with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)
        
        if decoded_msg.finished:
            logging.info("Received finished signal from other transformer!!.")
            self.transformer.process_pending()
            self.queue_communication_2.publish(body)
        return
    
    def other_callback(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        logging.info("RECEIVED A TRANSFORMER ACK")
        self.transformers_acked += 1
        if self.transformers_acked == self.config["transformer_replicas_count"]:
            logging.info("All transformers acked")
            self.comm_queue.put(True)
        else:
            logging.info(f"Transformer {self.transformers_acked} acked")
