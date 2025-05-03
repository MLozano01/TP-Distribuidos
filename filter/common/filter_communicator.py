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
        self.queue_communication = None

    def _settle_queues(self):
        """
        Initialize the communication queues based on the configuration.
        """
        self.queue_communication = RabbitMQ(self.config["exchange_communication"], self.config['queue_communication_name'], self.config["routing_communication_key"], self.config["exc_communication_type"])
        logging.info(f"Initialized communication queue")

    def run(self):
        """
        Register a filter instance with a unique name.
        """
        self._settle_queues()

        gets_the_finished = Process(target=self.manage_getting_finished, args=())

        gets_the_finished_notification = Process(target=self.manage_getting_finished_notification, args=())

        gets_the_finished.start()
        logging.info(f"GETS_THE_FINISHED: {gets_the_finished.pid}")
        gets_the_finished_notification.start()
        logging.info(f"GETS_THE_FINISHED_NOTIFICATION: {gets_the_finished_notification.pid}")

        gets_the_finished.join()
        gets_the_finished_notification.join()
        

    def manage_getting_finished(self):
        """
        Manage the inner communication between filters.
        """
        try:
            data = self.comm_queue.get()

            logging.info(f"Received finished signal from filter: {data}")

            self.queue_communication.publish(data)

            for i in range(self.config["filter_replicas_count"]):
                self.queue_communication.consume(self.other_callback, "ack_finished")

            logging.info("Finished acking the other filters")

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
