from protocol.rabbit_protocol import RabbitMQ
import logging
from multiprocessing import Process
from protocol.protocol import Protocol
import time
from threading import Event

logging.getLogger("pika").setLevel(logging.ERROR)



class FilterCommunicator:
    """
    This class is responsible for communicating between different filter components.
    It handles the sending and receiving of messages and data between filters.
    """

    def __init__(self, config, finish_receive_ntc, finish_notify_ntc, finish_receive_ctn, finish_notify_ctn, stop_event):
        self.finish_receive_ntc = finish_receive_ntc
        self.finish_notify_ntc = finish_notify_ntc
        self.finish_receive_ctn = finish_receive_ctn
        self.finish_notify_ctn = finish_notify_ctn
        self.stop_event = stop_event
        self.config = config
        self.protocol = Protocol()
        self.filters_acked = {}
        self.continue_running = True


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
        # Initialize publisher in this process
        name_one = self.config["queue_communication_name"] + "_1"
        key_one = self.config["routing_communication_key"] + "_1"
        queue_communication_1 = RabbitMQ(self.config["exchange_communication"], name_one, key_one, self.config["exc_communication_type"])

        consume_process = Process(target=self._manage_consume_pika, args=())
        consume_process.start()

        while not self.stop_event.is_set():
            try:
                data = self.finish_receive_ntc.get()
                
                if data == "STOP_EVENT":
                    logging.info("Received STOP_EVENT in manage_getting_finished")
                    break

                logging.info(f"Received finished signal from filter")
                time.sleep(0.5)
                queue_communication_1.publish(data)
                logging.info("Finished acking the other filters")

            except Exception as e:
                logging.error(f"Error in managing inner communication: {e}")
                self.comm_queue.put(False)

        consume_process.join()
        queue_communication_1.close_channel()

    def _manage_consume_pika(self):
        """
        Manage consuming messages from RabbitMQ.
        """
        try:
            name_two = self.config["queue_communication_name"] + "_2"
            key_two = self.config["routing_communication_key"] + "_2"
            consumer_queue = RabbitMQ(self.config["exchange_communication"], name_two, key_two, self.config["exc_communication_type"])
            consumer_queue.consume(self.other_callback, stop_event=self.stop_event)
            logging.info("Finished acking the other filters")
        except Exception as e:
            logging.error(f"Error in consumer process: {e}")

    def manage_getting_finished_notification(self):
        """
        Manage the communication between different filters.
        """
        try:
            name_one = self.config["queue_communication_name"] + "_1"
            key_one = self.config["routing_communication_key"] + "_1"
            consumer_queue = RabbitMQ(self.config["exchange_communication"], name_one, key_one, self.config["exc_communication_type"])
            consumer_queue.consume(self.callback, stop_event=self.stop_event)
        except Exception as e:
            logging.error(f"Error in managing inner communication: {e}")

    def callback(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        if self.stop_event.is_set():
            return

        logging.info(f"Received message on communication channel with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)
        
        if decoded_msg.finished:
            logging.info("Received finished signal from other filter!!.")
            self.finish_notify_ctn.put([decoded_msg.client_id, False])
            
            done_with_client = self.finish_notify_ntc.get()
            if done_with_client == "STOP_EVENT":
                logging.info("Received STOP_EVENT in callback")
                return

            logging.info(f"{done_with_client}")
            if done_with_client[1]:
                logging.info("Filter was done with the client!!.")
                name_two = self.config["queue_communication_name"] + "_2"
                key_two = self.config["routing_communication_key"] + "_2"
                queue_communication_2 = RabbitMQ(self.config["exchange_communication"], name_two, key_two, self.config["exc_communication_type"])
                queue_communication_2.publish(done_with_client[0].to_bytes(2, byteorder='big'))
                queue_communication_2.close_channel()
            else:
                raise Exception("Failed to send finished signal to other filter")
        return
    
    def other_callback(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        if self.stop_event.is_set():
            return

        logging.info("RECEIVED A FILTER ACK")
        client_id = int.from_bytes(body, byteorder='big')
        logging.info(f"Received ack from the client: {client_id}")
        
        self.filters_acked.setdefault(client_id, 0)
        self.filters_acked[client_id] += 1

        if self.filters_acked[client_id] == self.config["filter_replicas_count"]:
            logging.info("All filters acked")
            self.finish_receive_ctn.put(True)
        else:
            logging.info(f"Filter {self.filters_acked} acked")
