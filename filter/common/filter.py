from protocol.rabbit_protocol import RabbitMQ
from common.aux import parse_filter_funct
import logging
from protocol.protocol import Protocol
from queue import Empty
from multiprocessing import Process, Value
from .publisher.publisher import Publisher

logging.getLogger("pika").setLevel(logging.ERROR)
logging.getLogger("RabbitMQ").setLevel(logging.ERROR)

START = False
DONE = True

class Filter:
    def __init__(self, finish_notify_ntc, finish_notify_ctn, stop_event, comm_instance, publisher: Publisher, **kwargs):
        self.protocol = Protocol()
        self.queue_rcv = None
        self.publisher = publisher

        self.finished_filter_arg_step_publisher = None
        self.finish_notify_ntc = finish_notify_ntc
        self.finish_notify_ctn = finish_notify_ctn

        self.actual_client_id = Value('i', 0)
        self.actual_status = Value('b', True)
        
        self.finish_signal_checker = None
        self.stop_event = stop_event

        self.comm_instance = comm_instance
        for key, value in kwargs.items():
            setattr(self, key, value)

    def update_actual_client_id_status(self, client_id, status): 
        self.actual_client_id.value = client_id
        self.actual_status.value = status

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type)
        self.publisher.setup_queues()

    def run(self):
        """Start the filter to consume messages from the queue."""
        self._settle_queues()
        if not self.queue_rcv: # Check if receiver queue settled
            logging.error("Receiver queue not initialized. Filter cannot run.")
            return
        
        self.finish_signal_checker = Process(target=self.check_finished, args=())
        self.finish_signal_checker.start()
        
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
            
        self.update_actual_client_id_status(decoded_msg.client_id, START)
        if decoded_msg.finished:
            logging.info("Received MOVIES finished signal from server on data channel.")
            self.update_actual_client_id_status(decoded_msg.client_id, DONE)
            self._publish_movie_finished_signal(decoded_msg)
            return
        self.filter(decoded_msg)

    def filter(self, decoded_msg):
        try:
            result = parse_filter_funct(decoded_msg, self.filter_by)
            client_id = decoded_msg.client_id
            if result:
                self.publisher.publish(result, client_id, decoded_msg.secuence_number)
            else:
                logging.info(f"No matched the filter criteria.")
            logging.info(f"sending status")
            self.update_actual_client_id_status(decoded_msg.client_id, DONE)

        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return

    def _publish_movie_finished_signal(self, msg):
        """Publishes the movie finished signal."""
        logging.info(f"Published finished signal to communication channel, here the encoded message: {msg}")
        self.comm_instance.start_token_ring(msg.client_id)
        
        self.comm_instance.wait_eof_confirmation()
        logging.info("Received SEND finished signal from communication channel.")
        self.publisher.publish_finished_signal(msg)

    def check_finished(self):
        while not self.stop_event.is_set():
            try:
                msg = self.finish_notify_ctn.get()
                if msg == "STOP_EVENT":
                    logging.info(f"Filter check_finished process end")
                    break

                logging.info(f"Received finished signal from control channel: {msg}")

                client_id, status = self.get_last()
                client_finished = msg[0]
                if client_finished == client_id:
                    self.finish_notify_ntc.put([client_finished, status])
                    logging.info(f"Received finished signal from control channel for client {client_finished}, with status {status}.")
                else:
                    self.finish_notify_ntc.put([client_finished, True])
                    logging.info(f"Received finished signal from control channel for client {client_finished}, but working on {client_id}.")
            except Empty:
                logging.info("No finished signal received yet.")
                pass
            except Exception as e:
                logging.error(f"Error in finished signal checker: {e}")
                break

    def get_last(self):
        client_id = self.actual_client_id.value
        status = self.actual_status.value

        logging.info(f"Last client ID: {client_id}, status: {status}")
        return client_id, status

    def _close_publishers(self):
        """Close publish channels of the filter."""
        if self.publisher:
            self.publisher.close()

    
