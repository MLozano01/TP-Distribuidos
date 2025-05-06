
from common.aux import parse_aggregate_func
from protocol.rabbit_protocol import RabbitMQ
import logging
from protocol.protocol import Protocol
from queue import Empty
from multiprocessing import Process, Queue

START = False
DONE = True

logging.getLogger("pika").setLevel(logging.ERROR)
logging.getLogger("RabbitMQ").setLevel(logging.ERROR)
class Aggregator:
    def __init__(self, finish_receive_ntc, finish_notify_ntc, finish_receive_ctn, finish_notify_ctn, **kwargs):
        self.queue_rcv = None
        self.queue_snd = None

        for key, value in kwargs.items():
            setattr(self, key, value)

        self.is_alive = True
        self.protocol = Protocol()
         
        self.finish_receive_ntc = finish_receive_ntc
        self.finish_notify_ntc = finish_notify_ntc
        self.finish_receive_ctn = finish_receive_ctn
        self.finish_notify_ctn = finish_notify_ctn

        self.finish_signal_checker = None
        self.send_actual_client_id_status = Queue()

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type)
        self.queue_snd = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
        
    def run(self):
        """Start the aggregator to consume messages from the queue."""
        self._settle_queues()

        self.finish_signal_checker = Process(target=self.check_finished, args=())
        self.finish_signal_checker.start()

        self.queue_rcv.consume(self.callback)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.debug(f"Received message, with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)
        if decoded_msg.finished:
            self.publish_finished_msg(decoded_msg)
            return 
        self.send_actual_client_id_status.put([decoded_msg.client_id, START])
        self.aggregate(decoded_msg)

    def aggregate(self, decoded_msg):
        try:
            result = parse_aggregate_func(decoded_msg, self.key, self.field, self.operations, self.file_name)
            logging.info(f"Result: {result}")
            self.queue_snd.publish(self.protocol.create_aggr_batch(result))
            self.send_actual_client_id_status.put([decoded_msg.client_id, DONE])
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return
    
    def publish_finished_msg(self, decoded_msg):
        msg = decoded_msg.SerializeToString()
        self.finish_receive_ntc.put(msg)
        logging.info(f"Published finished signal to communication channel, here the encoded message: {msg}")

        if self.finish_receive_ctn.get() == True:
            logging.info("Received SEND finished signal from communication channel.")
           
            self.queue_snd.publish(msg)
            logging.info(f"Published finished signal to {self.queue_snd.exchange}")

        logging.info("FINISHED SENDING THE FINISH MESSAGE")

    def check_finished(self):
        while self.is_alive:
            try:
                msg = self.finish_notify_ctn.get()
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
        client_id = None
        status = None

        while not self.send_actual_client_id_status.empty():

            client_id, status = self.send_actual_client_id_status.get_nowait()

        logging.info(f"Last client ID: {client_id}, status: {status}")

        return client_id, status

    def end_aggregator(self):
        """End the aggregator and close the queue."""
        if self.queue_rcv:
            self.queue_rcv.close_channel()
        if self.queue_snd:
            self.queue_snd.close_channel()
        if self.finish_signal_checker:
            self.finish_signal_checker.terminate()
            self.finish_signal_checker.join()
            logging.info("Finished signal checker process terminated.")
        self.is_alive = False
        logging.info("Aggregator Stopped")