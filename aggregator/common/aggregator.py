from common.aux import parse_aggregate_func
from protocol.rabbit_protocol import RabbitMQ
import logging
from protocol.protocol import Protocol
from queue import Empty
from multiprocessing import Process, Value

START = False
DONE = True

logging.getLogger("pika").setLevel(logging.ERROR)
logging.getLogger("RabbitMQ").setLevel(logging.ERROR)
class Aggregator:
    def __init__(self, finish_notify_ntc, finish_notify_ctn, communicator_instance, **kwargs):
        self.queue_rcv = None
        self.queue_snd = None

        for key, value in kwargs.items():
            setattr(self, key, value)

        self.is_alive = True
        self.protocol = Protocol()
         
        self.finish_notify_ntc = finish_notify_ntc
        self.finish_notify_ctn = finish_notify_ctn

        self.finish_signal_checker = None
        self.actual_client_id = Value('i', 0)
        self.actual_status = Value('b', True)

        self.comm_instance = communicator_instance

    def update_actual_client_id_status(self, client_id, status): 
        self.actual_client_id.value = client_id
        self.actual_status.value = status

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
        logging.info(f"Received message, with routing key: {method.routing_key}")
        
        if getattr(self, 'file_name', '') == 'joined_ratings':
            decoded_msg = self.protocol.decode_joined_ratings_batch(body)
            logging.info(f"Received rating: {decoded_msg}")
        else:
            decoded_msg = self.protocol.decode_movies_msg(body)

        self.update_actual_client_id_status(decoded_msg.client_id, START)
        if decoded_msg.finished:
            self.update_actual_client_id_status(decoded_msg.client_id, DONE)
            self.publish_finished_msg(decoded_msg)
            return 
        self.aggregate(decoded_msg)

    def aggregate(self, decoded_msg):
        try:
            result = parse_aggregate_func(decoded_msg, self.key, self.field, self.operations, self.file_name)
            logging.info(f"Aggregation result: {result}")
            self.queue_snd.publish(self.protocol.create_aggr_batch(result, decoded_msg.client_id, decoded_msg.secuence_number))
            self.update_actual_client_id_status(decoded_msg.client_id, DONE)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return
    
    def publish_finished_msg(self, decoded_msg):
        """Publishes an AggregationBatch with finished=True to downstream consumers."""
        # Build an empty AggregationBatch finished message
        finished_batch = self.protocol.create_aggr_batch({}, decoded_msg.client_id, decoded_msg.secuence_number)
        # Mark finished flag using protobuf (since create_aggr_batch doesn't set it)
        aggr_pb = self.protocol.decode_aggr_batch(finished_batch)
        aggr_pb.finished = True
        finished_serialized = aggr_pb.SerializeToString()

        logging.info("Published finished signal to communication channel, here the encoded message: %s", finished_serialized)
        self.comm_instance.start_token_ring(decoded_msg.client_id)
        self.comm_instance.wait_eof_confirmation()
        logging.info("Received SEND finished signal from communication channel.")

        self.queue_snd.publish(finished_serialized)
        logging.info(f"Published finished signal to {self.queue_snd.exchange}")

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
        client_id = self.actual_client_id.value
        status = self.actual_status.value

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