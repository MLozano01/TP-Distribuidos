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
    def __init__(self, **kwargs):
        self.queue_rcv = None
        self.queue_snd = None

        for key, value in kwargs.items():
            setattr(self, key, value)

        self.is_alive = True
        self.protocol = Protocol()

    def update_actual_client_id_status(self, client_id, status): 
        self.actual_client_id.value = client_id
        self.actual_status.value = status

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type)
        self.queue_snd = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
        
    def run(self):
        """Start the aggregator to consume messages from the queue."""
        self._settle_queues()

        self.queue_rcv.consume(self.callback)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.info(f"Received message, with routing key: {method.routing_key}")
        
        if getattr(self, 'file_name', '') == 'joined_ratings':
            decoded_msg = self.protocol.decode_joined_ratings_batch(body)
            logging.info(f"Received rating: {decoded_msg}")
        else:
            decoded_msg = self.protocol.decode_movies_msg(body)

        if decoded_msg.finished:
            self.publish_finished_msg(decoded_msg)
            return

        self.aggregate(decoded_msg)

    def aggregate(self, decoded_msg):
        try:
            result = parse_aggregate_func(decoded_msg, self.key, self.field, self.operations, self.file_name)
            logging.info(f"Aggregation result: {result}")
            self.queue_snd.publish(
                self.protocol.create_aggr_batch(
                    result,
                    decoded_msg.client_id,
                    decoded_msg.secuence_number,
                )
            )
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return
        
    def publish_finished_msg(self, decoded_msg):
        """Publishes an AggregationBatch with finished=True to downstream consumers."""
        aggr_pb = self.protocol.decode_aggr_batch(self.protocol.create_aggr_batch({}, decoded_msg.client_id, decoded_msg.secuence_number))
        aggr_pb.finished = True
        if decoded_msg.force_finish:
            aggr_pb.force_finish = decoded_msg.force_finish
        
        finished_serialized = aggr_pb.SerializeToString()
        
        self.queue_snd.publish(finished_serialized)
        logging.info(f"Published finished signal to {self.queue_snd.exchange}")
        
    def stop(self):
        """End the aggregator and close the queue."""
        if self.queue_rcv:
            self.queue_rcv.close_channel()
        if self.queue_snd:
            self.queue_snd.close_channel()
        self.is_alive = False
        logging.info("Aggregator Stopped")