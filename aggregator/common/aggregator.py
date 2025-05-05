
from common.aux import parse_aggregate_func
from protocol.rabbit_protocol import RabbitMQ
import logging
from protocol.protocol import Protocol

logging.getLogger("pika").setLevel(logging.ERROR)
logging.getLogger("RabbitMQ").setLevel(logging.ERROR)
class Aggregator:
    def __init__(self, comm_queue, **kwargs):
        self.queue_rcv = None
        self.queue_snd = None
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.is_alive = True
        self.protocol = Protocol()
        self.current_finished_msg_amount = 0
        self.comm_queue = comm_queue

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type)
        self.queue_snd = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
        
    def run(self):
        """Start the aggregator to consume messages from the queue."""
        self._settle_queues()
        self.queue_rcv.consume(self.callback)
        self._check_finished()

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.debug(f"Received message, with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)
        if decoded_msg.finished:
            self.current_finished_msg_amount += 1
            logging.info(f"Current finished msg amount: {self.current_finished_msg_amount}/{self.expected_finished_msg_amount}")
            if self.current_finished_msg_amount >= self.expected_finished_msg_amount:
                self.publish_finished_msg(decoded_msg)
            return 
        self.aggregate(decoded_msg)

    def aggregate(self, decoded_msg):
        try:
            result = parse_aggregate_func(decoded_msg, self.key, self.field, self.operations, self.file_name)
            logging.info(f"Result: {result}")
            self.queue_snd.publish(self.protocol.create_aggr_batch(result))

        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return
    
    def publish_finished_msg(self, decoded_msg):
        msg = decoded_msg.SerializeToString()
        self.comm_queue.put(msg)
        logging.info(f"Published finished signal to communication channel, here the encoded message: {msg}")

        if self.comm_queue.get() == True:
            logging.info("Received SEND finished signal from communication channel.")
           
            self.queue_snd.publish(msg)
            logging.info(f"Published finished signal to {self.queue_snd.exchange}")

        logging.info("FINISHED SENDING THE FINISH MESSAGE")

    def _check_finished(self):
        if self.comm_queue.get_nowait():
            self.comm_queue.put(True)

    def end_aggregator(self):
        """End the aggregator and close the queue."""
        if self.queue_rcv:
            self.queue_rcv.close_channel()
        if self.queue_snd:
            self.queue_snd.close_channel()
        self.is_alive = False
        logging.info("Aggregator Stopped")