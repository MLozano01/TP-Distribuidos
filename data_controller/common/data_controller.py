import logging
import signal
from protocol import files_pb2
from protocol.protocol import Protocol, FileType
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.parsing_proto_utils import is_date
from .aux import filter_movies, filter_ratings, filter_credits
from queue import Empty
from multiprocessing import Process, Value

START = False
DONE = True

# Configure logging to reduce noise
logging.getLogger('pika').setLevel(logging.WARNING)
logging.getLogger('RabbitMQ').setLevel(logging.WARNING)

class DataController:
    def __init__(self, **kwargs):
        self.is_alive = True
        self.protocol = Protocol()
        self.work_consumer = None
        self.send_queue = None
        self.replica_id = kwargs.get('replica_id', 'unknown')

        # Set attributes from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)
        
        self.columns_needed = {
            'movies': ["id", "title", "genres", "release_date", "overview", 
                      "production_countries", "spoken_languages", "budget", "revenue"],
            'ratings': ["movieId", "rating", "timestamp"],
            'credits': ["id", "cast"]
        }
        
        # Setup signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        

    def _settle_queues(self):
        self.work_consumer = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type, auto_ack=False, prefetch_count=1)

        self.send_queue = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
        logging.info("Queues up, ready to use")


    def run(self):
        """Start the DataController with message consumption"""
        self._settle_queues()
        self.work_consumer.consume(self.callback)


    def callback(self, ch, method, properties, body):
        message_type, message = self.protocol.decode_client_msg(body, self.columns_needed)
        if not message_type or not message:
            logging.warning("Received invalid message from server")
            return
        
        if message.finished:
            self._handle_finished_message(message_type, message)
            return

        self._handle_data_message(message_type, message)


    def _handle_finished_message(self, message_type, msg):
        """Handle finished messages with coordination"""
        logging.info(f"Propagating finished {message_type} of client {msg.client_id} downstream")
        msg_to_send = msg.SerializeToString()
        self.send_queue.publish(msg_to_send)
        

    def _handle_data_message(self, message_type, msg):
        logging.info(f"got message of type: {message_type}")
        if message_type == FileType.MOVIES:
            self.publish_movies(msg)
        elif message_type == FileType.RATINGS:
            self.publish_ratings(msg)
        elif message_type == FileType.CREDITS:
            self.publish_credits(msg)

    def publish_movies(self, movies_csv):
        movies_pb = filter_movies(movies_csv)
        if movies_pb:
            self.send_queue.publish(movies_pb.SerializeToString())

    def publish_ratings(self, ratings_csv):
        ratings_batch = filter_ratings(ratings_csv)
        self.send_queue.publish(ratings_batch.SerializeToString(), routing_key=str(ratings_batch.client_id))

    def publish_credits(self, credits_csv):
        credits_batch = filter_credits(credits_csv)
        self.send_queue.publish(credits_batch.SerializeToString(), routing_key=str(credits_batch.client_id))

    def stop(self):
        """Stop the DataController and close all connections"""
        if not self.is_alive:
            logging.info(f"Already stopped DataController {self.replica_id}")
            return
        
        logging.info(f"Stopping DataController {self.replica_id}...")
        self.is_alive = False
        # Close work consumer connection
        if self.work_consumer:
            try:
                self.work_consumer.close_channel()
                logging.info("Work consumer channel closed")
            except Exception as e:
                logging.error(f"Error closing work consumer channel: {e}")
        
        # Close movies publisher connection
        if self.send_queue:
            try:
                self.send_queue.close_channel()
                logging.info("Publisher channel closed")
            except Exception as e:
                logging.error(f"Error closing publisher channel: {e}")
        
        # Terminate finish signal checkers
        if self.finish_signal_checker:
            self.finish_signal_checker.terminate()
            self.finish_signal_checker.join()
            logging.info(f"Finished signal checker process terminated.")
        
        logging.info(f"DataController {self.replica_id} stopped successfully")

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logging.info(f"DataController received signal {signum}. Shutting down gracefully...")
        self.stop()
        logging.info("DataController shutdown complete.")