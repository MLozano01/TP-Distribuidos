import logging
import signal
from protocol import files_pb2
from protocol.protocol import Protocol, FileType
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.parsing_proto_utils import is_date
from .aux import filter_movies, filter_ratings, filter_credits
from queue import Empty
from multiprocessing import Process, Value
import os

START = False
DONE = True

# Configure logging to reduce noise
logging.getLogger('pika').setLevel(logging.WARNING)
logging.getLogger('RabbitMQ').setLevel(logging.WARNING)

class DataController:
    def __init__(
            self,
            finish_notify_ntc,
            finish_notify_ctn,
            comm_instance,
            **kwargs
    ):
        self.is_alive = True
        self.protocol = Protocol()
        self.work_consumer = None
        self.send_queue = None
        self.replica_id = kwargs.get('replica_id', 'unknown')

        # communication
        self.finish_notify_ntc = finish_notify_ntc
        self.finish_notify_ctn = finish_notify_ctn
        self.actual_client_id = Value('i', 0)
        self.actual_status = Value('b', True)

        self.comm_instance = comm_instance
        self.finish_signal_checker = None
        
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

    def update_actual_client_id_status(self, client_id, status): 
        self.actual_client_id.value = client_id
        self.actual_status.value = status

    def _settle_queues(self):
        self.work_consumer = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type, auto_ack=False, prefetch_count=1)

        self.send_queue = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
        logging.info("Queues up, ready to use")

    def run(self):
        """Start the DataController with message consumption"""
        self._settle_queues()
        
        # Create separate processes for each type of check_finished
        self.finish_signal_checker = Process(target=self.check_finished, args=())
        
        self.finish_signal_checker.start()

        self.work_consumer.consume(self.callback)

        self.finish_signal_checker.join()

    def callback(self, ch, method, properties, body):
        message_type, message = self.protocol.decode_client_msg(body, self.columns_needed)
        if not message_type or not message:
            logging.warning("Received invalid message from server")
            return
        self.update_actual_client_id_status(message.client_id, START)
        if message.finished:
            self.update_actual_client_id_status(message.client_id, DONE)
            self._handle_finished_message(message_type, message)
        else:
            self._handle_data_message(message_type, message)


    def _handle_finished_message(self, message_type, msg):
        """Handle finished messages with coordination"""
        client_id = msg.client_id
        logging.info(f"Received {message_type.name} finished signal from server for client {client_id}")
        msg_to_send = msg.SerializeToString()
        self.comm_instance.start_token_ring(msg.client_id)
        
        self.comm_instance.wait_eof_confirmation()
        logging.info(f"Propagating finished {message_type} of client {client_id} downstream")
        self.send_queue.publish(msg_to_send)
        

    def _handle_data_message(self, message_type, msg):
        logging.info(f"got message of type: {message_type}")
        if message_type == FileType.MOVIES:
            self.publish_movies(msg)
        elif message_type == FileType.RATINGS:
            self.publish_ratings(msg)
        elif message_type == FileType.CREDITS:
            self.publish_credits(msg)
        self.update_actual_client_id_status(msg.client_id, DONE)

    def publish_movies(self, movies_csv):
        movies_pb = filter_movies(movies_csv)
        if movies_pb:
            logging.info(f"SECUENCE NUMBER: {movies_pb.secuence_number}")
            self.send_queue.publish(movies_pb.SerializeToString())

    def publish_ratings(self, ratings_csv):
        ratings_batch = filter_ratings(ratings_csv)
        # Route by client_id as before (one client→one joiner)
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

    def check_finished(self):
        """Check for finished signals for a specific type of message"""
        while self.is_alive:
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

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logging.info(f"DataController received signal {signum}. Shutting down gracefully...")
        self.stop()
        logging.info("DataController shutdown complete.")