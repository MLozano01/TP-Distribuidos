import logging
import signal
from protocol import files_pb2
from protocol.protocol import Protocol, FileType
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.parsing_proto_utils import is_date
from .aux import filter_movies, filter_ratings, filter_credits
from queue import Empty
from multiprocessing import Process, Queue

START = False
DONE = True

# Configure logging to reduce noise
logging.getLogger('pika').setLevel(logging.WARNING)
logging.getLogger('RabbitMQ').setLevel(logging.WARNING)

class DataController:
    def __init__(
            self,
            movies_finish_receive_ntc,
            movies_finish_notify_ntc,
            movies_finish_receive_ctn,
            movies_finish_notify_ctn,
            credits_finish_receive_ntc,
            credits_finish_notify_ntc,
            credits_finish_receive_ctn,
            credits_finish_notify_ctn,
            ratings_finish_receive_ntc,
            ratings_finish_notify_ntc,
            ratings_finish_receive_ctn,
            ratings_finish_notify_ctn,
            **kwargs
    ):
        self.is_alive = True
        self.protocol = Protocol()
        self.work_consumer = None
        self.movies_publisher = None
        self.ratings_publisher = None
        self.credits_publisher = None
        self.replica_id = kwargs.get('replica_id', 'unknown')  # Initialize replica_id with a default value

        # Movies communication
        self.movies_finish_receive_ntc = movies_finish_receive_ntc
        self.movies_finish_notify_ntc = movies_finish_notify_ntc
        self.movies_finish_receive_ctn = movies_finish_receive_ctn
        self.movies_finish_notify_ctn = movies_finish_notify_ctn

        # Credits communication
        self.credits_finish_receive_ntc = credits_finish_receive_ntc
        self.credits_finish_notify_ntc = credits_finish_notify_ntc
        self.credits_finish_receive_ctn = credits_finish_receive_ctn
        self.credits_finish_notify_ctn = credits_finish_notify_ctn

        # Ratings communication
        self.ratings_finish_receive_ntc = ratings_finish_receive_ntc
        self.ratings_finish_notify_ntc = ratings_finish_notify_ntc
        self.ratings_finish_receive_ctn = ratings_finish_receive_ctn
        self.ratings_finish_notify_ctn = ratings_finish_notify_ctn

        self.finish_signal_checker = None
        self.send_actual_client_id_status = Queue()
        
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
        self.work_consumer = RabbitMQ(
            self.input_exchange, 
            self.input_queue, 
            self.input_routing_key, 
            "direct",
            auto_ack=False,
            prefetch_count=1
        )
        self.movies_publisher = RabbitMQ(self.movies_exchange, self.movies_queue, self.movies_routing_key, "direct")
        self.ratings_publisher = RabbitMQ(self.ratings_exchange, "", "", "x-consistent-hash")
        self.credits_publisher = RabbitMQ(self.credits_exchange, "", "", "x-consistent-hash")
        logging.info("Queues up, ready to use")

    def run(self):
        """Start the DataController with message consumption"""
        self._settle_queues()
        
        # Create separate processes for each type of check_finished
        self.movies_finish_signal_checker = Process(target=self.check_finished, args=("MOVIES",))
        self.ratings_finish_signal_checker = Process(target=self.check_finished, args=("RATINGS",))
        self.credits_finish_signal_checker = Process(target=self.check_finished, args=("CREDITS",))
        
        self.movies_finish_signal_checker.start()
        self.ratings_finish_signal_checker.start()
        self.credits_finish_signal_checker.start()

        self.work_consumer.consume(self.callback)

    def callback(self, ch, method, properties, body):
        # try:
            message_type, message = self.protocol.decode_client_msg(body, self.columns_needed)
            if not message_type or not message:
                logging.warning("Received invalid message from server")
                # ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge invalid messages
                return
            
            if message.finished:
                self._handle_finished_message(message_type, message)
            else:
                self._handle_data_message(message_type, message)
            
            # Acknowledge successful processing
            # ch.basic_ack(delivery_tag=method.delivery_tag)
        # except Exception as e:
            # logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            # ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _handle_finished_message(self, message_type, msg):
        """Handle finished messages with coordination"""
        logging.info(f"Received {message_type.name} finished signal from server.")
        client_id = msg.client_id
        msg_to_send = msg.SerializeToString()
        
        if message_type == FileType.MOVIES:
            self.movies_finish_receive_ntc.put(msg_to_send)
            if self.movies_finish_receive_ctn.get() == True:
                logging.info(f"Propagating finished MOVIES of client {client_id} downstream")
                self.movies_publisher.publish(msg_to_send)
        elif message_type == FileType.RATINGS:
            self.ratings_finish_receive_ntc.put(msg_to_send)
            if self.ratings_finish_receive_ctn.get() == True:
                logging.info(f"Propagating finished RATINGS of client {client_id} downstream")
                self.ratings_publisher.publish(msg_to_send)
        elif message_type == FileType.CREDITS:
            self.credits_finish_receive_ntc.put(msg_to_send)
            if self.credits_finish_receive_ctn.get() == True:
                logging.info(f"Propagating finished CREDITS of client {client_id} downstream")
                self.credits_publisher.publish(msg_to_send)

    def _handle_data_message(self, message_type, msg):
        self.send_actual_client_id_status.put([msg.client_id, START])
        if message_type == FileType.MOVIES:
            self.publish_movies(msg)
        elif message_type == FileType.RATINGS:
            self.publish_ratings(msg)
        elif message_type == FileType.CREDITS:
            self.publish_credits(msg)
        self.send_actual_client_id_status.put([msg.client_id, DONE])

    def publish_movies(self, movies_csv):
        movies_pb = filter_movies(movies_csv)
        if movies_pb:
            self.movies_publisher.publish(movies_pb.SerializeToString())

    def publish_ratings(self, ratings_csv):
        ratings_by_movie = filter_ratings(ratings_csv)
        for movie_id, batch in ratings_by_movie.items():
            self.ratings_publisher.publish(batch.SerializeToString(), routing_key=str(movie_id))

    def publish_credits(self, credits_csv):
        credits_by_movie = filter_credits(credits_csv)
        for movie_id, batch in credits_by_movie.items():
            self.credits_publisher.publish(batch.SerializeToString(), routing_key=str(movie_id))

    def stop(self):
        """Stop the DataController and close all connections"""
        logging.info(f"Stopping DataController {self.replica_id}...")
        
        # Close work consumer connection
        if self.work_consumer:
            try:
                self.work_consumer.close_channel()
                logging.info("Work consumer channel closed")
            except Exception as e:
                logging.error(f"Error closing work consumer channel: {e}")
        
        # Close movies publisher connection
        if self.movies_publisher:
            try:
                self.movies_publisher.close_channel()
                logging.info("Movies publisher channel closed")
            except Exception as e:
                logging.error(f"Error closing movies publisher channel: {e}")
        
        # Close ratings publisher connection
        if self.ratings_publisher:
            try:
                self.ratings_publisher.close_channel()
                logging.info("Ratings publisher channel closed")
            except Exception as e:
                logging.error(f"Error closing ratings publisher channel: {e}")
        
        # Close credits publisher connection
        if self.credits_publisher:
            try:
                self.credits_publisher.close_channel()
                logging.info("Credits publisher channel closed")
            except Exception as e:
                logging.error(f"Error closing credits publisher channel: {e}")

        # Terminate all finish signal checkers
        for checker, name in [
            (self.movies_finish_signal_checker, "Movies"),
            (self.ratings_finish_signal_checker, "Ratings"),
            (self.credits_finish_signal_checker, "Credits")
        ]:
            if checker:
                checker.terminate()
                checker.join()
                logging.info(f"{name} finished signal checker process terminated.")
        
        self.is_alive = False
        
        logging.info(f"DataController {self.replica_id} stopped successfully")

    def check_finished(self, type_name):
        """Check for finished signals for a specific type of message"""
        while self.is_alive:
            try:
                if type_name == "MOVIES":
                    queue = self.movies_finish_notify_ctn
                    notify_queue = self.movies_finish_notify_ntc
                elif type_name == "RATINGS":
                    queue = self.ratings_finish_notify_ctn
                    notify_queue = self.ratings_finish_notify_ntc
                elif type_name == "CREDITS":
                    queue = self.credits_finish_notify_ctn
                    notify_queue = self.credits_finish_notify_ntc
                else:
                    logging.error(f"Unknown type in check_finished: {type_name}")
                    break

                msg = queue.get()
                logging.info(f"Received {type_name} finished signal from control channel: {msg}")

                client_id, status = self.get_last()
                client_finished = msg[0]
                if client_finished == client_id:
                    notify_queue.put([client_finished, status])
                    logging.info(f"Received {type_name} finished signal from control channel for client {client_finished}, with status {status}.")
                else:
                    notify_queue.put([client_finished, True])
                    logging.info(f"Received {type_name} finished signal from control channel for client {client_finished}, but working on {client_id}.")

            except Empty:
                logging.info(f"No {type_name} finished signal received yet.")
                pass
            except Exception as e:
                logging.error(f"Error in {type_name} finished signal checker: {e}")
                break

    def get_last(self):
        client_id = None
        status = None

        while not self.send_actual_client_id_status.empty():
            client_id, status = self.send_actual_client_id_status.get_nowait()

        logging.info(f"Last client ID: {client_id}, status: {status}")

        return client_id, status

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logging.info(f"DataController received signal {signum}. Shutting down gracefully...")
        self.stop()
        logging.info("DataController shutdown complete.")