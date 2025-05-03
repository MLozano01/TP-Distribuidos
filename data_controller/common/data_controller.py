import logging
import time
import signal
import os
import threading
from protocol import files_pb2
from protocol.protocol import Protocol, FileType
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.parsing_proto_utils import is_date
from .aux import filter_movies, filter_ratings, filter_credits

# Configure logging to reduce noise
logging.getLogger('pika').setLevel(logging.WARNING)  # Reduce Pika logs to warnings and above
logging.getLogger('root').setLevel(logging.INFO)     # Keep our logs at info level

class DataController:
    def __init__(self, movies_comm_queue, credits_comm_queue, ratings_comm_queue, **kwargs):
        self.protocol = Protocol()
        self.work_consumer = None
        self.movies_publisher = None
        self.ratings_publisher = None
        self.credits_publisher = None
        self.movies_comm_queue = movies_comm_queue
        self.credits_comm_queue = credits_comm_queue
        self.ratings_comm_queue = ratings_comm_queue
        
        # Set attributes from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)
        
        self.columns_needed = {
            'movies': ["id", "title", "genres", "release_date", "overview", 
                      "production_countries", "spoken_languages", "budget", "revenue"],
            'ratings': ["movieId", "rating", "timestamp"],
            'credits': ["id", "cast"]
        }
        

    def _settle_queues(self):
        self.work_consumer = RabbitMQ(self.input_exchange, self.input_queue, self.input_routing_key, "direct")
        self.movies_publisher = RabbitMQ(self.movies_exchange, self.movies_queue, self.movies_routing_key, "direct")
        self.ratings_publisher = RabbitMQ(self.ratings_exchange, "", "", "x-consistent-hash")
        self.credits_publisher = RabbitMQ(self.credits_exchange, "", "", "x-consistent-hash")


    def run(self):
        """Start the DataController with message consumption"""
        self._settle_queues()
        self.work_consumer.consume(self.callback)
        self.check_finished()

    def callback(self, ch, method, properties, body):
        logging.info(f"Received message, with routing key: {method.routing_key}")
        message_type, message = self.protocol.decode_client_msg(body, self.columns_needed)
        if not message_type or not message:
            logging.warning("Received invalid message from server")
            return
        
        if message.finished:
            self._handle_finished_message(message_type, message)
        else:
            self._handle_data_message(message_type, message)

    def _handle_finished_message(self, message_type, msg):
        """Handle finished messages with coordination"""
        logging.info(f"Received {message_type.name} finished signal from server.")
        msg_to_send = msg.SerializeToString()

        if message_type == FileType.MOVIES:
            self.movies_comm_queue.put(msg_to_send)
            logging.info(f"Published finished signal to communication channel, here the encoded message: {msg}")
            if self.movies_comm_queue.get() == True:
                logging.info("Received SEND finished signal from MOVIES communication channel.")
                self.movies_publisher.publish(msg_to_send)
        elif message_type == FileType.RATINGS:
            self.ratings_comm_queue.put(msg_to_send)
            logging.info(f"Published finished signal to communication channel, here the encoded message: {msg}")
            if self.ratings_comm_queue.get() == True:
                logging.info("Received SEND finished signal from RATINGS communication channel.")
                self.ratings_publisher.publish(msg_to_send)
        elif message_type == FileType.CREDITS:
            self.credits_comm_queue.put(msg_to_send)
            logging.info(f"Published finished signal to communication channel, here the encoded message: {msg}")
            if self.credits_comm_queue.get() == True:
                logging.info("Received SEND finished signal from CREDITS communication channel.")
                self.credits_publisher.publish(msg_to_send)


    def _handle_data_message(self, message_type, msg):
        if message_type == FileType.MOVIES:
            self.publish_movies(msg)
        elif message_type == FileType.RATINGS:
            self.publish_ratings(msg)
        elif message_type == FileType.CREDITS:
            self.publish_credits(msg)

    def publish_movies(self, movies_csv):
        movies_pb = filter_movies(movies_csv)
        logging.info(f"Forwarding movies")
        if movies_pb:
            self.movies_publisher.publish(movies_pb.SerializeToString())

    def publish_ratings(self, ratings_csv):
        ratings_by_movie = filter_ratings(ratings_csv)
        logging.info(f"Forwarding ratings")
        for movie_id, batch in ratings_by_movie.items():
            self.ratings_publisher.publish(batch.SerializeToString(), routing_key=str(movie_id))

    def publish_credits(self, credits_csv):
        credits_by_movie = filter_credits(credits_csv)
        logging.info(f"Forwarding credits")
        for movie_id, batch in credits_by_movie.items():
            self.credits_publisher.publish(batch.SerializeToString(), routing_key=str(movie_id))

    def stop(self):
        """Stop the DataController and close all connections"""
        logging.info(f"Stopping DataController {self.replica_id}...")
        # TODO: Implement
        logging.info(f"DataController {self.replica_id} stopped")

    def _check_finished(self):
        if self.comm_queue.get_nowait():
            self.comm_queue.put(True)