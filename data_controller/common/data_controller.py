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
logging.getLogger('pika').setLevel(logging.WARNING)
logging.getLogger('RabbitMQ').setLevel(logging.WARNING)

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


    def run(self):
        """Start the DataController with message consumption"""
        self._settle_queues()
        self.work_consumer.consume(self.callback)
        self.check_finished()

    def callback(self, ch, method, properties, body):
        try:
            message_type, message = self.protocol.decode_client_msg(body, self.columns_needed)
            logging.info(f"Received message from server of type: {message_type.name}")
            if not message_type or not message:
                logging.warning("Received invalid message from server")
                ch.basic_ack(delivery_tag=method.delivery_tag)  # Acknowledge invalid messages
                return
            
            if message.finished:
                self._handle_finished_message(message_type, message)
            else:
                self._handle_data_message(message_type, message)
            
            # Acknowledge successful processing
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            # Reject the message and requeue it
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def _handle_finished_message(self, message_type, msg):
        """Handle finished messages with coordination"""
        logging.info(f"Received {message_type.name} finished signal from server.")
        msg_to_send = msg.SerializeToString()

        if message_type == FileType.MOVIES:
            self.movies_comm_queue.put(msg_to_send)
            if self.movies_comm_queue.get() == True:
                logging.info("Propagating finished MOVIES downstream")
                self.movies_publisher.publish(msg_to_send)
        elif message_type == FileType.RATINGS:
            self.ratings_comm_queue.put(msg_to_send)
            if self.ratings_comm_queue.get() == True:
                logging.info("Propagating finished RATINGS downstream")
                self.ratings_publisher.publish(msg_to_send)
        elif message_type == FileType.CREDITS:
            self.credits_comm_queue.put(msg_to_send)
            if self.credits_comm_queue.get() == True:
                logging.info("Propagating finished CREDITS downstream")
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
        #logging.info(f"Forwarding movies")
        if movies_pb:
            self.movies_publisher.publish(movies_pb.SerializeToString())

    def publish_ratings(self, ratings_csv):
        ratings_by_movie = filter_ratings(ratings_csv)
        #logging.info(f"Forwarding ratings")
        for movie_id, batch in ratings_by_movie.items():
            #logging.info(f"Forwarding rating of {movie_id}")
            self.ratings_publisher.publish(batch.SerializeToString(), routing_key=str(movie_id))

    def publish_credits(self, credits_csv):
        credits_by_movie = filter_credits(credits_csv)
        #logging.info(f"Forwarding credits")
        for movie_id, batch in credits_by_movie.items():
            #logging.info(f"Forwarding credit of {movie_id}")
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
        
        logging.info(f"DataController {self.replica_id} stopped successfully")

    def _check_finished(self):
        if self.comm_queue.get_nowait():
            self.comm_queue.put(True)

    def _handle_shutdown(self, signum, frame):
        """Handle shutdown signals"""
        logging.info(f"DataController received signal {signum}. Shutting down gracefully...")
        self.stop()
        logging.info("DataController shutdown complete.")