import logging
import time
import signal
import os
import threading
from protocol import files_pb2
from protocol.protocol import Protocol, FileType
from protocol.rabbit_wrapper import RabbitMQConsumer, RabbitMQProducer
from protocol.utils.parsing_proto_utils import is_date
from .coordination_service import CoordinationService

# Configure logging to reduce noise
logging.getLogger('pika').setLevel(logging.WARNING)  # Reduce Pika logs to warnings and above
logging.getLogger('root').setLevel(logging.INFO)     # Keep our logs at info level

class DataController:
    def __init__(self, **kwargs):
        self.protocol = Protocol()
        
        # Get replica information from environment
        self.replica_id = int(os.getenv('DATA_CONTROLLER_REPLICA_ID', '1'))
        self.replica_count = int(os.getenv('DATA_CONTROLLER_REPLICA_COUNT', '1'))
        
        # Initialize coordination service
        self.coordination = CoordinationService(**kwargs)
        
        # Set attributes from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)
        
        self.columns_needed = {
            'movies': ["id", "title", "genres", "release_date", "overview", 
                      "production_countries", "spoken_languages", "budget", "revenue"],
            'ratings': ["movieId", "rating", "timestamp"],
            'credits': ["id", "cast"]
        }
        
        # Initialize RabbitMQ connections
        self._init_rabbitmq_connections()

    def _init_rabbitmq_connections(self):
        # Initialize work channel for data processing
        self.work_consumer = RabbitMQConsumer(
            host='rabbitmq',
            exchange=self.input_exchange,
            exchange_type="direct",
            queue_name=f"forward_queue-{self.replica_id}",
            routing_key=self.input_routing_key,
            durable=True
        )
        
        # Initialize publishers for different data types
        self.movies_publisher = RabbitMQProducer(
            host='rabbitmq',
            exchange=self.movies_exchange,
            exchange_type="direct",
            routing_key=self.movies_routing_key
        )
        
        self.ratings_publisher = RabbitMQProducer(
            host='rabbitmq',
            exchange=self.ratings_exchange,
            exchange_type="x-consistent-hash",
            routing_key=""  # Will be set per message
        )
        
        self.credits_publisher = RabbitMQProducer(
            host='rabbitmq',
            exchange=self.credits_exchange,
            exchange_type="x-consistent-hash",
            routing_key=""  # Will be set per message
        )
    

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logging.info(f"Received signal {signum}. Shutting down gracefully...")
        self.stop()
        logging.info("Shutdown complete.")

    def start(self):
        """Start the DataController with message consumption"""
        self._setup_signal_handlers()
        try:
            # Set prefetch count to 1 for work channel
            self.work_consumer.channel.basic_qos(prefetch_count=1)
            
            # Start consuming on both channels
            self.work_consumer.consume(self._process_server_message, auto_ack=False)

            # Start consuming in separate threads
            work_thread = threading.Thread(target=self.work_consumer.start_consuming)
            work_thread.start()
            work_thread.join()
            
        except Exception as e:
            logging.error(f"Error in DataController {self.replica_id}: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stop the DataController and close all connections"""
        logging.info(f"Stopping DataController {self.replica_id}...")
        
        self.coordination.stop()
        
        # Stop all consumers and producers
        for consumer in [self.work_consumer, self.control_consumer]:
            if consumer:
                consumer.stop()
        
        for publisher in [self.movies_publisher, self.ratings_publisher, 
                         self.credits_publisher, self.other_files_finished_publisher,
                         self.movies_files_finished_publisher]:
            if publisher:
                publisher.stop()
        
        logging.info(f"DataController {self.replica_id} stopped")

    def _process_server_message(self, ch, method, properties, body):
        """Process messages received from the server"""
        try:
            # Decode the message type and content
            message_type, message = self.protocol.decode_client_msg(body, self.columns_needed)
            if not message_type or not message:
                logging.warning("Received invalid message from server")
                return
            
            # Get client ID from message properties
            client_id = properties.headers.get('client_id')
            if client_id is None:
                logging.error("Received message without client_id in headers")
                return
            
            # Update in-flight count in coordination service
            self.coordination.update_in_flight(client_id, message_type, 1)
            
            # Process the message based on its type
            if message.finished:
                logging.info(f"Received finished signal for type {message_type.name} from client {client_id}")
                self._handle_finished_message(message_type, message, client_id)
            else:
                self._handle_data_message(message_type, message)
            
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logging.error(f"Error processing server message: {e}")
            # Negative acknowledge the message in case of error
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        finally:
            # Update in-flight count in coordination service
            self.coordination.update_in_flight(client_id, message_type, -1)

    def _handle_finished_message(self, message_type, msg, client_id):
        """Handle finished messages with coordination"""
        try:
            # Let the coordination service handle everything
            finish_msg = self.coordination.handle_finish_signal(
                message_type,
                msg,
                client_id
            )
            
            if finish_msg is not None:
                # Publish to the appropriate output queue
                if message_type == FileType.MOVIES:
                    self.movies_files_finished_publisher.publish(finish_msg)
                    logging.info(f"Published finish signal for MOVIES to output queue")
                elif message_type in [FileType.RATINGS, FileType.CREDITS]:
                    self.other_files_finished_publisher.publish(finish_msg)
                    logging.info(f"Published finish signal for {message_type.name} to output queue")
                
                # Clean up the finish signal tracking
                self.coordination.cleanup_finish_signal(client_id, message_type)
                
        except Exception as e:
            logging.error(f"Error handling finished message: {e}", exc_info=True)

    def _handle_data_message(self, message_type, msg):
        # logging.info(f"Received data message for type {message_type.name}")
        if message_type == FileType.MOVIES:
            self.filter_movies(msg)
        elif message_type == FileType.RATINGS:
            self.filter_ratings(msg)
        elif message_type == FileType.CREDITS:
            self.filter_credits(msg)

    def filter_movies(self, movies_csv):
        movies_pb = files_pb2.MoviesCSV()
        for movie in movies_csv.movies:
            if not movie.id or movie.id < 0 or not movie.release_date:
                continue
            if not is_date(movie.release_date):
                continue

            filtered_genres = [genre for genre in movie.genres if genre.name]
            countries = map(lambda country: country.name, movie.countries)
            countries = list(filter(lambda name: name, countries))

            movie_pb = movies_pb.movies.add()
            movie_pb.id = movie.id
            movie_pb.title = movie.title
            movie_pb.release_date = movie.release_date
            movie_pb.overview = movie.overview
            movie_pb.budget = movie.budget
            movie_pb.revenue = movie.revenue
            movie_pb.genres.extend(filtered_genres)

            for country in countries:
                country_pb = movie_pb.countries.add()
                country_pb.name = country

        if not len(movies_pb.movies):
            return
        self.movies_publisher.publish(movies_pb.SerializeToString())

    def filter_ratings(self, ratings_csv):
        ratings_by_movie = dict()
        for rating in ratings_csv.ratings:
            if not rating.movieId or rating.movieId < 0 or not rating.rating or rating.rating < 0:
                continue

            rating_pb = files_pb2.RatingCSV()
            rating_pb.userId = rating.userId
            rating_pb.movieId = rating.movieId
            rating_pb.rating = rating.rating

            ratings_by_movie.setdefault(rating.movieId, files_pb2.RatingsCSV())
            ratings_pb = ratings_by_movie[rating.movieId]
            ratings_pb.ratings.append(rating_pb)
            ratings_by_movie[rating.movieId] = ratings_pb

        for movie_id, batch in ratings_by_movie.items():
            self.ratings_publisher.publish(batch.SerializeToString(), routing_key=str(movie_id))

    def filter_credits(self, credits_csv):
        credits_by_movie = dict()
        for credit in credits_csv.credits:
            if not credit.id or credit.id < 0 or not len(credit.cast):
                continue

            names = map(lambda cast: cast.name, credit.cast)
            names = list(filter(lambda name: name, names))

            credit_pb = files_pb2.CreditCSV()
            credit_pb.id = credit.id

            for name in names:
                cast_pb = credit_pb.cast.add()
                cast_pb.name = name

            credits_by_movie.setdefault(credit.id, files_pb2.CreditsCSV())
            credits_pb = credits_by_movie[credit.id]
            credits_pb.credits.append(credit_pb)
            credits_by_movie[credit.id] = credits_pb

        for movie_id, batch in credits_by_movie.items():
            self.credits_publisher.publish(batch.SerializeToString(), routing_key=str(movie_id)) 