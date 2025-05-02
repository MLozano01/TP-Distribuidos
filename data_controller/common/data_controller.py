import logging
import time
import signal
import os
import threading
from protocol import files_pb2
from protocol.protocol import Protocol, FileType
from protocol.rabbit_wrapper import RabbitMQConsumer, RabbitMQProducer
from protocol.utils.parsing_proto_utils import is_date

class DataController:
    def __init__(self, **kwargs):
        self.protocol = Protocol()
        
        # Get replica information from environment
        self.replica_id = int(os.getenv('DATA_CONTROLLER_REPLICA_ID', '1'))
        self.replica_count = int(os.getenv('DATA_CONTROLLER_REPLICA_COUNT', '1'))
        
        # Set attributes from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)
        
        self.columns_needed = {
            'movies': ["id", "title", "genres", "release_date", "overview", 
                      "production_countries", "spoken_languages", "budget", "revenue"],
            'ratings': ["movieId", "rating", "timestamp"],
            'credits': ["id", "cast"]
        }
        
        # Initialize state variables for drain-and-finish pattern
        self.finish_received = {
            FileType.MOVIES: False,
            FileType.RATINGS: False,
            FileType.CREDITS: False
        }
        self.in_flight = 0
        self.lock = threading.Lock()
        
        # Initialize RabbitMQ connections
        self._init_rabbitmq_connections()

    def _init_rabbitmq_connections(self):
        # Initialize work channel for data processing
        self.work_consumer = RabbitMQConsumer(
            host='rabbitmq',
            exchange="server_to_data_controller",
            exchange_type="direct",
            queue_name=f"forward_queue-{self.replica_id}",
            routing_key="forward",
            durable=True
        )
        
        # Initialize control channel for finish signals
        self.control_consumer = RabbitMQConsumer(
            host='rabbitmq',
            exchange="data_controller_control",
            exchange_type="fanout",
            queue_name='',  # Let RabbitMQ generate a unique queue name
            routing_key='',  # Fanout ignores routing key
            exclusive=True,
            auto_delete=True
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
        
        # Initialize control publishers
        self.other_files_finished_publisher = RabbitMQProducer(
            host='rabbitmq',
            exchange=self.finished_file_exchange,
            exchange_type="fanout",
            routing_key=""
        )
        
        self.movies_files_finished_publisher = RabbitMQProducer(
            host='rabbitmq',
            exchange=self.finished_movies_exchange,
            exchange_type="fanout",
            routing_key=""
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
            self.control_consumer.consume(self._on_control_message, auto_ack=True)
            
            # Start consuming in separate threads
            work_thread = threading.Thread(target=self.work_consumer.start_consuming)
            control_thread = threading.Thread(target=self.control_consumer.start_consuming)
            
            work_thread.start()
            control_thread.start()
            
            work_thread.join()
            control_thread.join()
            
        except Exception as e:
            logging.error(f"Error in DataController {self.replica_id}: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stop the DataController and close all connections"""
        logging.info(f"Stopping DataController {self.replica_id}...")
        
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
            # Increment in-flight counter
            with self.lock:
                self.in_flight += 1
            
            # Decode the message type and content
            message_type, message = self.protocol.decode_client_msg(body, self.columns_needed)
            if not message_type or not message:
                logging.warning("Received invalid message from server")
                return
            
            # Process the message based on its type
            if message.finished:
                self._handle_finished_message(message_type, message)
            else:
                self._handle_data_message(message_type, message)
            
            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logging.error(f"Error processing server message: {e}")
            # Negative acknowledge the message in case of error
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        finally:
            # Decrement in-flight counter and check for finish
            with self.lock:
                self.in_flight -= 1
                self._maybe_propagate_finish(message_type)

    def _on_control_message(self, ch, method, properties, body):
        """Handle control messages"""
        message_type, message = self.protocol.decode_client_msg(body, self.columns_needed)
        if message and message.finished:
            with self.lock:
                self.finish_received[message_type] = True
                self._maybe_propagate_finish(message_type)

    def _maybe_propagate_finish(self, message_type):
        """Check if we should propagate the finish signal for a specific message type"""
        try:
            # Get message count from work queue
            queue_info = self.work_consumer.channel.queue_declare(
                queue=f"forward_queue-{self.replica_id}",
                passive=True
            )
            message_count = queue_info.method.message_count
            
            if self.finish_received[message_type] and self.in_flight == 0 and message_count == 0:
                # Propagate finish based on message type
                if message_type == FileType.MOVIES:
                    self.movies_files_finished_publisher.publish(
                        self.protocol.create_finished_message(message_type)
                    )
                elif message_type in [FileType.RATINGS, FileType.CREDITS]:
                    self.other_files_finished_publisher.publish(
                        self.protocol.create_finished_message(message_type)
                    )
                
                
        except Exception as e:
            logging.error(f"Error in _maybe_propagate_finish: {e}")

    def _handle_finished_message(self, message_type, msg):
        if message_type in [FileType.RATINGS, FileType.CREDITS]:
            self._handle_ratings_or_credits_finished(message_type)
        elif message_type == FileType.MOVIES:
            self._handle_movies_finished(msg)
        else:
            logging.warning(f"Received finished signal for unhandled type: {message_type.name}")

    def _handle_ratings_or_credits_finished(self, message_type):
        logging.info(f"Received finished signal for type {message_type.name}. Publishing type code to control exchange...")
        try:
            time.sleep(3)
            self.other_files_finished_publisher.publish(
                self.protocol.create_finished_message(message_type)
            )
            logging.info(f"Published finish signal message for {message_type.name}")
        except Exception as e:
            logging.error(f"Failed to publish finish signal message: {e}")

    def _handle_movies_finished(self, msg):
        logging.info("Received finished signal for type MOVIES. Forwarding full message to data exchange...")
        try:
            self.movies_files_finished_publisher.publish(msg.SerializeToString())
            logging.info("Forwarded full MOVIES finished message")
        except Exception as e:
            logging.error(f"Failed to forward full MOVIES finished message: {e}")

    def _handle_data_message(self, message_type, msg):
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