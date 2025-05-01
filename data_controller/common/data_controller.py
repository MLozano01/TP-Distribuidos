import logging
import time
import signal
import os
from protocol import files_pb2
from protocol.protocol import Protocol, FileType
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.parsing_proto_utils import is_date

class DataController:
    def __init__(self, **kwargs):
        self.protocol = Protocol()
        # self.filtered_movies_ids = [] wont work with multiple instances of data controller
        
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
        
        # Initialize RabbitMQ connections
        self._init_rabbitmq_connections()

    def _init_rabbitmq_connections(self):
        self.movies_queue = RabbitMQ(
            self.movies_exchange,
            "",
            self.movies_routing_key,
            "direct"
        )
        self.ratings_queue = RabbitMQ(
            self.ratings_exchange,
            "",  # queue name not needed for publishing
            "",  # routing key will be movie ID at publish time
            "x-consistent-hash"
        )
        self.credits_queue = RabbitMQ(
            self.credits_exchange,
            "",  # queue name not needed for publishing
            "",  # routing key will be movie ID at publish time
            "x-consistent-hash"
        )
        
        # Initialize control exchanges
        self.other_files_finished_publisher = RabbitMQ(
            self.finished_file_exchange,
            "",  # empty queue name for fanout
            "",  # empty routing key for fanout
            "fanout"
        )
        self.movies_files_finished_publisher = RabbitMQ(
            self.finished_movies_exchange,
            "",  # empty queue name for fanout
            "",  # empty routing key for fanout
            "fanout"
        )

        # Initialize server message consumer - each data controller instance has its own queue
        self.server_consumer = RabbitMQ(
            "server_to_data_controller",  # exchange
            f"forward_queue-{self.replica_id}",  # unique queue per instance
            "forward",  # routing key
            "direct"    # exchange type
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
            # Start consuming messages from server
            self.server_consumer.consume(self._process_server_message)
            logging.info(f"DataController {self.replica_id} started consuming messages...")

            
        except Exception as e:
            logging.error(f"Error in DataController {self.replica_id}: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stop the DataController and close all connections"""
        logging.info(f"Stopping DataController {self.replica_id}...")
        
        # Stop all RabbitMQ connections
        for queue in [self.movies_queue, self.ratings_queue, self.credits_queue,
                     self.other_files_finished_publisher, self.movies_files_finished_publisher,
                     self.server_consumer]:
            if queue:
                try:
                    queue.stop()
                except Exception as e:
                    logging.error(f"Error stopping queue: {e}")
        
        logging.info(f"DataController {self.replica_id} stopped")

    def _process_server_message(self, ch, method, properties, body):
        """Process messages received from the server"""
        try:
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
            #ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            logging.error(f"Error processing server message: {e}")
            # Negative acknowledge the message in case of error
            #ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

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

            # year = int(movie.release_date.split('-')[0])
            # has_argentina = False
            for country in countries:
                country_pb = movie_pb.countries.add()
                country_pb.name = country
                if country == "Argentina":
                    has_argentina = True

            # if year >= 2000 and has_argentina: Wont work with multiple instances of data controller
            #     self.filtered_movies_ids.append(movie.id)

        if not len(movies_pb.movies):
            return
        self.movies_queue.publish(movies_pb.SerializeToString())

    def filter_ratings(self, ratings_csv):
        ratings_by_movie = dict()
        for rating in ratings_csv.ratings:
            if not rating.movieId or rating.movieId < 0 or not rating.rating or rating.rating < 0:
                continue

            #if rating.movieId not in self.filtered_movies_ids:
            #   continue

            rating_pb = files_pb2.RatingCSV()
            rating_pb.userId = rating.userId
            rating_pb.movieId = rating.movieId
            rating_pb.rating = rating.rating

            ratings_by_movie.setdefault(rating.movieId, files_pb2.RatingsCSV())
            ratings_pb = ratings_by_movie[rating.movieId]
            ratings_pb.ratings.append(rating_pb)
            ratings_by_movie[rating.movieId] = ratings_pb

        for movie_id, batch in ratings_by_movie.items():
            self.ratings_queue.publish(batch.SerializeToString(), routing_key=str(movie_id))

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
            self.credits_queue.publish(batch.SerializeToString(), routing_key=str(movie_id)) 