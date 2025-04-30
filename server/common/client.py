from multiprocessing import Process
import socket
import logging
import time
import json
from protocol import files_pb2
from protocol.protocol import Protocol, FileType
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.parsing_proto_utils import is_date
from protocol.utils.socket_utils import recvall
from protocol.protocol import MOVIES_FILE_CODE, RATINGS_FILE_CODE, CREDITS_FILE_CODE, CODE_LENGTH

class Client:
  def __init__(self, client_sock):
    self.socket = client_sock
    self.data_controller = None
    self.result_controller = None
    #TODO: config con queues y columnas
    self.movies_queue = RabbitMQ("exchange_rcv_movies", "rcv_movies", "movies_plain", "direct")
    self.ratings_queue = RabbitMQ("exchange_rcv_ratings", "rcv_ratings", "ratings_plain", "x-consistent-hash")
    self.credits_queue = RabbitMQ("exchange_rcv_credits", "rcv_credits", "credits_plain", "x-consistent-hash")
    self.protocol = Protocol()
    self.other_files_finished_publisher = RabbitMQ("server_finished_file_exchange", None, "", "fanout") # Use empty string for routing key
    self.movies_files_finished_publisher = RabbitMQ("server_finished_movies_file_exchange", None, "", "fanout") # Use empty string for routing key
    self.columns_nedded = {'movies': ["id", "title", "genres", "release_date", "overview", "production_countries", "spoken_languages", "budget", "revenue"],
                           'ratings': ["movieId", "rating", "timestamp"],
                           'credits': ["id", "cast"]}
    self.filtered_movies_ids = []

  def run(self):
    self.data_controller = Process(target=self.handle_connection, args=[self.socket])
    self.data_controller.start()

    self.result_controller = Process(target=self.return_results, args=[self.socket])
    self.result_controller.start()

    self.data_controller.join()
    self.result_controller.join()

  def stop(self):
    if self.data_controller.is_alive():
      self.data_controller.terminate()
    if self.result_controller.is_alive():
      self.result_controller.terminate()
  

  def handle_connection(self, conn: socket.socket):
    closed_socket = False
    while not closed_socket:
      read_amount = self.protocol.define_initial_buffer_size()
      buffer = bytearray()
      closed_socket = recvall(conn, buffer, read_amount)
      if closed_socket:
        return
      read_amount = self.protocol.define_buffer_size(buffer)
      closed_socket = recvall(conn, buffer, read_amount)
      if closed_socket:
        return
      
      type, msg = self.protocol.decode_client_msg(buffer, self.columns_nedded)
      if msg.finished:
          self._handle_finished_message(type, msg)
      else:
          self._handle_data_message(type, msg)

  def _handle_finished_message(self, type, msg):
      """Handles received messages where the finished flag is set."""
      if type in [FileType.RATINGS, FileType.CREDITS]:
          self._handle_ratings_or_credits_finished(type)
      elif type == FileType.MOVIES:
          self._handle_movies_finished(msg)
      else:
          logging.warning(f"Received finished signal for unhandled type: {type.name}")

  def _handle_ratings_or_credits_finished(self, type):
      """Handles finished messages for Ratings or Credits."""
      logging.info(f"Received finished signal for type {type.name}. Publishing type code to control exchange...")
      type_code = self._get_type_code(type)
      if type_code is not None and self.other_files_finished_publisher:
          self._publish_finished_message(type)
      elif not self.other_files_finished_publisher:
          logging.error(f"Control publisher not initialized. Cannot send finish signal for {type.name}.")
      else:
          logging.warning(f"Could not determine type code for finished signal: {type.name}")

  def _get_type_code(self, type):
      """Returns the type code for Ratings or Credits."""
      if type == FileType.RATINGS:
          return RATINGS_FILE_CODE
      elif type == FileType.CREDITS:
          return CREDITS_FILE_CODE
      return None

  def _publish_finished_message(self, type):
      """Publishes the finished message to the control exchange."""
      try:
          time.sleep(3)
          self.other_files_finished_publisher.publish(self.protocol.create_finished_message(type))
          logging.info(f"Published finish signal message for {type.name} to {self.other_files_finished_publisher.exchange}")
      except Exception as e_pub:
          logging.error(f"Failed to publish finish signal message for {type.name} to control exchange: {e_pub}")

  def _handle_movies_finished(self, msg):
      """Handles finished messages for Movies."""
      logging.info(f"Received finished signal for type MOVIES. Forwarding full message to data exchange...")
      try:
          self.movies_queue.publish(msg.SerializeToString())
          self.movies_files_finished_publisher.publish(msg.SerializeToString())
          logging.info(f"Forwarded full MOVIES finished message to {self.movies_queue.exchange}")
      except Exception as e_pub:
          logging.error(f"Failed to forward full MOVIES finished message: {e_pub}")

  def _handle_data_message(self, type, msg):
      """Handles received messages containing data (finished flag is false)."""
      if type == FileType.MOVIES:
        self.filter_movies(msg)
      elif type == FileType.RATINGS:
        self.filter_ratings(msg)
      elif type == FileType.CREDITS:
        self.filter_credits(msg)

  def filter_movies(self, movies_csv):
    movies_pb = files_pb2.MoviesCSV()
    for movie in movies_csv.movies:
      if not movie.id or movie.id < 0 or not movie.release_date:
        continue
      if not is_date(movie.release_date):
        continue

      # if not movie.budget or movie.budget < 0 or not movie.revenue or movie.revenue < 0:
      #   continue
      
      # Filter the original Genre objects based on their name attribute
      filtered_genres = [genre for genre in movie.genres if genre.name]
      # if not len(filtered_genres):
      #     continue

      countries = map(lambda country: country.name, movie.countries)
      countries = list(filter(lambda name: name, countries))
      # if not len(countries):
      #   continue

      movie_pb = movies_pb.movies.add()
      movie_pb.id = movie.id
      movie_pb.title = movie.title
      movie_pb.release_date = movie.release_date
      movie_pb.overview = movie.overview
      movie_pb.budget = movie.budget
      movie_pb.revenue = movie.revenue

      # Add the filtered Genre objects to the new message
      movie_pb.genres.extend(filtered_genres)

      for country in countries:
        country_pb = movie_pb.countries.add()
        country_pb.name = country
      self.filtered_movies_ids.append(movie.id)

    if not len(movies_pb.movies):
      return
    self.movies_queue.publish(movies_pb.SerializeToString())
  
  def filter_ratings(self, ratings_csv):
    ratings_by_movie = dict() 
    for rating in ratings_csv.ratings:
      if not rating.movieId or rating.movieId < 0 or not rating.rating or rating.rating < 0:
        continue

      if rating.movieId not in self.filtered_movies_ids:
        continue
      
      rating_pb = files_pb2.RatingCSV()
      rating_pb.userId = rating.userId
      rating_pb.movieId = rating.movieId
      rating_pb.rating = rating.rating
      # rating_pb.timestamp = rating.timestamp

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
      # if not len(names):
      #   continue

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


  def return_results(self, conn: socket.socket):
    queue = RabbitMQ('exchange_snd_results', 'result', 'results', 'direct')
    queue.consume(self.result_controller_func)
  
  def result_controller_func(self, ch, method, properties, body):
    try:
      # data = json.loads(body)
      logging.info(f"got result: {body}") 
      msg = self.protocol.create_client_result(body)
      logging.info(f"sending message: {msg}")
      self.socket.sendall(msg)
    except json.JSONDecodeError as e:
      logging.error(f"Failed to decode JSON: {e}")
      return
    except Exception as e:
      logging.error(f"Error processing message: {e}")
      return