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
    self.movies_queue = RabbitMQ("exchange_rcv_movies", "rcv_movies", "movies_plain", "direct")
    self.ratings_queue = RabbitMQ("exchange_rcv_ratings", "rcv_ratings", "ratings_plain", "x-consistent-hash")
    self.credits_queue = RabbitMQ("exchange_rcv_credits", "rcv_credits", "credits_plain", "x-consistent-hash")
    self.protocol = Protocol()
    self.file_finished_server_step_publisher = RabbitMQ("server_finished_file_exchange", None, "", "fanout") # Use empty string for routing key

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
    time.sleep(10)
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
      
      type, msg = self.protocol.decode_msg(buffer)

      if msg.finished:
          self._handle_finished_message(type, msg)
      else:
          self._handle_data_message(type, msg)

  def _handle_finished_message(self, type, msg):
      """Handles received messages where the finished flag is set."""
      # Send control signal code for Ratings/Credits via Control Publisher
      if type == FileType.RATINGS or type == FileType.CREDITS:
          logging.info(f"Received finished signal for type {type.name}. Publishing type code to control exchange...")
          type_code = None
          if type == FileType.RATINGS:
              type_code = RATINGS_FILE_CODE
          elif type == FileType.CREDITS:
              type_code = CREDITS_FILE_CODE

          if type_code is not None and self.file_finished_server_step_publisher:
              try:
                  self.file_finished_server_step_publisher.publish(self.protocol.create_finished_message_for_joiners(type))
                  logging.info(f"Published finish signal message for {type.name} to {self.file_finished_server_step_publisher.exchange}")
              except Exception as e_pub:
                  logging.error(f"Failed to publish finish signal message for {type.name} to control exchange: {e_pub}")
          elif not self.file_finished_server_step_publisher:
              logging.error(f"Control publisher not initialized. Cannot send finish signal for {type.name}.")
          else:
              logging.warning(f"Could not determine type code for finished signal: {type.name}")
      # Send full original Movies finished message via Movie Data Queue
      elif type == FileType.MOVIES:
          logging.info(f"Received finished signal for type {type.name}. Forwarding full message to data exchange...")
          try:
              # Publish ONLY the serialized payload as Filter expects this
              self.movies_queue.publish(msg.SerializeToString())
              logging.info(f"Forwarded full MOVIES finished message to {self.movies_queue.exchange}")
          except Exception as e_pub:
              logging.error(f"Failed to forward full MOVIES finished message: {e_pub}")
      else:
           logging.warning(f"Received finished signal for unhandled type: {type.name}")

  def _handle_data_message(self, type, msg):
      """Handles received messages containing data (finished flag is false)."""
      if type == FileType.MOVIES:
        self.filter_movies(msg)
        time.sleep(1)
      elif type == FileType.RATINGS:
        self.filter_ratings(msg)
      elif type == FileType.CREDITS:
        self.filter_credits(msg)

  def filter_movies(self, movies_csv):
    movies_pb = files_pb2.MoviesCSV()
    for movie in movies_csv.movies:
      if not movie.id or movie.id < 0 or not movie.title or not movie.release_date:
        continue
      if not is_date(movie.release_date) or not movie.overview:
        continue
      
      genres = map(lambda genre: genre.name, movie.genres)
      genres = list(filter(lambda name: name, genres))
      if not len(genres):
        continue

      countries = map(lambda country: country.name, movie.countries)
      countries = list(filter(lambda name: name, countries))
      if not len(countries):
        continue

      movie_pb = movies_pb.movies.add()
      movie_pb.id = movie.id
      movie_pb.title = movie.title
      movie_pb.release_date = movie.release_date
      movie_pb.overview = movie.overview
      movie_pb.budget = movie.budget
      movie_pb.revenue = movie.revenue

      for genre in genres:
        genre_pb = movie_pb.genres.add()
        genre_pb.name = genre
      for country in countries:
        country_pb = movie_pb.countries.add()
        country_pb.name = country

    if not len(movies_pb.movies):
      return
    self.movies_queue.publish(movies_pb.SerializeToString())
  
  def filter_ratings(self, ratings_csv):
    for rating in ratings_csv.ratings:
      if not rating.movieId or rating.movieId < 0 or not rating.rating or rating.rating < 0:
        continue
      
      ratings_pb = files_pb2.RatingsCSV()
      rating_pb = ratings_pb.ratings.add()
      rating_pb.userId = rating.userId
      rating_pb.movieId = rating.movieId
      rating_pb.rating = rating.rating
      rating_pb.timestamp = rating.timestamp

      self.ratings_queue.publish(ratings_pb.SerializeToString(), routing_key=str(rating.movieId))

  def filter_credits(self, credits_csv):
    for credit in credits_csv.credits:
      if not credit.id or credit.id < 0 or not len(credit.cast):
        continue

      names = map(lambda cast: cast.name, credit.cast)
      names = list(filter(lambda name: name, names))
      if not len(names):
        continue

      credits_pb = files_pb2.CreditsCSV()
      credit_pb = credits_pb.credits.add()
      credit_pb.id = credit.id
      
      for name in names:
        cast_pb = credit_pb.cast.add()
        cast_pb.name = name
      
      # Publish the single-entry CreditsCSV message
      self.credits_queue.publish(credits_pb.SerializeToString(), routing_key=str(credit.id))


  def return_results(self, conn: socket.socket):
    queue = RabbitMQ('exchange_snd_movies', 'snd_movies', 'filtered_by_2000', 'direct')
    queue.consume(self.result_controller_func)
  
  def result_controller_func(self, ch, method, properties, body):
    try:
      # data = json.loads(body)
      # logging.info("got result: {body}")
      msg = self.protocol.create_result(body)
      self.socket.sendall(msg)
    except json.JSONDecodeError as e:
      logging.error(f"Failed to decode JSON: {e}")
      return
    except Exception as e:
      logging.error(f"Error processing message: {e}")
      return