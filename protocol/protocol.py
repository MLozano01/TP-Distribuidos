import csv
import io
from protocol import files_pb2
from protocol.utils.parsing_proto_utils import *
from enum import Enum
import logging

INT_LENGTH = 4
CODE_LENGTH = 1
BOOL_LENGTH = 1

MOVIES_FILE_CODE = 1
RATINGS_FILE_CODE = 2
CREDITS_FILE_CODE = 3

RESULT_CODE = 4
END_FILE_CODE = 5

class FileType(Enum):
    MOVIES = 1
    RATINGS = 2
    CREDITS = 3


class Protocol:
  # Headers for each file type
  MOVIES_HEADERS = ["adult", "belongs_to_collection", "budget", "genres", "homepage", "id", "imdb_id", 
                   "original_language", "original_title", "overview", "popularity", "poster_path", 
                   "production_companies", "production_countries", "release_date", "revenue", "runtime", 
                   "spoken_languages", "status", "tagline", "title", "video", "vote_average", "vote_count"]
  
  RATINGS_HEADERS = ["userId", "movieId", "rating", "timestamp"]
  
  CREDITS_HEADERS = ["cast", "crew", "id"]

  def __init__(self):
    self.__file_classes = {FileType.MOVIES: files_pb2.MoviesCSV(), 
                         FileType.RATINGS: files_pb2.RatingsCSV(), 
                         FileType.CREDITS: files_pb2.CreditsCSV()}
    self.__type_codes = {FileType.MOVIES: MOVIES_FILE_CODE, 
                         FileType.RATINGS: RATINGS_FILE_CODE, 
                         FileType.CREDITS: CREDITS_FILE_CODE}
    self.__codes_to_types = {MOVIES_FILE_CODE: FileType.MOVIES, 
                            RATINGS_FILE_CODE: FileType.RATINGS, 
                            CREDITS_FILE_CODE: FileType.CREDITS}
    self.__codes_to_string = {MOVIES_FILE_CODE: "movies", 
                            RATINGS_FILE_CODE: "ratings", 
                            CREDITS_FILE_CODE: "credits"}
    self.__headers = {
      FileType.MOVIES: self.MOVIES_HEADERS,
      FileType.RATINGS: self.RATINGS_HEADERS,
      FileType.CREDITS: self.CREDITS_HEADERS
    }
    self.__headers_dict = {
      FileType.MOVIES: {h: i for i, h in enumerate(self.MOVIES_HEADERS)},
      FileType.RATINGS: {h: i for i, h in enumerate(self.RATINGS_HEADERS)},
      FileType.CREDITS: {h: i for i, h in enumerate(self.CREDITS_HEADERS)}
    }



  def define_initial_buffer_size(self):
    return CODE_LENGTH + INT_LENGTH

  def define_buffer_size(self, msg):
    return int.from_bytes(msg[CODE_LENGTH::], byteorder='big')

  def create_batch(self, type, lines):
    msg_pb = files_pb2.CSVBatch()
    for line in lines:
      msg_pb.rows.append(line)
    
    batch = msg_pb.SerializeToString()
    len_batch = len(batch)
    if len_batch == 0:
      return bytearray()
    
    code = self.__type_codes.get(type)
    return self.create_bytes(code, batch)

  def create_inform_end_file(self, type,force=False):
    code = self.__type_codes.get(type)
    msg = bytearray()
    msg.extend(code.to_bytes(CODE_LENGTH, byteorder='big'))
    if force:
      msg.extend(force.to_bytes(BOOL_LENGTH, byteorder='big'))
    return self.create_bytes(END_FILE_CODE, msg)

  def create_finished_message(self, type, client_id, force=False):
      """Creates and serializes a 'finished' message for the given type."""
      msg = self.__file_classes[type]
      msg.finished = True
      msg.client_id = client_id
      msg.force_finish = force

      serialized_msg = msg.SerializeToString()
      code = self.__type_codes.get(type)
      return self.create_bytes(code, serialized_msg)


  def create_bytes(self, code, data):
    message = bytearray()
    len_msg = len(data)
    message.extend(code.to_bytes(CODE_LENGTH, byteorder='big'))
    message.extend(len_msg.to_bytes(INT_LENGTH, byteorder='big'))
    message.extend(data)
    return message

  def get_file_type(self, message):
    """Reads the message and returns the file type of the data received."""
    code = int.from_bytes(message[:CODE_LENGTH], byteorder='big')
    data = message[CODE_LENGTH + INT_LENGTH:]
  
    type = int.from_bytes(data[:CODE_LENGTH], byteorder='big') if code == END_FILE_CODE else code
    
    return code == END_FILE_CODE, self.__codes_to_string[type]

  def is_end_file(self, message):
    """Return True if *message* is an END_FILE control message."""
    if not message:
      return False
    return int.from_bytes(message[:CODE_LENGTH], byteorder='big') == END_FILE_CODE

  def string_to_file_type(self, s):
    """Map 'ratings'/'credits'/'movies' → FileType enum (or raise KeyError)."""
    code = {v: k for k, v in self.__codes_to_string.items()}[s]
    return self.__codes_to_types[code]

  def add_metadata(self, message, client_id, secuence_number):
    """Adds client ID to the message to call before forwarding to the distributed system."""
    code = int.from_bytes(message[:CODE_LENGTH], byteorder='big')
    length = int.from_bytes(message[CODE_LENGTH:CODE_LENGTH + INT_LENGTH], byteorder='big')
    data = message[CODE_LENGTH + INT_LENGTH:]
    
    # Add metadata to the data
    client_id_bytes = client_id.to_bytes(INT_LENGTH, byteorder='big')
    secuence_number_bytes = secuence_number.to_bytes(INT_LENGTH, byteorder='big')
    new_data = client_id_bytes + secuence_number_bytes + data
    
    # Create new message with updated length
    new_message = bytearray()
    new_message.extend(code.to_bytes(CODE_LENGTH, byteorder='big'))
    new_message.extend((length + 2 * INT_LENGTH).to_bytes(INT_LENGTH, byteorder='big'))
    new_message.extend(new_data)
    
    return new_message

  def decode_client_msg(self, msg_buffer, columns):
    code = int.from_bytes(msg_buffer[:CODE_LENGTH], byteorder='big')
    amount_read = CODE_LENGTH
    length = int.from_bytes(msg_buffer[amount_read:amount_read + INT_LENGTH], byteorder='big')
    amount_read += INT_LENGTH 
    # Extract client ID
    client_id = int.from_bytes(msg_buffer[amount_read:amount_read + INT_LENGTH], byteorder='big')
    amount_read += INT_LENGTH 
    secuence_number = int.from_bytes(msg_buffer[amount_read:amount_read + INT_LENGTH], byteorder='big')
    amount_read += INT_LENGTH 
    
    msg = msg_buffer[amount_read:]
    
    if code == END_FILE_CODE:
      cursor = 0
      file_code = int.from_bytes(msg[cursor:cursor + CODE_LENGTH], byteorder='big')
      cursor += CODE_LENGTH

      # Default when the sender provided no total counter (backwards compat.)
      total_to_process = None

      # If there are at least 4 extra bytes, read them as total_to_process.
      if len(msg) >= cursor + INT_LENGTH:
        total_to_process = int.from_bytes(
          msg[cursor:cursor + INT_LENGTH], byteorder='big'
        )

      file_type = self.__codes_to_types[file_code]
      msg_finished = self.__file_classes[file_type]
      msg_finished.finished = True
      if len(msg[CODE_LENGTH:]) > 0:
        msg_finished.force_finish = True
      else:
        msg_finished.force_finish = False
      msg_finished.client_id = client_id
      msg_finished.secuence_number = secuence_number

      # Only RatingsCSV and CreditsCSV currently support this extra field.
      if total_to_process is not None and hasattr(msg_finished, "total_to_process"):
        msg_finished.total_to_process = total_to_process

      return file_type, msg_finished

    file_type = self.__codes_to_types[code]
    # First decode the CSV batch without client_id
    csv_batch = files_pb2.CSVBatch()
    csv_batch.ParseFromString(msg)
    
    # Then process the batch and add client_id to the final message
    batch = self.lines_to_batch(csv_batch, columns, file_type)
    batch.client_id = client_id
    batch.secuence_number = secuence_number
    return file_type, batch

  def lines_to_batch(self, csv_batch, columns, file_type):
    ProtoClass = type(self.__file_classes[file_type])
    data_csv = ProtoClass()

    for line in csv_batch.rows:
      data = self.parse_line(line, file_type)
      line_parsed = None
      is_added = False
      if file_type == FileType.MOVIES:
        line_parsed, is_added = self.update_movies_msg(data, columns['movies'])
      elif file_type == FileType.RATINGS:
        line_parsed, is_added = self.update_ratings_msg(data, columns['ratings'])
      elif file_type == FileType.CREDITS:
        line_parsed, is_added = self.update_credits_msg(data, columns['credits'])
      
      if is_added:
        if file_type == FileType.MOVIES:
          data_csv.movies.append(line_parsed)
        elif file_type == FileType.RATINGS:
          data_csv.ratings.append(line_parsed)
        elif file_type == FileType.CREDITS:
          data_csv.credits.append(line_parsed)
    return data_csv

  def begins_field(self, part):
    #tiene tada, no arranca con " o arranca con "[
    val= (part and part[0] != ' ' and part != '"') or part.startswith('"[') or part.startswith('"{')
    return val
  
  def parse_line(self, line, file_type):

    try:
      string_io = io.StringIO(line)
      reader = csv.reader(string_io)
      data = dict()
      for row in reader:
        for index, data_field in enumerate(row):
          header = self.__headers[file_type][index]
          data[header] = data_field
    except Exception as e:
      return dict()
    return data


  def update_ratings_msg(self, rating, columns):
    rating_pb = files_pb2.RatingCSV()
    if self.drop_row(rating, columns):
      return rating_pb, False
    rating_pb.userId = to_int(rating.get('userId', -1))
    rating_pb.movieId = to_int(rating.get('movieId', -1))
    rating_pb.rating = to_float(rating.get('rating', -1.0))
    rating_pb.timestamp = to_string(rating.get('timestamp', ''))
    return rating_pb, True
  
  def drop_row(self, row, columns):
    row_keys = row.keys()
    for column in columns:
      if column not in row_keys:
        return True
    return False
  
  def update_credits_msg(self, credit, columns):
    credit_pb = files_pb2.CreditCSV()
    if self.drop_row(credit, columns):
      return credit_pb, False
      
    self.create_cast(credit_pb, credit.get('cast', ''))
    self.create_crew(credit_pb, credit.get('crew', ''))
    credit_pb.id = to_int(credit.get('id', -1))
    return credit_pb, True
  
  def create_cast(self, credit_pb, cast_list):
    if cast_list == '' or cast_list == None:
      return 
    data_list = create_data_list(cast_list)
    for cast in data_list:
      if not cast:
        return
      cast_pb = credit_pb.cast.add()
      cast_pb.cast_id = to_int(cast.get('cast_id', -1))
      cast_pb.character = to_string(cast.get('character', ''))
      cast_pb.credit_id = to_string(cast.get('credit_id', ''))
      cast_pb.gender = to_int(cast.get('gender', -1))
      cast_pb.id = to_int(cast.get('id', -1))
      cast_pb.name = to_string(cast.get('name', ''))
      cast_pb.order = to_int(cast.get('order', -1))
      cast_pb.profile_path = to_string(cast.get('profile_path', ''))
  
  def create_crew(self, credit_pb, crew_list):
    if crew_list == '' or crew_list == None:
      return 
    data_list = create_data_list(crew_list)
    for crew in data_list:
      if not crew:
        return
      crew_pb = credit_pb.crew.add()
      crew_pb.credit_id = to_string(crew.get('credit_id', ''))
      crew_pb.department = to_string(crew.get('department', ''))
      crew_pb.gender = to_int(crew.get('gender', -1))
      crew_pb.id = to_int(crew.get('id', -1))
      crew_pb.job = to_string(crew.get('job', ''))
      crew_pb.name = to_string(crew.get('name', ''))
      crew_pb.profile_path = to_string(crew.get('profile_path', ''))


  def update_movies_msg(self, movie, columns):
    movie_pb = files_pb2.MovieCSV()
    if self.drop_row(movie, columns):
      return movie_pb, False
    movie_pb.id = to_int(movie.get('id', -1))
    movie_pb.adult = to_bool(movie.get('adult', 'False'))
    self.create_collection(movie_pb, movie.get('belongs_to_collection', ''))
    movie_pb.budget = to_int(movie.get('budget', -1))
    self.create_genres(movie_pb, movie.get('genres', ''))
    movie_pb.homepage = to_string(movie.get('homepage', ''))
    movie_pb.imdb_id = to_string(movie.get('imdb_id', ''))
    movie_pb.original_language = to_string(movie.get('original_language', ''))
    movie_pb.original_title = to_string(movie.get('original_title', ''))
    movie_pb.overview = to_string(movie.get('overview', ''))
    movie_pb.popularity = to_float(movie.get('popularity', -1.0))
    movie_pb.poster_path = to_string(movie.get('poster_path', ''))
    self.create_companies(movie_pb, movie.get('production_companies', ''))
    self.create_countries(movie_pb, movie.get('production_countries', ''))
    movie_pb.release_date = to_string(movie.get('release_date', ''))
    movie_pb.revenue = to_int(movie.get('revenue', -1))
    movie_pb.runtime = to_float(movie.get('runtime', -1.0))
    self.create_languages(movie_pb, movie.get('spoken_languages', ''))
    movie_pb.status = to_string(movie.get('status', ''))
    movie_pb.tagline = to_string(movie.get('tagline', ''))
    movie_pb.title = to_string(movie.get('title', ''))
    movie_pb.video = to_bool(movie.get('video', 'False'))
    movie_pb.vote_average = to_float(movie.get('vote_average', -1.0))
    movie_pb.vote_count = to_int(movie.get('vote_count', -1))
    return movie_pb, True
  
  def create_collection(self, movie_pb, collection_data):
    data_list = create_data_list(collection_data)
    collection = data_list[0]
    if not collection:
      return
    collection_pb = movie_pb.belongs_to_collection
    collection_pb.id = to_int(collection.get('id', -1))
    collection_pb.name = to_string(collection.get('name', ''))
    collection_pb.poster_path = to_string(collection.get('poster_path', ''))
    collection_pb.backdrop_path = to_string(collection.get('backdrop_path', ''))

  def create_genres(self, movie_pb, genres_data):
    data_list = create_data_list(genres_data)
    for genre_dict in data_list:
      if not genre_dict:
        continue
      
      genre_pb = movie_pb.genres.add()
      genre_pb.id = to_int(genre_dict.get('id', -1))
      genre_pb.name = to_string(genre_dict.get('name', ''))

  def create_companies(self, movie_pb, companies_data):
    data_list = create_data_list(companies_data)
    for company in data_list:
      if not company:
        return
      company_pb = movie_pb.companies.add()
      company_pb.id = to_int(company.get('id', -1))
      company_pb.name = to_string(company.get('name', ''))

  def create_countries(self, movie_pb, countries_data):
    data_list = create_data_list(countries_data)
    for country in data_list:
      if not country:
        return
      country_pb = movie_pb.countries.add()
      country_pb.iso_3166_1 = to_string(country.get('iso_3166_1', ''))
      country_pb.name = to_string(country.get('name', ''))
  
  def create_languages(self, movie_pb, languages_data):
    data_list = create_data_list(languages_data)
    for language in data_list:
      if not language:
        return
      language_pb = movie_pb.languages.add()
      language_pb.iso_639_1 = to_string(language.get('iso_639_1', ''))
      language_pb.name = to_string(language.get('name', ''))


  def decode_msg(self, msg_buffer):
    code = int.from_bytes(msg_buffer[:CODE_LENGTH], byteorder='big')

    msg = msg_buffer[CODE_LENGTH + INT_LENGTH::]
    if code == MOVIES_FILE_CODE:
      return FileType.MOVIES, self.decode_movies_msg(msg)
    elif code == RATINGS_FILE_CODE:
      return FileType.RATINGS, self.decode_ratings_msg(msg)
    elif code == CREDITS_FILE_CODE:
      return FileType.CREDITS, self.decode_credits_msg(msg)
    elif code == RESULT_CODE:
      return None, self.decode_result(msg)

  
  def decode_movies_msg(self, msg_buffer):
    movies = files_pb2.MoviesCSV()
    movies.ParseFromString(msg_buffer)
    return movies
  
  def decode_ratings_msg(self, msg_buffer):
    ratings = files_pb2.RatingsCSV()
    ratings.ParseFromString(msg_buffer)
    return ratings
  
  def decode_credits_msg(self, msg_buffer):
    credits = files_pb2.CreditsCSV()
    credits.ParseFromString(msg_buffer)
    return credits
  
  def decode_joined_ratings_batch(self, buffer):
      """Deserializes a JoinedRatingsBatch message."""
      try:
          batch = files_pb2.JoinedRatingsBatch()
          batch.ParseFromString(buffer)
          return batch
      except Exception as e:
          logging.error(f"Error decoding JoinedRatingsBatch: {e}")
          return None

  def encode_joined_rating_msg(self, client_id, movie_id, title, rating, timestamp):
      """Encodes a single joined rating into a JoinedRatingsBatch of one."""
      rating_pb = files_pb2.JoinedRating(
          movie_id=movie_id,
          title=title,
          rating=rating,
          timestamp=timestamp,
      )
      batch_pb = files_pb2.JoinedRatingsBatch(client_id=client_id)
      batch_pb.ratings.append(rating_pb)
      return batch_pb.SerializeToString()

  def create_client_result(self, data):
    message = bytearray()
    len_msg = len(data)
    message.extend(RESULT_CODE.to_bytes(CODE_LENGTH, byteorder='big'))
    message.extend(len_msg.to_bytes(INT_LENGTH, byteorder='big'))
    message.extend(data)
    return message
  
  def create_movie_finished_msg(self, client_id, force=False):
    movies_pb = files_pb2.MoviesCSV()
    movies_pb.finished = True
    movies_pb.force_finish = force
    movies_pb.client_id = client_id
    return movies_pb.SerializeToString()
  
  def create_actor_participations_finished_msg(self, client_id, force=False):
    actor_participations_pb = files_pb2.ActorParticipationsBatch()
    actor_participations_pb.finished = True
    actor_participations_pb.force_finish = force
    actor_participations_pb.client_id = client_id
    return actor_participations_pb.SerializeToString()

  def create_movie_list(self, movies, client_id, secuence_number):
    movies_pb = files_pb2.MoviesCSV()

    for movie in movies:
      movies_pb.movies.append(movie)

    movies_pb.client_id = client_id
    movies_pb.secuence_number = secuence_number
    movies_pb_str = movies_pb.SerializeToString()
    return movies_pb_str

  def create_aggr_batch(self, dict_results, client_id, secuence_number):
    batch_pb = files_pb2.AggregationBatch()

    batch_pb.client_id = client_id
    batch_pb.secuence_number = secuence_number

    for key, results in dict_results.items():
      aggr_pb = batch_pb.aggr_row.add()
      aggr_pb.key = str(key)
      if "sum" in results:
        aggr_pb.sum =  to_float(results.get("sum"))
      if "count" in results:
        aggr_pb.count =  to_int(results.get("count"))
    batch_pb_str = batch_pb.SerializeToString()
    return batch_pb_str

  def decode_aggr_batch(self, buffer):
    aggr = files_pb2.AggregationBatch()
    aggr.ParseFromString(buffer)
    return aggr

  def create_actor_participations_batch(self, participations, client_id):
      """Creates and serializes an ActorParticipationsBatch message."""
      batch = files_pb2.ActorParticipationsBatch()
      batch.participations.extend(participations)
      batch.client_id = client_id
      return batch.SerializeToString()

  def create_finished_actor_participations_msg(self, force=False):
      """Creates and serializes an ActorParticipationsBatch message with finished=True."""
      batch_pb = files_pb2.ActorParticipationsBatch()
      batch_pb.finished = True
      batch_pb.force_finish = force
      return batch_pb.SerializeToString()
  
  def create_finished_movies_msg(self, force=False):
      """Creates and serializes an MoviesCsv message with finished=True."""
      movies_pb = files_pb2.MoviesCSV()
      movies_pb.finished = True
      movies_pb.force_finish = force
      return movies_pb.SerializeToString()

  def decode_actor_participations_batch(self, buffer):
      """Deserializes an ActorParticipationsBatch message."""
      batch_pb = files_pb2.ActorParticipationsBatch()
      batch_pb.ParseFromString(buffer)
      return batch_pb

  def create_result(self, dict_results, client_id, final=False):
    batch_pb = files_pb2.ResultBatch()
    batch_pb.client_id = client_id

    if final:
      batch_pb.final = True

    for key, results in dict_results.items():
      if key == "country":
        batch_pb.query_id = 2
        for country, value in results.items():
          res_pb = batch_pb.result_row.add()
          res_pb.country = country
          res_pb.sum = to_float(value)
      elif key == "actor":
        batch_pb.query_id = 4
        for actor, value in results.items():
          res_pb = batch_pb.result_row.add()
          res_pb.actor_name = actor
          res_pb.sum = to_float(value)
      elif key == "max-min":
        batch_pb.query_id = 3
        for title, value in results.items():
          res_pb = batch_pb.result_row.add()
          res_pb.title = title
          res_pb.average = to_float(value)
      elif key == "sentiment":
        batch_pb.query_id = 5
        for sentiment, value in results.items():
          res_pb = batch_pb.result_row.add()
          res_pb.sentiment = sentiment
          res_pb.average = to_float(value)
      elif key == "movies":
        batch_pb.query_id = 1
        for title, value in results.items():
          res_pb = batch_pb.result_row.add()
          res_pb.title = title
          if isinstance(value, list):
              res_pb.genres.extend(value)

    batch_pb_str = batch_pb.SerializeToString()
    return batch_pb_str
  
  def decode_result(self, buffer):
    result = files_pb2.ResultBatch()
    result.ParseFromString(buffer)
    return result

  def encode_movies_msg(self, movies_list, client_id, finished=False, force=False):
      """Encodes a list of MovieCSV objects into a serialized MoviesCSV message.

      Args:
          movies_list (list[files_pb2.MovieCSV]): The list of movies to include in the batch.
          client_id (int): The client identifier.
          finished (bool): Whether this is a FINISHED signal (defaults to False).

      Returns:
          bytes: Serialized MoviesCSV protobuf message.
      """
      movies_pb = files_pb2.MoviesCSV()
      if movies_list:
          movies_pb.movies.extend(movies_list)
      movies_pb.client_id = client_id
      if finished:
          movies_pb.finished = True
          movies_pb.force_finish = force
      return movies_pb.SerializeToString()

  def encode_actor_participations_msg(self, participations_list, client_id, finished=False, force=False):
      """Encodes a list of ActorParticipation objects into a serialized ActorParticipationsBatch.

      Args:
          participations_list (list[files_pb2.ActorParticipation]): Actor participations to include.
          client_id (int): The client identifier.
          finished (bool): Whether this is a FINISHED signal (defaults to False).

      Returns:
          bytes: Serialized ActorParticipationsBatch protobuf message.
      """
      batch_pb = files_pb2.ActorParticipationsBatch()
      if participations_list:
          batch_pb.participations.extend(participations_list)
      batch_pb.client_id = client_id
      if finished:
          batch_pb.finished = True
          batch_pb.force_finish = force
      return batch_pb.SerializeToString()


  def count_csv_rows(self, message):
    """Return the number of CSV rows in a *ratings* or *credits* batch.

    For non-ratings/credits messages, or END_FILE messages, returns 0.
    """
    if not message:
      return 0

    code = int.from_bytes(message[:CODE_LENGTH], byteorder='big')
    if code not in (RATINGS_FILE_CODE, CREDITS_FILE_CODE):
      return 0

    try:
      length = int.from_bytes(message[CODE_LENGTH:CODE_LENGTH + INT_LENGTH], byteorder='big')
      data = message[CODE_LENGTH + INT_LENGTH : CODE_LENGTH + INT_LENGTH + length]
      csv_batch = files_pb2.CSVBatch()
      csv_batch.ParseFromString(data)
      return len(csv_batch.rows)
    except Exception:
      return 0
