from protocol import files_pb2
from protocol.utils.parsing_proto_utils import *
from enum import Enum

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
    self.current_file_type = None
    self.current_headers = []
    self.current_headers_dict = dict()



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

  def create_inform_end_file(self, type):
    code = self.__type_codes.get(type)
    msg = code.to_bytes(CODE_LENGTH, byteorder='big')
    return self.create_bytes(END_FILE_CODE, msg)

  def create_finished_message(self, type):
      """Creates and serializes a 'finished' message for the given type."""
      msg = self.__file_classes[type]
      msg.finished = True

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


  def decode_client_msg(self, msg_buffer):
    code = int.from_bytes(msg_buffer[:CODE_LENGTH], byteorder='big')

    msg = msg_buffer[CODE_LENGTH + INT_LENGTH::]
    if code == END_FILE_CODE:
      file_code = int.from_bytes(msg[:CODE_LENGTH], byteorder='big')
      file_type = self.__codes_to_types[file_code]
      return file_type, self.create_finished_message(file_type)

    file_type = self.__codes_to_types[code]
    if file_type != self.current_file_type:
      self.current_headers = []
      self.current_file_type = file_type
      self.current_headers_dict = dict()

    return file_type, self.lines_to_batch(msg)
  
  def lines_to_batch(self, line_bytes):
    lines = files_pb2.CSVBatch()
    lines.ParseFromString(line_bytes)
    if not len(self.current_headers):
      self.set_headers(lines.rows.pop(0))
    
    data_csv = self.__file_classes[self.current_file_type]
    for line in lines.rows:
      data = self.parse_line(line)
      if self.current_file_type == FileType.MOVIES:
        data_csv.movies.append(self.update_movies_msg(data))
      elif self.current_file_type == FileType.RATINGS:
        data_csv.ratings.append(self.update_ratings_msg(data))
      elif self.current_file_type == FileType.CREDITS:
        data_csv.credits.append(self.update_credits_msg(data))
    return data_csv
  
  def set_headers(self, headers):
    self.current_headers = headers.strip('\n').split(',')
    self.current_headers_dict = dict()
    for ind, header in enumerate(self.current_headers):
      self.current_headers_dict[header] = ind

  def parse_line(self, line):
    parts = line.split(',')
    data = dict()
    index = -1
    for part in parts:

      if not part or part[0] != ' ':
        index+=1
      
      data.setdefault(self.current_headers[index], "")
      cleaned = part.strip(" '\\").strip('" \n')
      if part and part[0] == ' ':
        cleaned = "," + part
      data[self.current_headers[index]] += cleaned
    
    return data


  def update_ratings_msg(self, rating):
    rating_pb = files_pb2.RatingCSV()
    rating_pb.userId = to_int(rating.get('userId', -1))
    rating_pb.movieId = to_int(rating.get('movieId', -1))
    rating_pb.rating = to_float(rating.get('rating', -1.0))
    rating_pb.timestamp = to_string(rating.get('timestamp', ''))
    return rating_pb
  
  def update_credits_msg(self, credit):
    credit_pb = files_pb2.CreditCSV()
    self.create_cast(credit_pb, credit.get('cast', ''))
    self.create_crew(credit_pb, credit.get('crew', ''))
    credit_pb.id = to_int(credit.get('id', -1))
    return credit_pb
  
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


  def update_movies_msg(self, movie):
    movie_pb = files_pb2.MovieCSV()
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
    return movie_pb
  
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
    for genre in data_list:
      if not genre:
        return
      genre_pb = movie_pb.genres.add()
      genre_pb.id = to_int(genre.get('id', -1))
      genre_pb.name = to_string(genre.get('name', ''))

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
  

  def create_result(self, data):
    return self.create_bytes(RESULT_CODE, data)

  def decode_result(self, buffer):
    msg = buffer[CODE_LENGTH + INT_LENGTH::]
    return self.decode_movies_msg(msg)
  
  def create_movie_list(self, movies):
    movies_pb = files_pb2.MoviesCSV()

    for movie in movies:
      movies_pb.movies.append(movie)

    movies_pb_str = movies_pb.SerializeToString()
    return movies_pb_str

  def create_aggr_batch(self, dict_results):
    batch_pb = files_pb2.AggregationBatch()

    for key, results in dict_results.items():
      aggr_pb = batch_pb.aggr_row.add()
      aggr_pb.key = key
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

  def create_actor_participations_batch(self, participations):
      """Creates and serializes an ActorParticipationsBatch message."""
      batch_pb = files_pb2.ActorParticipationsBatch()
      # participations should be a list of ActorParticipation objects
      batch_pb.participations.extend(participations)
      return batch_pb.SerializeToString()

  def decode_actor_participations_batch(self, buffer):
      """Deserializes an ActorParticipationsBatch message."""
      batch_pb = files_pb2.ActorParticipationsBatch()
      batch_pb.ParseFromString(buffer)
      return batch_pb

