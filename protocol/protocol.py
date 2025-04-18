from protocol import files_pb2
from protocol.parsing_proto_utils import *

from enum import Enum

INT_LENGTH = 4
CODE_LENGTH = 1
BOOL_LENGTH = 1

MOVIES_FILE_CODE = 1
RATINGS_FILE_CODE = 2
CREDITS_FILE_CODE = 3

class FileType(Enum):
    MOVIES = 1
    RATINGS = 2
    CREDITS = 3


class Protocol:
  def __init__(self, max_batch_size):
    self.msg_in_creation = None #pb msg
    self.batch_ready = bytearray()
    self.max_batch_size = int(max_batch_size)

    self.__file_classes = {FileType.MOVIES: files_pb2.MoviesCSV(), 
                         FileType.RATINGS: files_pb2.RatingsCSV(), 
                         FileType.CREDITS: files_pb2.CreditsCSV()}
    self.__type_codes = {FileType.MOVIES: MOVIES_FILE_CODE, 
                         FileType.RATINGS: RATINGS_FILE_CODE, 
                         FileType.CREDITS: CREDITS_FILE_CODE}



  def define_initial_buffer_size(self):
    return CODE_LENGTH + INT_LENGTH

  def define_buffer_size(self, msg):
    return int.from_bytes(msg[CODE_LENGTH::], byteorder='big')
  
  def reset_batch_message(self):
    self.batch_ready = self.msg_in_creation
    self.msg_in_creation = None

  def get_batch_msg(self, force_send, type):
    if force_send:
      self.reset_batch_message()
    
    batch = self.batch_ready.SerializeToString()
    len_batch = len(batch)
    if len_batch == 0:
      return bytearray()
    
    message = bytearray()
    code = self.__type_codes.get(type)
    message.extend(code.to_bytes(CODE_LENGTH, byteorder='big'))
    message.extend(len_batch.to_bytes(INT_LENGTH, byteorder='big'))
    message.extend(batch)

    return message

  def add_to_batch(self, type, data):
    is_ready = False
    msg_data = self.update_msg(type, data)
    parsed = msg_data.SerializeToString() #actually bytes, str is a container
    new_len = len(parsed) + CODE_LENGTH + INT_LENGTH ## full msg len
    if new_len > self.max_batch_size:
        self.reset_batch_message()
        is_ready = True
        msg_data = self.update_msg(type, data)
    
    self.msg_in_creation = msg_data
    return is_ready
  
  def update_msg(self, type, data):
    if self.msg_in_creation == None:
      self.msg_in_creation = self.__file_classes.get(type)

    if type == FileType.MOVIES:
      return self.update_movies_msg(data)
    elif type == FileType.RATINGS:
      return self.update_ratings_msg(data)
    elif type == FileType.CREDITS:
      return self.update_credits_msg(data)
  

  def update_ratings_msg(self, rating):
    msg = files_pb2.RatingsCSV()
    if self.msg_in_creation.ratings:
      msg.ratings.extend(self.msg_in_creation.ratings)
    rating_pb = files_pb2.RatingCSV()
    rating_pb.userId = to_int(rating.get('userId', -1))
    rating_pb.movieId = to_int(rating.get('movieId', -1))
    rating_pb.rating = to_float(rating.get('rating', -1.0))
    rating_pb.timestamp = to_string(rating.get('timestamp', ''))
    msg.ratings.append(rating_pb)
    return msg
  
  def update_credits_msg(self, credit):
    msg = files_pb2.CreditsCSV()
    if self.msg_in_creation.credits:
      msg.credits.extend(self.msg_in_creation.credits)
    credit_pb = files_pb2.CreditCSV()
    self.create_cast(credit_pb, credit.get('cast', ''))
    self.create_crew(credit_pb, credit.get('crew', ''))
    credit_pb.id = to_int(credit.get('id', -1))
    msg.credits.append(credit_pb)
    return msg
  
  def create_cast(self, credit_pb, cast_list):
    if cast_list == '' or cast_list == None:
      return 
    data_list = self.create_data_list(cast_list)
    for cast in data_list:
      cast_pb = credit_pb.cast.add()
      cast_pb.cast_id = to_int(cast.get('cast_id', -1))
      cast_pb.character = to_string(cast.get('character', ''))
      cast_pb.credit_id = to_string(cast.get('credit_id', ''))
      cast_pb.gender = to_int(cast.get('gender', -1))
      cast_pb.id = to_int(cast.get('id', -1))
      cast_pb.name = to_string(cast.get('name', ''))
      cast_pb.order = to_int(cast.get('order', -1))
      cast_pb.profile_path = to_string(cast.get('profile_path', ''))

  def create_data_list(self, string_data):
    string_data = string_data.replace('[', "")
    string_data = string_data.replace(']', "")
    data_list = string_data.split('}')
    list_values = []
    for data in data_list:
      data = data.replace('{', "")
      values = data.split(",")
      dict_value = dict()
      for val in values:
        key_value = val.split(':')
        if len(key_value) != 2:
          continue
        [key, value] = key_value
        dict_value[key.strip()] = value.strip()
      list_values.append(dict_value)

    return list_values
  
  def create_crew(self, credit_pb, crew_list):
    if crew_list == '' or crew_list == None:
      return 
    data_list = self.create_data_list(crew_list)
    for crew in data_list:
      crew_pb = credit_pb.crew.add()
      crew_pb.credit_id = to_string(crew.get('credit_id', ''))
      crew_pb.department = to_string(crew.get('department', ''))
      crew_pb.gender = to_int(crew.get('gender', -1))
      crew_pb.id = to_int(crew.get('id', -1))
      crew_pb.job = to_string(crew.get('job', ''))
      crew_pb.name = to_string(crew.get('name', ''))
      crew_pb.profile_path = to_string(crew.get('profile_path', ''))


  def update_movies_msg(self, movie):
    msg = files_pb2.MoviesCSV()
    if self.msg_in_creation.movies:
      msg.movies.extend(self.msg_in_creation.movies)
    movie_pb = files_pb2.MovieCSV()
    movie_pb.id = to_int(movie.get('id', -1))
    movie_pb.adult = to_bool(movie.get('adult', 'False'))
    self.create_collection(movie_pb, movie.get('belongs_to_collection', ''))
    self.create_genres(movie_pb, movie.get('genres', ''))
    movie_pb.homepage = to_string(movie.get('homepage', ''))
    movie_pb.imdb_id = to_string(movie.get('imdb_id', ''))
    movie_pb.original_language = to_string(movie.get('original_language', ''))
    movie_pb.original_title = to_string(movie.get('original_title', ''))
    movie_pb.overview = to_string(movie.get('overview', ''))
    movie_pb.popularity = to_float(movie.get('popularity', -1.0))
    movie_pb.poster_path = to_string(movie.get('poster_path', ''))
    self.create_companies(movie_pb, movie.get('companies', ''))
    self.create_countries(movie_pb, movie.get('contries', ''))
    movie_pb.release_date = to_string(movie.get('release_date', ''))
    movie_pb.revenue = to_int(movie.get('revenue', -1))
    movie_pb.runtime = to_float(movie.get('runtime', -1.0))
    self.create_languages(movie_pb, movie.get('languages', ''))
    movie_pb.status = to_string(movie.get('status', ''))
    movie_pb.tagline = to_string(movie.get('tagline', ''))
    movie_pb.title = to_string(movie.get('title', ''))
    if(type(to_bool(movie.get('video', 'False'))) is not bool):
      print(f"Rompio con este valor {movie_pb.title}: {to_bool(movie.get('video', 'False'))} | {type(to_bool(movie.get('video', 'False')))}")
    movie_pb.video = to_bool(movie.get('video', 'False'))
    movie_pb.vote_average = to_float(movie.get('vote_average', -1.0))
    movie_pb.vote_count = to_int(movie.get('vote_count', -1))
    msg.movies.append(movie_pb)
    return msg
  
  def create_collection(self, movie_pb, collection_data):
    data_list = self.create_data_list(collection_data)
    for collection in data_list:
      collection_pb = movie_pb.belongs_to_collection
      collection_pb.id = to_int(collection.get('id', -1))
      collection_pb.name = to_string(collection.get('name', ''))
      collection_pb.poster_path = to_string(collection.get('poster_path', ''))
      collection_pb.backdrop_path = to_string(collection.get('backdrop_path', ''))

  def create_genres(self, movie_pb, genres_data):
    data_list = self.create_data_list(genres_data)
    for genre in data_list:
      genre_pb = movie_pb.genres.add()
      genre_pb.id = to_int(genre.get('id', -1))
      genre_pb.name = to_string(genre.get('name', ''))

  def create_companies(self, movie_pb, companies_data):
    data_list = self.create_data_list(companies_data)
    for company in data_list:
      company_pb = movie_pb.companies.add()
      company_pb.id = to_int(company.get('id', -1))
      company_pb.name = to_string(company.get('name', ''))

  def create_countries(self, movie_pb, countries_data):
    data_list = self.create_data_list(countries_data)
    for country in data_list:
      country_pb = movie_pb.countries.add()
      country_pb.iso_3166_1 = to_string(country.get('iso_3166_1', ''))
      country_pb.name = to_string(country.get('name', ''))
  
  def create_languages(self, movie_pb, languages_data):
    data_list = self.create_data_list(languages_data)
    for language in data_list:
      language_pb = movie_pb.languages.add()
      language_pb.iso_639_1 = to_string(language.get('iso_639_1', ''))
      language_pb.name = to_string(language.get('name', ''))

