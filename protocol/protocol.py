from protocol import files_pb2

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
    self.max_batch_size = max_batch_size

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
  

  def add_to_batch(self, type, data):
    is_ready = False
    msg_data = self.update_msg(type, data)
    print(f"msg_data: {msg_data}")
    parsed = msg_data.SerializeToString() #actually bytes, str is a container
    new_len = len(parsed) + CODE_LENGTH + INT_LENGTH ## full msg len

    if new_len > self.max_batch_size:
        self.reset_batch_message()
        is_ready = True
        msg_data = self.updated_msg(type, data)
    
    self.msg_in_creation = msg_data
    return is_ready
  
  def update_msg(self, type, data):
    if self.msg_in_creation == None:
      self.msg_in_creation = self.__file_classes.get(type)

    if type == FileType.MOVIES:
      self.update_movies_msg(data)
    elif type == FileType.RATINGS:
      self.update_ratings_msg(data)
    elif type == FileType.CREDITS:
      self.update_credits_msg(data)
  

  def update_ratings_msg(self, rating):
    msg = files_pb2.RatingsCSV()
    if self.msg_in_creation.ratings:
      msg.ratings.extend(self.msg_in_creation.ratings)
    rating_pb = files_pb2.RatingCSV()
    rating_pb.userId = rating.get('userId', -1)
    rating_pb.movieId = rating.get('movieId', -1)
    rating_pb.rating = rating.get('rating', -1.0)
    rating_pb.timestamp = rating.get('timestamp', '')
    msg.ratings.append(rating_pb)
    return msg
  
  def update_credits_msg(self, rating):
    msg = files_pb2.CreditsCSV()
    if self.msg_in_creation.credits:
      msg.credits.extend(self.msg_in_creation.credits)
    credit_pb = files_pb2.CreditCSV()
    credit_pb.cast = rating.get('cast', '')
    credit_pb.crew = rating.get('crew', '')
    credit_pb.id = rating.get('id', -1)
    msg.credits.append(credit_pb)
    return msg

  def update_movies_msg(self, movie):
    msg = files_pb2.MoviesCSV()
    if self.msg_in_creation.movies:
      msg.movies.extend(self.msg_in_creation.movies)
    movie_pb = files_pb2.MovieCSV()
    movie_pb.id = movie_pb.get('id', -1)
    movie_pb.adult = movie_pb.get('adult', False)
    movie_pb.belongs_to_collection = movie_pb.get('belongs_to_collection', '')
    #TODO: genres
    movie_pb.homepage = movie_pb.get('homepage', '')
    movie_pb.imdb_id = movie_pb.get('imdb_id', '')
    movie_pb.original_language = movie_pb.get('original_language', '')
    movie_pb.original_title = movie_pb.get('original_title', '')
    movie_pb.overview = movie_pb.get('overview', '')
    movie_pb.popularity = movie_pb.get('popularity', -1.0)
    movie_pb.poster_path = movie_pb.get('poster_path', '')
    #TODO: companies and countries
    movie_pb.release_date = movie_pb.get('release_date', '')
    movie_pb.revenue = movie_pb.get('revenue', -1.0)
    movie_pb.runtime = movie_pb.get('runtime', -1.0)
    #TODO: languages
    movie_pb.status = movie_pb.get('status', '')
    movie_pb.tagline = movie_pb.get('tagline', '')
    movie_pb.title = movie_pb.get('title', '')
    movie_pb.video = movie_pb.get('video', False)
    movie_pb.vote_average = movie_pb.get('vote_average', -1.0)
    movie_pb.vote_count = movie_pb.get('vote_count', -1)
    msg.movies.append(movie_pb)
    return msg
  

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
