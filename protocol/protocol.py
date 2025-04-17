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
  

  def add_to_batch(self, type, data):
    is_ready = False
    msg_data = self.update_msg(type, data)
    parsed = msg_data.SerializeToString() #actually bytes, str is a container
    new_len = len(parsed) + CODE_LENGTH + INT_LENGTH ## full msg len
    print(f"new len: {new_len} | max: {self.max_batch_size}")
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
    rating_pb.userId = rating.get('userId', -1)
    rating_pb.movieId = rating.get('movieId', -1)
    rating_pb.rating = rating.get('rating', -1.0)
    rating_pb.timestamp = rating.get('timestamp', '')
    msg.ratings.append(rating_pb)
    return msg
  
  def update_credits_msg(self, credit):
    msg = files_pb2.CreditsCSV()
    if self.msg_in_creation.credits:
      msg.credits.extend(self.msg_in_creation.credits)
    credit_pb = files_pb2.CreditCSV()
    credit_pb.cast = credit.get('cast', '')
    credit_pb.crew = credit.get('crew', '')
    credit_pb.id = int(credit.get('id', -1))
    msg.credits.append(credit_pb)
    return msg

  def update_movies_msg(self, movie):
    msg = files_pb2.MoviesCSV()
    if self.msg_in_creation.movies:
      msg.movies.extend(self.msg_in_creation.movies)
    movie_pb = files_pb2.MovieCSV()
    movie_pb.id = movie.get('id', -1)
    movie_pb.adult = movie.get('adult', False)
    movie_pb.belongs_to_collection = movie.get('belongs_to_collection', '')
    #TODO: genres
    movie_pb.homepage = movie.get('homepage', '')
    movie_pb.imdb_id = movie.get('imdb_id', '')
    movie_pb.original_language = movie.get('original_language', '')
    movie_pb.original_title = movie.get('original_title', '')
    movie_pb.overview = movie.get('overview', '')
    movie_pb.popularity = movie.get('popularity', -1.0)
    movie_pb.poster_path = movie.get('poster_path', '')
    #TODO: companies and countries
    movie_pb.release_date = movie.get('release_date', '')
    movie_pb.revenue = movie.get('revenue', -1.0)
    movie_pb.runtime = movie.get('runtime', -1.0)
    #TODO: languages
    movie_pb.status = movie.get('status', '')
    movie_pb.tagline = movie.get('tagline', '')
    movie_pb.title = movie.get('title', '')
    movie_pb.video = movie.get('video', False)
    movie_pb.vote_average = movie.get('vote_average', -1.0)
    movie_pb.vote_count = movie.get('vote_count', -1)
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

    return message
