from protocol import files_pb2
from protocol.utils.parsing_proto_utils import is_date
import logging

def filter_movies(movies_csv):
    movies_pb = files_pb2.MoviesCSV()
    movies_pb.client_id = movies_csv.client_id
    movies_pb.secuence_number = movies_csv.secuence_number
    for movie in movies_csv.movies:
        if not movie.id or movie.id < 0 or not movie.release_date or not movie.overview:
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

    return movies_pb if len(movies_pb.movies) else None

def filter_ratings(ratings_csv):
    ratings_batch = files_pb2.RatingsCSV()
    ratings_batch.client_id = ratings_csv.client_id
    total_ratings = len(ratings_csv.ratings)
    filtered_out = 0
    
    for rating in ratings_csv.ratings:
        if not rating.movieId or rating.movieId < 0 or rating.rating is None or rating.rating < 0:
            filtered_out += 1
            continue

        rating_pb = files_pb2.RatingCSV()
        rating_pb.userId = rating.userId
        rating_pb.movieId = rating.movieId
        rating_pb.rating = rating.rating

        ratings_batch.ratings.append(rating_pb)

    logging.debug(f"Filtered {filtered_out} out of {total_ratings} ratings. Remaining: {len(ratings_batch.ratings)}")
    return ratings_batch

def filter_credits(credits_csv):
    credits_batch = files_pb2.CreditsCSV()
    credits_batch.client_id = credits_csv.client_id
    total_credits = len(credits_csv.credits)
    filtered_out = 0
    for credit in credits_csv.credits:
        if not credit.id or credit.id < 0:
            filtered_out += 1
            continue

        names = map(lambda cast: cast.name, credit.cast)
        names = list(filter(lambda name: name, names))

        credit_pb = files_pb2.CreditCSV()
        credit_pb.id = credit.id

        for name in names:
            cast_pb = credit_pb.cast.add()
            cast_pb.name = name

        credits_batch.credits.append(credit_pb)

    logging.debug(f"Filtered {filtered_out} out of {total_credits} credits. Remaining: {len(credits_batch.credits)}")
    return credits_batch 