from protocol import files_pb2
from protocol.utils.parsing_proto_utils import is_date
import logging

def filter_movies(movies_csv):
    movies_pb = files_pb2.MoviesCSV()
    movies_pb.client_id = movies_csv.client_id
    movies_pb.secuence_number = movies_csv.secuence_number
    discarded = movies_csv.discarded_count
    for movie in movies_csv.movies:
        if not movie.id or movie.id < 0 or not movie.release_date or not movie.overview:
            discarded += 1
            continue
        if not is_date(movie.release_date):
            discarded += 1
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

    movies_pb.discarded_count = discarded
    logging.info(
        f"Discarded {discarded} movies."
    )
    return movies_pb

def filter_ratings(ratings_csv):
    """Shard ratings by movie_id while keeping validation logic."""

    by_movie = {}
    cid = ratings_csv.client_id
    seq = ratings_csv.secuence_number

    total_ratings = len(ratings_csv.ratings)
    filtered_out = 0

    for rating in ratings_csv.ratings:
        if not rating.movieId or rating.movieId < 0 or rating.rating is None or rating.rating < 0:
            filtered_out += 1
            continue

        batch = by_movie.setdefault(rating.movieId, files_pb2.RatingsCSV())
        batch.client_id = cid
        batch.secuence_number = seq

        r_pb = batch.ratings.add()
        r_pb.userId = rating.userId
        r_pb.movieId = rating.movieId
        r_pb.rating = rating.rating

    logging.info(
        f"Filtered {filtered_out} out of {total_ratings} ratings. Remaining: {total_ratings - filtered_out}"
    )

    return list(by_movie.items())  # List[(movie_id, RatingsCSV)]

def filter_credits(credits_csv):
    """Shard credits by movie_id with original filters maintained."""

    by_movie = {}
    cid = credits_csv.client_id
    seq = credits_csv.secuence_number

    total_credits = len(credits_csv.credits)
    filtered_out = 0

    for credit in credits_csv.credits:
        if not credit.id or credit.id < 0:
            filtered_out += 1
            continue

        names = [c.name for c in credit.cast if c.name]

        # Always create a CreditCSV entry, even if the cast list is empty.
        # Downstream joiners will count the row but ignore it when producing
        # ActorParticipations if no names are present.
        batch = by_movie.setdefault(credit.id, files_pb2.CreditsCSV())
        batch.client_id = cid
        batch.secuence_number = seq

        c_pb = batch.credits.add()
        c_pb.id = credit.id
        for n in names:
            cp = c_pb.cast.add()
            cp.name = n

    logging.info(
        f"Filtered {filtered_out} out of {total_credits} credits. Remaining: {total_credits - filtered_out}"
    )

    return list(by_movie.items())  # List[(movie_id, CreditsCSV)] 