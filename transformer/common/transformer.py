import logging
import signal
from transformers import pipeline
import json
from protocol import files_pb2
from protocol.utils.parsing_proto_utils import *

from protocol.rabbit_protocol import RabbitMQ

class Transformer:
    def __init__(self, **kwargs):
        self.sentiment_analyzer = None
        self.queue_rcv = None
        self.queue_snd = None
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logging.info(f"Received signal {signum}. Shutting down gracefully...")
        self.stop()
        logging.info("Shutdown complete.")

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type)
        self.queue_snd = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
            
    def start(self):
        """Start the Transformer to consume messages from the queue."""
        try:
            self._initialize_sentiment_analyzer()
            self._settle_queues()
            self.queue_rcv.consume(self._process_message)
        except Exception as e:
            logging.error(f"Failed to start Transformer: {e}", exc_info=True)
            self.stop()

    def _initialize_sentiment_analyzer(self):
        """Initializes the Hugging Face sentiment analysis pipeline."""
        try:
            logging.info("Initializing sentiment analysis model...")
            self.sentiment_analyzer = pipeline(
                'sentiment-analysis',
                model='distilbert-base-uncased-finetuned-sst-2-english',
            )
            logging.info("Sentiment analysis model initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize sentiment analysis model: {e}", exc_info=True)
            raise


    def _calculate_rate(self, revenue, budget):
        """Calculates revenue/budget ratio. Assumes budget is not zero."""
        try:
            return float(revenue) / float(budget)
        except (ValueError, TypeError, ZeroDivisionError) as e:
            logging.warning(f"Rate calculation error for rev={revenue}, bud={budget}: {e}. Returning 0.0")
            return 0.0

    def _process_message(self, ch, method, properties, body):
        """Callback function to process received messages"""
        processed_movies_list = []
        try:
            #incoming_movies_msg = files_pb2.MoviesCSV()
            #incoming_movies_msg.ParseFromString(body)
            data = json.loads(body) 

            #if not incoming_movies_msg or not incoming_movies_msg.movies:
             #    logging.warning("Received empty or invalid Protobuf movies batch structure. Skipping.")

            # [ Change: Validate JSON dictionary structure ]
            # if not data or not isinstance(data, dict):
            #     logging.warning("Received empty or invalid JSON movie data (expected dict). Skipping.")
            #     return
            # [ Change: Validate Protobuf-style JSON structure {"movies": [...]} ]
            if not data or not isinstance(data, dict) or "movies" not in data or not isinstance(data["movies"], list):
                 logging.warning("Received empty or invalid JSON movie data (expected '{\"movies\": [...]}'). Skipping.")
                 return

            # logging.info(f"Processing batch with {len(data)} movies (from JSON keys).")
            logging.info(f"Processing batch with {len(data['movies'])} movies (from JSON list).")

            # [ Change: Iterate over movie dictionaries within the 'movies' list ]
            # for movie_dict in data.values():
            for movie_dict in data["movies"]:
                # Ensure movie_dict is a dictionary
                if not isinstance(movie_dict, dict):
                    logging.warning(f"Skipping non-dictionary item found in JSON 'movies' list: {movie_dict}")
                    continue

                # Safely get budget and revenue from dictionary
                budget_val = movie_dict.get("budget")
                revenue_val = movie_dict.get("revenue")

                # Check if budget and revenue exist, are numeric, and > 0
                try:
                    #is_budget_valid = budget_val > 0
                    #is_revenue_valid = revenue_val > 0
                    is_budget_valid = budget_val is not None and float(budget_val) > 0
                    is_revenue_valid = revenue_val is not None and float(revenue_val) > 0
                except (ValueError, TypeError):
                    is_budget_valid = False
                    is_revenue_valid = False
                    #movie_id = movie_pb.id if movie_pb.id else 'UNKNOWN_PROTO_ID'
                    #logging.warning(f"Invalid numeric value (unexpected) for budget/revenue for movie ID {movie_id}. Skipping.")
                    movie_id = movie_dict.get('id', 'UNKNOWN_JSON_ID')
                    logging.warning(f"Invalid numeric value for budget/revenue for movie ID {movie_id}. Skipping.")


                if is_budget_valid and is_revenue_valid:
                    # --- Process only movies that pass the filter ---
                    
                    # [ Create Protobuf object to populate ]
                    movie_pb = files_pb2.MovieCSV()

                    # [ Populate basic fields from dict using helpers ]
                    # Note: This assumes the JSON from the server contains these fields. Add more as needed.
                    movie_pb.id = to_int(movie_dict.get('id'))
                    movie_pb.adult = to_bool(movie_dict.get('adult', 'False')) # Handle potential bool string
                    movie_pb.budget = to_int(budget_val) # Already validated/converted
                    movie_pb.homepage = to_string(movie_dict.get('homepage'))
                    movie_pb.imdb_id = to_string(movie_dict.get('imdb_id'))
                    movie_pb.original_language = to_string(movie_dict.get('original_language'))
                    movie_pb.original_title = to_string(movie_dict.get('original_title'))
                    movie_pb.overview = to_string(movie_dict.get('overview'))
                    movie_pb.popularity = to_float(movie_dict.get('popularity'))
                    movie_pb.poster_path = to_string(movie_dict.get('poster_path'))
                    movie_pb.release_date = to_string(movie_dict.get('release_date'))
                    movie_pb.revenue = to_int(revenue_val) # Already validated/converted
                    movie_pb.runtime = to_float(movie_dict.get('runtime'))
                    movie_pb.status = to_string(movie_dict.get('status'))
                    movie_pb.tagline = to_string(movie_dict.get('tagline'))
                    movie_pb.title = to_string(movie_dict.get('title'))
                    movie_pb.video = to_bool(movie_dict.get('video', 'False')) # Handle potential bool string
                    movie_pb.vote_average = to_float(movie_dict.get('vote_average'))
                    movie_pb.vote_count = to_int(movie_dict.get('vote_count'))
                    
                    # TODO: Populate nested structures like genres, companies, countries, languages, collection
                    # This requires parsing the string representations often found in CSVs/JSON if they exist in movie_dict
                    # Example (if genres is a list of dicts):
                    # genres_data = movie_dict.get('genres', [])
                    # if isinstance(genres_data, list):
                    #    for genre in genres_data:
                    #       if isinstance(genre, dict):
                    #           genre_pb = movie_pb.genres.add()
                    #           genre_pb.id = to_int(genre.get('id'))
                    #           genre_pb.name = to_string(genre.get('name'))


                    # Calculate Sentiment
                    sentiment = None
                    overview = movie_pb.overview
                    try:
                        sentiment_result = self.sentiment_analyzer(overview[:512])
                        if sentiment_result and isinstance(sentiment_result, list):
                           sentiment = sentiment_result[0]['label']
                    except Exception as e:
                        movie_id = movie_pb.id if movie_pb.id else 'UNKNOWN_PROTO_ID'
                        logging.error(f"Sentiment analysis failed for movie id {movie_id}: {e}", exc_info=True)
                    
                    if sentiment is not None:
                        movie_pb.sentiment = sentiment

                    movie_pb.rate_revenue_budget = self._calculate_rate(revenue_val, budget_val)

                    processed_movies_list.append(movie_pb)
                else:
                    #movie_id = movie_pb.id if movie_pb.id else 'UNKNOWN_PROTO_ID'
                    movie_id = movie_dict.get('id', 'UNKNOWN_JSON_ID')
                    logging.debug(f"Skipping movie ID {movie_id} due to zero/missing/invalid budget or revenue.")

            if processed_movies_list:
                outgoing_movies_msg = files_pb2.MoviesCSV()
                outgoing_movies_msg.movies.extend(processed_movies_list)

                serialized_output = outgoing_movies_msg.SerializeToString()

                logging.info(f"Sending {len(processed_movies_list)} processed movies (Protobuf binary)")
                self.queue_snd.publish(serialized_output)
                logging.info(f"Successfully processed and sent {len(processed_movies_list)} movies matching criteria.")
            else:
                logging.info(f"No movies suitable for sending after processing and filtering.")

            # ACK should happen after successful processing or skipping
            # ch.basic_ack(delivery_tag=method.delivery_tag) # Removed ACK here based on user's previous edit

        except json.JSONDecodeError as e: # Keep JSON decode error for input
             logging.error(f"Failed to decode incoming JSON: {e}")
             return
        except Exception as e:
            logging.error(f"Error processing message: {e}", exc_info=True)
            return

    def stop(self):
        """End the filter and close the queue."""
        if self.queue_rcv:
            self.queue_rcv.close_channel()
        if self.queue_snd:
            self.queue_snd.close_channel()
        logging.info("Filter Stopped")