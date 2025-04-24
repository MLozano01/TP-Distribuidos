import logging
import signal
from transformers import pipeline
import json
from protocol import files_pb2
from protocol.utils.parsing_proto_utils import *
from protocol.protocol import Protocol
from protocol.rabbit_protocol import RabbitMQ

class Transformer:
    def __init__(self, **kwargs):
        self.sentiment_analyzer = None
        self.queue_rcv = None
        self.queue_snd = None
        self.protocol = Protocol()
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

    def _validate_movie(self, movie):
        """Checks if the movie's budget and revenue are valid for processing."""
        try:
            budget_val = movie.budget
            revenue_val = movie.revenue
            is_budget_valid = budget_val is not None and float(budget_val) > 0
            is_revenue_valid = revenue_val is not None and float(revenue_val) > 0
            if not (is_budget_valid and is_revenue_valid):
                movie_id = movie.id if movie.id else 'UNKNOWN_PROTO_ID'
                logging.debug(f"Skipping movie ID {movie_id} due to zero/missing/invalid budget or revenue.")
                return False
            return True
        except (ValueError, TypeError):
            movie_id = movie.id if movie.id else 'UNKNOWN_PROTO_ID'
            logging.warning(f"Invalid numeric value (unexpected) for budget/revenue for movie ID {movie_id}. Skipping.")
            return False

    def _calculate_sentiment(self, overview):
        """Calculates the sentiment of the movie overview."""
        if not overview:
            logging.debug("Overview is empty, skipping sentiment analysis.")
            return None
        try:
            # Limit length to avoid issues with very long overviews
            sentiment_result = self.sentiment_analyzer(overview[:512])
            if sentiment_result and isinstance(sentiment_result, list):
                return sentiment_result[0]['label']
            return None
        except Exception as e:
            logging.error(f"Sentiment analysis failed: {e}", exc_info=True)
            return None

    def _enrich_movie(self, movie):
        """Adds sentiment and rate to the movie, logs it, and returns the movie."""
        sentiment = self._calculate_sentiment(movie.overview)
        if sentiment is not None:
            movie.sentiment = sentiment

        movie.rate_revenue_budget = self._calculate_rate(movie.revenue, movie.budget)
        return movie

    def _send_processed_batch(self, processed_movies):
        """Creates the outgoing MoviesCSV message and publishes it."""
        if not processed_movies:
            logging.debug("No movies suitable for sending after processing and filtering.")
            return

        try:
            outgoing_movies_msg = self.protocol.create_movie_list(processed_movies)
            logging.debug(f"Sending {len(processed_movies)} processed movies")
            self.queue_snd.publish(outgoing_movies_msg)
            logging.debug(f"Successfully processed and sent {len(processed_movies)} movies matching criteria.")
        except Exception as e:
            logging.error(f"Failed to send processed batch: {e}", exc_info=True)


    def _process_message(self, ch, method, properties, body):
        """Callback function to process received messages"""
        try:
            incoming_movies_msg = self.protocol.decode_movies_msg(body)

            if (incoming_movies_msg.finished):
                self.queue_snd.publish(incoming_movies_msg.SerializeToString())
                return

            if not incoming_movies_msg or not incoming_movies_msg.movies:
                 logging.warning("Received empty or invalid Protobuf movies batch structure. Skipping.")
                 return

            # logging.info(f"Processing batch with {len(incoming_movies_msg.movies)} movies (from Protobuf list).")

            processed_movies_list = []
            for incoming_movie in incoming_movies_msg.movies:
                if self._validate_movie(incoming_movie):
                    processed_movie = self._enrich_movie(incoming_movie)
                    processed_movies_list.append(processed_movie)
                    logging.info(f"Movie ID {processed_movie.id}: Sentiment='{processed_movie.sentiment}', Rate='{processed_movie.rate_revenue_budget:.4f}'")

            self._send_processed_batch(processed_movies_list)

        except Exception as e:
            logging.error(f"Error processing message batch: {e}", exc_info=True)

    def stop(self):
        """End the filter and close the queue."""
        if self.queue_rcv:
            self.queue_rcv.close_channel()
        if self.queue_snd:
            self.queue_snd.close_channel()
        logging.info("Filter Stopped")