import logging
import signal
from transformers import pipeline
import json

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
            # device = 0 if torch.cuda.is_available() else -1
            self.sentiment_analyzer = pipeline(
                'sentiment-analysis',
                model='distilbert-base-uncased-finetuned-sst-2-english',
               # device=device
            )
            logging.info("Sentiment analysis model initialized successfully.")
        except Exception as e:
            logging.error(f"Failed to initialize sentiment analysis model: {e}", exc_info=True)
            raise


    def _calculate_rate(self, revenue, budget):
        """Calculates revenue/budget ratio. Assumes budget is not zero."""
        try:
            # Ensure they are treated as numbers
            return float(revenue) / float(budget)
        except (ValueError, TypeError, ZeroDivisionError) as e:
            # Log warning if conversion/division fails unexpectedly despite prior checks
            logging.warning(f"Rate calculation error for rev={revenue}, bud={budget}: {e}. Returning 0.0")
            return 0.0

    def _process_message(self, ch, method, properties, body):
        """Callback function to process received messages"""
        processed_movies = {}
        try:
            data = json.loads(body)

            if not data or 'movies' not in data or not isinstance(data['movies'], list):
                 logging.warning("Received empty or invalid movies batch structure. Skipping.")
                 return

            logging.info(f"Processing batch with {len(data['movies'])} movies.")

            for movie in data["movies"]:
                # Ensure movie is a dictionary
                if not isinstance(movie, dict):
                    logging.warning(f"Skipping non-dictionary item in movies list: {movie}")
                    continue

                # Safely get budget and revenue
                budget_val = movie.get("budget")
                revenue_val = movie.get("revenue")

                # Check if budget and revenue exist, are numeric, and > 0
                try:
                    is_budget_valid = budget_val is not None and float(budget_val) > 0
                    is_revenue_valid = revenue_val is not None and float(revenue_val) > 0
                except (ValueError, TypeError):
                    # If conversion to float fails, treat as invalid
                    is_budget_valid = False
                    is_revenue_valid = False
                    movie_id = movie.get('id', 'UNKNOWN')
                    logging.warning(f"Invalid numeric value for budget/revenue for movie ID {movie_id}. Skipping.")


                if is_budget_valid and is_revenue_valid:
                    # --- Process only movies that pass the filter ---
                    # Calculate Sentiment (handle missing overview
                    sentiment = None
                    overview = movie.get("overview", "")
                    try:
                        # Pass overview (potentially empty) to analyzer
                        sentiment_result = self.sentiment_analyzer(overview[:512])
                        if sentiment_result and isinstance(sentiment_result, list):
                           sentiment = sentiment_result[0]['label']
                    except Exception as e:
                        movie_id = movie.get('id', 'UNKNOWN')
                        logging.error(f"Sentiment analysis failed for movie id {movie_id}: {e}", exc_info=True)
                    
                    movie['sentiment'] = sentiment

                    # Calculate Rate (direct division is safe here)
                    movie['rate_revenue_budget'] = self._calculate_rate(revenue_val, budget_val)

                    # Add to results if title exists
                    title = movie.get("title")
                    if title:
                        processed_movies[title] = movie
                    else:
                        movie_id = movie.get('id', 'UNKNOWN')
                        logging.warning(f"Filtered movie with ID {movie_id} is missing a title. Skipping adding to results.")
                else:
                    # Log movies being skipped due to the filter
                    movie_id = movie.get('id', 'UNKNOWN')
                    logging.debug(f"Skipping movie ID {movie_id} due to zero/missing/invalid budget or revenue.")

            if processed_movies:
                json_output_string = json.dumps(processed_movies)
                logging.info(f"Sending processed movies data (JSON): {json_output_string}")
                self.queue_snd.publish(json_output_string)
                logging.info(f"Successfully processed and sent {len(processed_movies)} movies matching criteria.")
            else:
                logging.info(f"No movies suitable for sending after processing and filtering.")


        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}")
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