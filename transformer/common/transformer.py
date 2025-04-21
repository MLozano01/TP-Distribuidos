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
        """Calculates revenue/budget ratio, handling division by zero."""
        if budget and budget > 0:
            try:
                return float(revenue) / float(budget)
            except (ValueError, TypeError):
                logging.warning(f"Invalid type for revenue or budget: rev={revenue}, bud={budget}. Returning 0.0")
                return 0.0
        return 0.0

    def _process_message(self, ch, method, properties, body):
        """Callback function to process received messages."""
        try:
            data = json.loads(body)
            result = {}
            if not data:
                 logging.info("Received empty or invalid movies batch. Skipping.")
                 return

            logging.info(f"Processing batch with {len(data['movies'])} movies.")

            for movie in data["movies"]:
                sentiment = None
                if movie["overview"]:
                    try:
                        result = self.sentiment_analyzer(movie["overview"][:512])
                        if result and isinstance(result, list):
                           sentiment = result[0]['label']
                    except Exception as e:
                        logging.error(f"Sentiment analysis failed for movie id {movie.id}: {e}", exc_info=True)
                movie['sentiment'] = sentiment

                revenue = movie["revenue"] if movie["revenue"] else 0
                budget = movie["budget"] if movie["budget"] else 0
                movie['rate_revenue_budget'] = self._calculate_rate(revenue, budget)

                result[movie["title"]] = movie
            
            if result:
                self.queue_snd.publish(json.dumps(result))
            else:
                logging.info(f"No movie matched the transformer criteria.")

        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}")
            return
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return


    def stop(self):
        """End the filter and close the queue."""
        if self.queue_rcv:
            self.queue_rcv.close_channel()
        if self.queue_snd:
            self.queue_snd.close_channel()
        logging.info("Filter Stopped")