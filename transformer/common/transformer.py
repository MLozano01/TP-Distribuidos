import logging
import signal
import threading
from transformers import pipeline
import json
from protocol import files_pb2
from protocol.utils.parsing_proto_utils import *
from protocol.protocol import Protocol
from protocol.rabbit_wrapper import RabbitMQConsumer, RabbitMQProducer
import time

class Transformer:
    def __init__(self, **kwargs):
        self.sentiment_analyzer = None
        self.queue_rcv = None
        self.queue_snd = None
        self.control_consumer = None
        self.protocol = Protocol()
        self._stop_event = threading.Event()
        self._finished_signal_received = False
        self._finished_message_body = None
        
        # New state variables for drain-and-finish pattern
        self.finish_received = False
        self.in_flight = 0
        self.lock = threading.Lock()
        
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logging.info(f"Received signal {signum}. Shutting down gracefully...")
        self.stop()
        logging.info("Shutdown complete.")

    def _settle_connections(self):
        """Instantiate and setup RabbitMQ consumers and producer using wrapper classes."""
        try:
            # Work channel for consuming movie tasks
            self.queue_rcv = RabbitMQConsumer(
                host=self.rabbit_host,
                exchange=self.exchange_rcv,
                exchange_type=self.exc_rcv_type,
                queue_name=self.queue_rcv_name,
                routing_key=self.routing_rcv_key,
                durable=True
            )
            
            # Set prefetch count to 1 for work channel
            self.queue_rcv.channel.basic_qos(prefetch_count=1)
            
            # Producer for sending processed movies
            self.queue_snd = RabbitMQProducer(
                host=self.rabbit_host,
                exchange=self.exchange_snd,
                exchange_type=self.exc_snd_type,
                routing_key=self.routing_snd_key
            )
            
            # Control channel for finish signals
            self.control_consumer = RabbitMQConsumer(
                host=self.rabbit_host,
                exchange=self.control_exchange,
                exchange_type="fanout",
                queue_name='',  # Let RabbitMQ generate a unique queue name
                routing_key='',  # Fanout ignores routing key
                exclusive=True,
                auto_delete=True
            )
            
            logging.info("RabbitMQ consumers and producer settled using rabbit_wrapper.")
        except Exception as e:
            logging.error(f"Fatal error during RabbitMQ connection setup: {e}", exc_info=True)
            self.stop()
            raise

    def start(self):
        """Start the Transformer with separate threads for data and control messages."""
        self._setup_signal_handlers()
        try:
            self._initialize_sentiment_analyzer()
            self._settle_connections()

            if not all([self.queue_rcv, self.queue_snd, self.control_consumer]):
                logging.error("Not all required RabbitMQ connections were initialized. Aborting start.")
                return

            # Set up message consumption with manual acknowledgment
            self.queue_rcv.consume(self._process_message, auto_ack=False)
            self.control_consumer.consume(self._process_control_signal, auto_ack=True)

            data_thread = threading.Thread(target=self.queue_rcv.start_consuming, daemon=True)
            control_thread = threading.Thread(target=self.control_consumer.start_consuming, daemon=True)

            data_thread.start()
            control_thread.start()

            logging.info("Transformer started with data and control threads...")
            data_thread.join()
            control_thread.join()

        except Exception as e:
            logging.error(f"Failed to start Transformer: {e}", exc_info=True)
        finally:
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
            logging.info(f"Successfully SENT batch of {len(processed_movies)} movies to exchange '{self.queue_snd.exchange}' with key '{self.queue_snd.default_routing_key}'")
        except Exception as e:
            logging.error(f"Failed to send processed batch: {e}", exc_info=True)

    def _maybe_propagate_finish(self):
        """Check if we should propagate the finish signal."""
        try:
            # Get message count from work queue
            queue_info = self.queue_rcv.channel.queue_declare(
                queue=self.queue_rcv_name,
                passive=True
            )
            message_count = queue_info.method.message_count
            
            if self.finish_received and self.in_flight == 0 and message_count == 0:
                logging.info("All messages processed, propagating FINISHED signal...")
                try:
                    self.queue_snd.publish(self._finished_message_body)
                    logging.info(f"Successfully propagated FINISHED signal to exchange '{self.queue_snd.exchange}'")
                    
                except Exception as e:
                    logging.error(f"Failed to propagate FINISHED signal: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"Error in _maybe_propagate_finish: {e}", exc_info=True)

    def _process_control_signal(self, ch, method, properties, body):
        """Callback function to process control signals (e.g., finished)."""
        if self._stop_event.is_set():
            logging.debug("Stop event set, ignoring control signal.")
            return
        try:
            incoming_msg = self.protocol.decode_movies_msg(body)

            if incoming_msg and incoming_msg.finished and not self.finish_received:
                logging.warning("Received FINISHED signal via control channel.")
                with self.lock:
                    self.finish_received = True
                    self._finished_message_body = body
                    self._maybe_propagate_finish()
            elif self.finish_received:
                logging.info("Ignoring duplicate FINISHED signal on control channel.")
            else:
                logging.info(f"Received non-finished or undecodable message on control channel. Ignoring.")

        except Exception as e:
            logging.error(f"Error processing control signal: {e}", exc_info=True)

    def _process_message(self, ch, method, properties, body):
        """Callback function to process received data messages"""
        try:
            # Increment in-flight counter under lock
            with self.lock:
                self.in_flight += 1

            # Log reception of message
            logging.info(f"Received message batch on data channel (delivery_tag: {method.delivery_tag})")
            incoming_movies_msg = self.protocol.decode_movies_msg(body)

            if not incoming_movies_msg or not incoming_movies_msg.movies:
                logging.warning("Received empty or invalid Protobuf movies batch structure. Skipping.")
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            processed_movies_list = []
            for incoming_movie in incoming_movies_msg.movies:
                if self._validate_movie(incoming_movie):
                    processed_movie = self._enrich_movie(incoming_movie)
                    processed_movies_list.append(processed_movie)

            if processed_movies_list:
                self._send_processed_batch(processed_movies_list)

            # Acknowledge the message
            ch.basic_ack(delivery_tag=method.delivery_tag)

        except Exception as e:
            logging.error(f"Error processing message batch (tag: {method.delivery_tag}): {e}", exc_info=True)
            # Negative acknowledge the message in case of error
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        finally:
            # Decrement in-flight counter and check for finish
            with self.lock:
                self.in_flight -= 1
                self._maybe_propagate_finish()

    def stop(self):
        """Stop the filter, signal threads, and close the queues/connections."""
        if not self._stop_event.is_set():
            logging.info("Stopping Transformer...")
            self._stop_event.set()

            # First stop the control consumer if it exists and hasn't been stopped already
            if self.control_consumer and not (self.control_consumer == self.control_consumer and self._finished_signal_received):
                try:
                    logging.info("Stopping control consumer...")
                    self.control_consumer.stop()
                    logging.info("Control consumer stopped.")
                except Exception as e:
                    logging.error(f"Error stopping control consumer: {e}")

            # Then stop the data consumer
            if self.queue_rcv:
                try:
                    logging.info("Stopping data consumer...")
                    self.queue_rcv.stop()
                    logging.info("Data consumer stopped.")
                except Exception as e:
                    logging.error(f"Error stopping data consumer: {e}")

            # Finally stop the producer
            if self.queue_snd:
                try:
                    logging.info("Stopping producer...")
                    self.queue_snd.stop()
                    logging.info("Producer stopped.")
                except Exception as e:
                    logging.error(f"Error stopping producer: {e}")

            logging.info("Transformer Stopped")