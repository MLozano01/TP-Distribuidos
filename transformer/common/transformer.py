import logging
import signal
import threading
from transformers import pipeline
import json
from protocol import files_pb2
from protocol.utils.parsing_proto_utils import *
from protocol.protocol import Protocol
from protocol.rabbit_wrapper import RabbitMQConsumer, RabbitMQProducer

logging.getLogger('pika').setLevel(logging.WARNING)

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
        self.rabbit_host = kwargs.get('rabbit_host', 'localhost')
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
            self.queue_rcv = RabbitMQConsumer(
                host=self.rabbit_host,
                exchange=self.exchange_rcv,
                exchange_type=self.exc_rcv_type,
                queue_name=self.queue_rcv_name,
                routing_key=self.routing_rcv_key,
                durable=True
            )
            self.queue_snd = RabbitMQProducer(
                host=self.rabbit_host,
                exchange=self.exchange_snd,
                exchange_type=self.exc_snd_type,
                routing_key=self.routing_snd_key
            )
            self.control_consumer = RabbitMQConsumer(
                host=self.rabbit_host,
                exchange=self.control_exchange,
                exchange_type="fanout",
                queue_name=None,
                routing_key=''
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

            self.queue_rcv.consume(self._process_message)
            self.control_consumer.consume(self._process_control_signal)

            data_thread = threading.Thread(target=self.queue_rcv.start_consuming, daemon=True)
            control_thread = threading.Thread(target=self.control_consumer.start_consuming, daemon=True)

            data_thread.start()
            control_thread.start()

            logging.info("Transformer started with data and control threads...")
            # Wait for threads to complete naturally
            logging.info("Main thread waiting for data and control threads to join...")
            data_thread.join()
            logging.info("Data thread joined.")

            # Explicitly stop the control consumer now that data processing is done
            logging.info("Stopping control consumer...")
            if self.control_consumer:
                try:
                    self.control_consumer.stop()
                    logging.info("Control consumer stopped.")
                except Exception as e_stop:
                    logging.error(f"Error stopping control consumer: {e_stop}")
            else:
                logging.warning("Control consumer not initialized, cannot stop.")

            # Now wait for the control thread to terminate
            control_thread.join()
            logging.info("Control thread joined.")

            # After both threads complete, check if we received the signal and forward it
            if self._finished_signal_received and self._finished_message_body:
                logging.info("All threads joined. Forwarding stored FINISHED signal...")
                try:
                    # Add delay if needed, though maybe less critical now
                    # time.sleep(3)
                    self.queue_snd.publish(self._finished_message_body)
                    logging.info(f"Successfully SENT stored FINISHED signal to exchange '{self.queue_snd.exchange}' with key '{self.queue_snd.default_routing_key}'")
                except Exception as e_pub:
                    logging.error(f"Failed to publish stored FINISHED signal: {e_pub}", exc_info=True)
            else:
                logging.warning("Threads joined, but no FINISHED signal was processed or stored. No final signal sent.")

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

    def _process_control_signal(self, ch, method, properties, body):
        """Callback function to process control signals (e.g., finished)."""
        if self._stop_event.is_set():
            logging.debug("Stop event set, ignoring control signal.")
            return
        try:
            incoming_msg = self.protocol.decode_movies_msg(body)

            if incoming_msg and incoming_msg.finished and not self._finished_signal_received:
                logging.warning("Received FINISHED signal via control channel.")
                self._finished_signal_received = True

                # Cancel the data consumer to stop receiving new messages
                try:
                    if self.queue_rcv and self.queue_rcv.consumer_tag:
                        logging.info(f"Cancelling data consumer (tag: {self.queue_rcv.consumer_tag}) due to FINISHED signal.")
                        self.queue_rcv.channel.basic_cancel(self.queue_rcv.consumer_tag)
                        logging.info("Data consumer cancelled.")
                    else:
                        logging.warning("Data consumer (queue_rcv) or consumer_tag not available for cancellation.")
                except Exception as e_cancel:
                    logging.error(f"Error cancelling data consumer: {e_cancel}", exc_info=True)

                # Store the finished message body, DO NOT forward yet
                self._finished_message_body = body
                logging.info("FINISHED signal received and data consumer cancelled. Signal stored.")

                # Do NOT set self._stop_event here. Let threads finish naturally.
            elif self._finished_signal_received:
                 logging.info("Ignoring duplicate FINISHED signal on control channel.")
            else:
                logging.info(f"Received non-finished or undecodable message on control channel. Ignoring.")

        except Exception as e:
            logging.error(f"Error processing control signal: {e}", exc_info=True)

    def _process_message(self, ch, method, properties, body):
        """Callback function to process received data messages"""
        try:
            # Log reception of message
            logging.info(f"Received message batch on data channel (delivery_tag: {method.delivery_tag})")
            incoming_movies_msg = self.protocol.decode_movies_msg(body)

            if not incoming_movies_msg or not incoming_movies_msg.movies:
                 logging.warning("Received empty or invalid Protobuf movies batch structure. Skipping.")
                 return

            processed_movies_list = []
            for incoming_movie in incoming_movies_msg.movies:
                if self._validate_movie(incoming_movie):
                    processed_movie = self._enrich_movie(incoming_movie)
                    processed_movies_list.append(processed_movie)
                    # Log successful processing of a movie
                    logging.info(f"Processed Movie ID {processed_movie.id}: Sentiment='{processed_movie.sentiment}', Rate='{processed_movie.rate_revenue_budget:.4f}'")

            if not processed_movies_list:
                logging.info(f"No valid movies found in batch (delivery_tag: {method.delivery_tag}). Skipping send.")
            else:
                self._send_processed_batch(processed_movies_list)


        except Exception as e:
            logging.error(f"Error processing message batch (tag: {method.delivery_tag}): {e}", exc_info=True)
            # Nack the message if processing fails to potentially requeue or dead-letter it
            try:
                if ch.is_open:
                    logging.warning(f"NACKed message batch (tag: {method.delivery_tag}) due to processing error.")
                else:
                    logging.warning(f"Channel closed, cannot NACK message (tag: {method.delivery_tag}).")
            except Exception as e_nack:
                 logging.error(f"Failed to NACK message (tag: {method.delivery_tag}): {e_nack}")

    def stop(self):
        """Stop the filter, signal threads, and close the queues/connections."""
        if not self._stop_event.is_set():
            logging.info("Stopping Transformer...")
            self._stop_event.set()

            consumers = [self.queue_rcv, self.control_consumer]
            for consumer in consumers:
                # Check if this is the control consumer AND if the finished signal was received
                if consumer == self.control_consumer and self._finished_signal_received:
                     logging.debug("Skipping stop() for control_consumer in finally block as it was stopped explicitly.")
                     continue # Skip stopping it again

                if consumer:
                    try:
                        consumer.stop()
                    except Exception as e:
                        logging.error(f"Error stopping consumer {consumer}: {e}")

            if self.queue_snd:
                try:
                    self.queue_snd.stop()
                except Exception as e:
                        logging.error(f"Error stopping producer {self.queue_snd}: {e}")

            logging.info("Transformer Stopped")