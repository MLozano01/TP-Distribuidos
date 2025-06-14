import logging
from multiprocessing import Condition, Manager, Process, Queue, Value
import signal
import threading
from transformers import pipeline
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.parsing_proto_utils import *
from protocol.protocol import Protocol
from queue import Empty


logging.getLogger('pika').setLevel(logging.WARNING)

logging.getLogger("pika").setLevel(logging.ERROR)

START = False
DONE = True

class Transformer:
    def __init__(self, finish_notify_ntc, finish_notify_ctn, communicator_instance, **kwargs):
        self.sentiment_analyzer = None
        self.queue_rcv = None
        self.queue_snd = None
        self.protocol = Protocol()
        self._stop_event = threading.Event()
        self._finished_signal_received = False
        self._finished_message_body = None

        self.finished_filter_arg_step_publisher = None # for notifying joiners
        self.finish_notify_ntc = finish_notify_ntc
        self.finish_notify_ctn = finish_notify_ctn

        self.comm_instance = communicator_instance

        self.data_thread = None

        self.actual_client_id = Value('i', 0)
        self.actual_status = Value('b', True)
        self.finish_signal_checker = None

        self.is_alive = True


        for key, value in kwargs.items():
            setattr(self, key, value)

    def update_actual_client_id_status(self, client_id, status): 
        self.actual_client_id.value = client_id
        self.actual_status.value = status

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
        
        logging.info("RabbitMQ queues started")
    

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.info(f"Received message, with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)
        self.update_actual_client_id_status(decoded_msg.client_id, START)
            
        if decoded_msg.finished:
            logging.info("Received MOVIES finished signal from server on data channel.")
            self.update_actual_client_id_status(decoded_msg.client_id, DONE)
            self._publish_movie_finished_signal(decoded_msg)
            return
        
        self._process_message(decoded_msg)
        
        
    def start(self):
        """Start the Transformer with separate threads for data and control messages."""
        self._setup_signal_handlers()
        try:
            self._initialize_sentiment_analyzer()

            self._settle_queues()

            if not all([self.queue_rcv, self.queue_snd]):
                logging.error("Not all required RabbitMQ connections were initialized. Aborting start.")
                return
        
            self.finish_signal_checker = Process(target=self.check_finished, args=())
            self.finish_signal_checker.start()

            self.queue_rcv.consume(self.callback)

        except Exception as e:
            logging.error(f"Failed to start Transformer: {e}", exc_info=True)
        


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
            if not (is_budget_valid and is_revenue_valid and movie.overview):
                movie_id = movie.id if movie.id else 'UNKNOWN_PROTO_ID'
                logging.debug(f"Skipping movie ID {movie_id} due to zero/missing/invalid budget, revenue or overview.")
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

    def _send_processed_batch(self, processed_movies, client_id):
        """Creates the outgoing MoviesCSV message and publishes it."""
        if not processed_movies:
            logging.debug("No movies suitable for sending after processing and filtering.")
            return

        try:
            outgoing_movies_msg = self.protocol.create_movie_list(processed_movies, client_id)
            logging.debug(f"Sending {len(processed_movies)} processed movies")
            self.queue_snd.publish(outgoing_movies_msg)
            logging.info(f"Successfully SENT batch of {len(processed_movies)} movies to exchange '{self.queue_snd.exchange}'")
        except Exception as e:
            logging.error(f"Failed to send processed batch: {e}", exc_info=True)


    def _publish_movie_finished_signal(self, msg):
        logging.info(f"Published finished signal to communication channel, here the encoded message: {msg}")
        self.comm_instance.start_token_ring(msg.client_id)
        
        self.comm_instance.wait_eof_confirmation()
        logging.info("Received SEND finished signal from communication channel.")
        self.queue_snd.publish(msg.SerializeToString())
        logging.info(f"Published movie finished signal to {self.queue_snd.exchange}")


        
    def _process_movie(self, incoming_movie):
        if self._validate_movie(incoming_movie):
            processed_movie = self._enrich_movie(incoming_movie)
            # Log successful processing of a movie
            logging.info(f"Processed Movie ID {processed_movie.id}: Sentiment='{processed_movie.sentiment}', Rate='{processed_movie.rate_revenue_budget:.4f}'")
            return processed_movie
        return None

    def _process_message(self, incoming_movies_msg):
        """Function to process received data messages"""
        try:
            if not incoming_movies_msg or not incoming_movies_msg.movies:
                logging.warning("Received empty or invalid Protobuf movies batch structure. Skipping.")
                return
            logging.info("processing batch")
            processed_movies_list = []
            for incoming_movie in incoming_movies_msg.movies:
                processed_movie = self._process_movie(incoming_movie)
                if processed_movie:
                    processed_movies_list.append(processed_movie)

            if not processed_movies_list:
                logging.info(f"No valid movies found in batch. Skipping send.")
            else:
                self._send_processed_batch(processed_movies_list, incoming_movies_msg.client_id)

            self.update_actual_client_id_status(incoming_movies_msg.client_id, DONE)
        except Exception as e:
            logging.error(f"Error processing message batch: {e}", exc_info=True)

    def check_finished(self):
        while self.is_alive:
            try:
                msg = self.finish_notify_ctn.get()
                logging.info(f"Received finished signal from control channel: {msg}")

                client_id, status = self.get_last()
                client_finished = msg[0]
                if client_finished == client_id:
                    self.finish_notify_ntc.put([client_finished, status])
                    logging.info(f"Received finished signal from control channel for client {client_finished}, with status {status}.")
                else:
                    self.finish_notify_ntc.put([client_finished, True])
                    logging.info(f"Received finished signal from control channel for client {client_finished}, but working on {client_id}.")

            except Empty:
                logging.info("No finished signal received yet.")
                pass
            except Exception as e:
                logging.error(f"Error in finished signal checker: {e}")
                break

    def get_last(self):
        client_id = self.actual_client_id.value
        status = self.actual_status.value

        logging.info(f"Last client ID: {client_id}, status: {status}")

        return client_id, status

    def stop(self):
        """Stop the filter, signal threads, and close the queues/connections."""

        if not self._stop_event.is_set():
            logging.info("Stopping Transformer...")
            self._stop_event.set()

            if self.queue_rcv:
                try:
                    self.queue_rcv.close_channel()
                except Exception as e:
                    logging.error(f"Error stopping consumer {self.queue_rcv}: {e}")

            if self.queue_snd:
                try:
                    self.queue_snd.close_channel()
                except Exception as e:
                        logging.error(f"Error stopping producer {self.queue_snd}: {e}")

            if self.data_thread and self.data_thread.is_alive():
                self.data_thread.terminate()

            if self.finish_signal_checker:
                self.finish_signal_checker.terminate()
                self.finish_signal_checker.join()
                logging.info("Finished signal checker process terminated.")

        self.is_alive = False

        logging.info("Transformer Stopped")
