import logging
from multiprocessing import Condition, Manager, Process, Queue
import signal
import threading
from transformers import pipeline
import json
from protocol import files_pb2
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.parsing_proto_utils import *
from protocol.protocol import Protocol
from protocol.rabbit_wrapper import RabbitMQConsumer, RabbitMQProducer
import time

logging.getLogger("pika").setLevel(logging.ERROR)

class Transformer:
    def __init__(self, communication_config, **kwargs):
        self.sentiment_analyzer = None
        self.queue_rcv = None
        self.queue_snd = None
        self.protocol = Protocol()
        self._stop_event = threading.Event()
        self._finished_signal_received = False
        self._finished_message_body = None
        self.buffered_movies = None
        self.analyzer_ready = threading.Event()
        self.rabbit_host = kwargs.get('rabbit_host', 'localhost')
        self.comm_queue = None
        self.data_thread = None

        self.communication_config = communication_config
        self.queue_communication_1 = None
        self.queue_communication_2 = None
        self.transformers_acked = 0
        for key, value in kwargs.items():
            setattr(self, key, value)

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logging.info(f"Received signal {signum}. Shutting down gracefully...")
        self.stop()
        logging.info("Shutdown complete.")

    def _create_comm_queue(self, number):
        name = self.communication_config["queue_communication_name"] + f"_{number}"
        key = self.communication_config["routing_communication_key"] + f"_{number}"
        return RabbitMQ(self.communication_config["exchange_communication"], name, key, self.communication_config["exc_communication_type"])
        

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.exchange_rcv, self.queue_rcv_name, self.routing_rcv_key, self.exc_rcv_type)
        self.queue_snd = RabbitMQ(self.exchange_snd, self.queue_snd_name, self.routing_snd_key, self.exc_snd_type)
        
        logging.info("RabbitMQ queues started")
    

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.info(f"Received message, with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)
            
        if decoded_msg.finished:
            logging.info("Received MOVIES finished signal from server on data channel.")
            self._publish_movie_finished_signal(decoded_msg)
            return
        self._process_message(decoded_msg)
        
    def start(self):
        """Start the Transformer with separate threads for data and control messages."""
        self._setup_signal_handlers()
        try:
            with Manager() as manager:
                comm_queue = Queue()
                self.comm_queue = comm_queue
                self._settle_queues()
                self.buffered_movies = manager.list()

                if not all([self.queue_rcv, self.queue_snd]):
                    logging.error("Not all required RabbitMQ connections were initialized. Aborting start.")
                    return
                
                self.data_thread = threading.Thread(target=self.start_consuming, daemon=True)
                self.data_thread.start()

                gets_the_finished = threading.Thread(target=self.manage_getting_finished, args=())

                gets_the_finished_notification = threading.Thread(target=self.manage_getting_finished_notification, args=())

                gets_the_finished.start()
                gets_the_finished_notification.start()

                self._initialize_sentiment_analyzer()

                gets_the_finished.join()
                gets_the_finished_notification.join()
                self.data_thread.join()

        except Exception as e:
            logging.error(f"Failed to start Transformer: {e}", exc_info=True)
        finally:
            self.stop()

    def start_consuming(self):
        self.queue_rcv.consume(self.callback)


    def _initialize_sentiment_analyzer(self):
        """Initializes the Hugging Face sentiment analysis pipeline."""
        try:
            logging.info("Initializing sentiment analysis model...")
            self.sentiment_analyzer = pipeline(
                'sentiment-analysis',
                model='distilbert-base-uncased-finetuned-sst-2-english',
            )
            self.analyzer_ready.set()
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

    def _send_processed_batch(self, processed_movies):
        """Creates the outgoing MoviesCSV message and publishes it."""
        if not processed_movies:
            logging.debug("No movies suitable for sending after processing and filtering.")
            return

        try:
            outgoing_movies_msg = self.protocol.create_movie_list(processed_movies)
            logging.debug(f"Sending {len(processed_movies)} processed movies")
            self.queue_snd.publish(outgoing_movies_msg)
            logging.info(f"Successfully SENT batch of {len(processed_movies)} movies to exchange '{self.queue_snd.exchange}'")
        except Exception as e:
            logging.error(f"Failed to send processed batch: {e}", exc_info=True)


    def process_pending(self):
        logging.info(f"Processing pending movies batches: {len(self.buffered_movies)}")
        if not self.analyzer_ready.is_set():
            logging.info("Waiting for analyzer")
            self.analyzer_ready.wait()
        logging.info("Starting buffer processing")
        for movie_batch in self.buffered_movies:
            batch = self.protocol.decode_movies_msg(movie_batch)
            processed_movies_list = []
            for movie in batch.movies:
                processed_movie = self._process_movie(movie)
                if processed_movie:
                    processed_movies_list.append(processed_movie)

            if not processed_movies_list:
                logging.info("No valid movies found in batch. Skipping send.")
            else:
                self._send_processed_batch(processed_movies_list)
        self.buffered_movies[:] = []

    def _publish_movie_finished_signal(self, msg):
        self.comm_queue.put(msg.SerializeToString())
        logging.info(f"Published finished signal to communication channel, here the encoded message: {msg}")

        if self.comm_queue.get() == True:
            logging.info("Received SEND finished signal from communication channel.")
            self.queue_snd.publish(msg.SerializeToString())
            logging.info(f"Published movie finished signal to {self.queue_snd.exchange}")

        logging.info("FINISHED SENDING THE FINISH MESSAGE")

        
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
            if not self.sentiment_analyzer:
                logging.info("Buffering batch")
                self.buffered_movies.append(incoming_movies_msg.SerializeToString())
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
                self._send_processed_batch(processed_movies_list)
            

        except Exception as e:
            logging.error(f"Error processing message batch: {e}", exc_info=True)

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
            logging.info("Transformer Stopped")



    ## comunication section:
    def manage_getting_finished(self):
        """
        Manage the inner communication between transformers.
        """
        try:

            data = self.comm_queue.get()

            logging.info(f"Received finished signal from transformer")

            consume_process = Process(target=self._manage_consume_pika, args=())
            consume_process.start()

            time.sleep(0.5)  # Ensure the consumer is ready before publishing

            comm_queue_1 = self._create_comm_queue(1)
            comm_queue_1.publish(data)
            consume_process.join()

            
            logging.info("Finished acking the other transformers")

        except Exception as e:
            logging.error(f"Error in managing inner communication: {e}")
            self.comm_queue.put(False)
    def _manage_consume_pika(self):
        consumer_queue = self._create_comm_queue(2)
        consumer_queue.consume(self.other_callback)
        logging.info("Finished acking the other transformers")


    def manage_getting_finished_notification(self):
        """
        Manage the communication between different transformers.
        """
        try: 
            consumer_queue = self._create_comm_queue(1)
            consumer_queue.consume(self.callback_comm)
        except Exception as e:
            logging.error(f"Error in managing inner communication: {e}")

    def callback_comm(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        logging.info(f"Received message on communication channel with routing key: {method.routing_key}")
        decoded_msg = self.protocol.decode_movies_msg(body)
        
        if decoded_msg.finished:
            logging.info("Received finished signal from other transformer!!.")
            self.process_pending()
            consumer_queue = self._create_comm_queue(2)
            consumer_queue.publish(body)
        return
    
    def other_callback(self, ch, method, properties, body):
        """
        Callback function to process incoming messages.
        """
        logging.info("RECEIVED A TRANSFORMER ACK")
        self.transformers_acked += 1
        if self.transformers_acked == self.communication_config["transformer_replicas_count"]:
            logging.info("All transformers acked")
            self.comm_queue.put(True)
        else:
            logging.info(f"Transformer {self.transformers_acked} acked")
