import logging
import signal
import threading
import time
from protocol.rabbit_protocol import RabbitMQ
from protocol.protocol import Protocol
from state.joiner_state import JoinerState

class JoinerNode:
    def __init__(self, config, join_strategy):
        self.config = config
        self.replica_id = self.config['replica_id']
        self.join_strategy = join_strategy
        self.state = JoinerState()
        self.protocol = Protocol()
        self.other_data_type = self.config.get('other_data_type', 'RATINGS')
        
        self.output_producer = None
        self._stop_event = threading.Event()
        self._threads = []

    def start(self):
        self._setup_signal_handlers()
        try:
            self._setup_producer()
            if not self.output_producer:
                logging.error("Output producer not initialized. Aborting.")
                return

            self._threads = [
                threading.Thread(target=self._run_movies_consumer, name="MoviesConsumerThread", daemon=True),
                threading.Thread(target=self._run_other_consumer, name="OtherConsumerThread", daemon=True)
            ]
            for t in self._threads:
                t.start()
                logging.info(f"Started thread: {t.name}")

            # Keep main thread alive and wait for stop event
            self._stop_event.wait()

        except Exception as e:
            logging.error(f"Failed to start JoinerNode: {e}", exc_info=True)
        finally:
            self.stop()

    def stop(self):
        if not self._stop_event.is_set():
            self._stop_event.set()
            logging.info(f"Stopping Joiner Replica {self.replica_id}...")
            
            # Wait for consumer threads to finish
            for t in self._threads:
                t.join(timeout=5.0)

            logging.info(f"Joiner Replica {self.replica_id} shutdown complete.")

    def _setup_producer(self):
        try:
            # The producer in rabbit_protocol.py does not need a queue name
            self.output_producer = RabbitMQ(
                exchange=self.config['exchange_output'],
                q_name=None, 
                key=self.config['routing_key_output'],
                exc_type='direct'
            )
            logging.info("Output producer set up successfully.")
        except Exception as e:
            logging.error(f"Error setting up RabbitMQ Producer: {e}", exc_info=True)
            self.stop()
            raise

    def _run_movies_consumer(self):
        """Runs the movie consumer loop using rabbit_protocol."""
        while not self._stop_event.is_set():
            try:
                consumer = RabbitMQ(
                    exchange=self.config['exchange_movies'],
                    q_name=self.config['queue_movies_name'],
                    key="1",
                    exc_type='x-consistent-hash',
                    prefetch_count=100
                )
                consumer.consume(self._process_movie_message, stop_event=self._stop_event)
            except Exception as e:
                logging.error(f"Error in movies consumer loop: {e}. Reconnecting...", exc_info=True)
            
            if not self._stop_event.is_set():
                time.sleep(5)
        logging.info("Movies consumer thread finished.")

    def _run_other_consumer(self):
        """Runs the other data consumer loop using rabbit_protocol."""
        while not self._stop_event.is_set():
            try:
                consumer = RabbitMQ(
                    exchange=self.config['exchange_other'],
                    q_name=self.config['queue_other_name'],
                    key="1",
                    exc_type='x-consistent-hash',
                    prefetch_count=100
                )
                consumer.consume(self._process_other_message, stop_event=self._stop_event)
            except Exception as e:
                logging.error(f"Error in other consumer loop: {e}. Reconnecting...", exc_info=True)
            
            if not self._stop_event.is_set():
                time.sleep(5)
        logging.info("Other consumer thread finished.")

    def _process_movie_message(self, ch, method, properties, body):
        logging.debug(f"[Node] Received a movie message. Size: {len(body)} bytes.")
        if self._stop_event.is_set():
            logging.info("[Node] Stop event set, ignoring movie message.")
            return
        try:
            movies_msg = self.protocol.decode_movies_msg(body)
            if not movies_msg:
                logging.warning("[Node] Received invalid movie message, could not decode.")
                return

            client_id = movies_msg.client_id
            logging.info(f"[Node] Processing movie message for client {client_id}. Finished: {movies_msg.finished}, Items: {len(movies_msg.movies)}")

            if movies_msg.finished:
                logging.info(f"[Node] Movie EOF received for client {client_id}. Checking if other EOF is present.")
                should_trigger = self.state.set_movies_eof(client_id)
                logging.info(f"[Node] Result of EOF check for client {client_id}: {should_trigger}")
                if should_trigger:
                    logging.info(f"[Node] Both EOFs present for client {client_id}. Triggering final processing.")
                    self.join_strategy.trigger_final_processing(client_id, self.state, self.output_producer)
                return

            for movie in movies_msg.movies:
                self.state.add_movie(client_id, movie)
            logging.debug(f"[Node] Added {len(movies_msg.movies)} movies to buffer for client {client_id}")

        except Exception as e:
            logging.error(f"[Node] Error processing movie message: {e}", exc_info=True)

    def _process_other_message(self, ch, method, properties, body):
        logging.debug(f"[Node] Received an other message. Size: {len(body)} bytes.")
        if self._stop_event.is_set():
            logging.info("[Node] Stop event set, ignoring other message.")
            return

        try:
            self.join_strategy.process_other_message(body, self.state, self.output_producer)
        except Exception as e:
            logging.error(f"[Node] Error processing other message: {e}", exc_info=True)

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logging.warning(f"Received signal {signum}. Initiating shutdown...")
        self.stop() 