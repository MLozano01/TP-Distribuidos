import logging
import signal
import threading
import time
import socket
from protocol.rabbit_protocol import RabbitMQ
from protocol.protocol import Protocol
from state.joiner_state import JoinerState
from common.requeue import RequeueException
from common.sequence_number_monitor import SequenceNumberMonitor

logging.getLogger("pika").setLevel(logging.ERROR)   
logging.getLogger("RabbitMQ").setLevel(logging.DEBUG)

class JoinerNode:
    def __init__(self, config, join_strategy, state_manager):
        self.config = config
        self.replica_id = self.config['replica_id']
        self.join_strategy = join_strategy

        backup_file = self.config.get('backup_file', f"joiner_state_{self.replica_id}.json")
        node_tag = f"{self.config.get('joiner_name', 'joiner')}_{self.replica_id}"

        backup_dir = "/backup"

        self.state = JoinerState(
            backup_file=backup_file,
            node_tag=node_tag,
            base_dir=backup_dir,
        )
        self.protocol = Protocol()
        self.other_data_type = self.config.get('other_data_type', 'RATINGS')
        
        self.output_producer = None
        self._stop_event = threading.Event()
        self._threads = []
        self._hc_sckt = None
        self._seq_monitor = SequenceNumberMonitor(state_manager)

    def start(self):
        self._setup_signal_handlers()
        try:
            self._setup_producer()
            if not self.output_producer:
                logging.error("Output producer not initialized. Aborting.")
                return

            self._threads = [
                threading.Thread(target=self._run_movies_consumer, name="MoviesConsumerThread", daemon=True),
                threading.Thread(target=self._run_other_consumer, name="OtherConsumerThread", daemon=True),
                threading.Thread(target=self._run_healthcheck_listener, name="HealthCheckThread", daemon=True)
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

            if self._hc_sckt:
                self._hc_sckt.close()
                self._hc_sckt = None

            logging.info(f"Joiner Replica {self.replica_id} shutdown complete.")

    def _setup_producer(self):
        try:
            self.output_producer = RabbitMQ(
                exchange=self.config['exchange_output'],
                q_name=self.config['queue_output_name'], 
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
                    key=self.config["routing_key"],
                    exc_type='direct',
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
                    key=self.config["routing_key"],
                    exc_type='direct',
                    prefetch_count=100
                )
                consumer.consume(self._process_other_message, stop_event=self._stop_event)
            except Exception as e:
                logging.error(f"Error in other consumer loop: {e}. Reconnecting...", exc_info=True)
            
            if not self._stop_event.is_set():
                time.sleep(5)
        logging.info("Other consumer thread finished.")

    def _run_healthcheck_listener(self):
        self._hc_sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._hc_sckt.bind(('', self.config['hc_port']))
        self._hc_sckt.listen(self.config['listen_backlog'])
        logging.info("Healthcheck listener started")

        while not self._stop_event.is_set():
            try:
                conn, addr = self._hc_sckt.accept()
                #logging.info(f"Received healthcheck from {addr}")
                conn.close()
            except socket.error as e:
                if self._stop_event.is_set():
                    logging.info("Socket closed for shutdown.")
                    break
                logging.error(f"Socket error in healthcheck listener: {e}")
            except Exception as e:
                if not self._stop_event.is_set():
                    logging.error(f"An unexpected error occurred in healthcheck listener: {e}", exc_info=True)
        
        logging.info("Healthcheck listener stopped.")

    def _process_movie_message(self, ch, method, properties, body):
        #logging.info(f"[Node] Received a movie message. Size: {len(body)} bytes.")

        if self._should_requeue():
            raise RequeueException()

        try:
            movies_msg = self.protocol.decode_movies_msg(body)
            if not movies_msg:
                logging.warning("[Node] Received invalid movie message, could not decode.")
                return

            client_id = str(movies_msg.client_id)
            """logging.info(
                f"[Node] Processing movie message for client {client_id}. "
                f"Finished: {movies_msg.finished}, Items: {len(movies_msg.movies)}"
            )"""

            seq = movies_msg.secuence_number
            if self._seq_monitor.is_duplicate(client_id, seq):
                logging.info(
                    f"[CreditsJoinStrategy] Dropping duplicate OTHER message seq={seq} client={client_id}"
                )
                return None

            if movies_msg.finished:
                self._handle_movie_eof(client_id, int(movies_msg.secuence_number))
            else:
                self.state.increment_movies_seen(client_id)
                self._handle_movie_batch(client_id, movies_msg.movies)
                self._seq_monitor.record(client_id, seq)

        except RequeueException:
            raise
        except Exception as e:
            logging.error("[Node] Error processing movie message: %s", e, exc_info=True)
            raise

    def _process_other_message(self, ch, method, properties, body):

        if self._should_requeue():
            raise RequeueException()

        try:
            client_finished_id = self.join_strategy.process_other_message(
                body, self.state, self.output_producer
            )

            if client_finished_id:
                self._check_and_handle_client_finished(str(client_finished_id))
        except RequeueException:
            raise
        except Exception as e:
            logging.error("[Node] Error processing other message: %s", e, exc_info=True)
            raise

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logging.warning(f"Received signal {signum}. Initiating shutdown...")
        self.stop()

    def _check_and_handle_client_finished(self, client_id: str):
        """If both streams have finished for *client_id*, finalise processing."""
        if not self.state.has_both_eof(client_id):
            return

        logging.info(f"[Node] Both streams finished for client {client_id}. Finalising...")
        try:
            self.join_strategy.handle_client_finished(
                client_id, self.state, self.output_producer
            )
        finally:
            # Always cleanup local buffers to avoid leaks.
            self.state.remove_client_data(client_id)

    def _should_requeue(self) -> bool:
        """Return True when node is stopping and delivery must be requeued."""
        return self._stop_event.is_set()

    def _handle_movie_eof(self, client_id: str, expected_total: int) -> None:
        """Handle EOF for movies stream of *client_id*."""
        logging.info("[Node] Movie EOF received for client %s.", client_id)
        processed_total = self.state.get_movies_count(client_id)

        if expected_total is not None and expected_total != processed_total:
            logging.warning(
                f"[CreditsJoinStrategy] Mismatch total_to_process (expected={expected_total}, processed={processed_total}) – requeuing."
            )
            raise RequeueException()

        self.state.set_stream_eof(client_id, "movies")
        self.join_strategy.handle_movie_eof(client_id, self.state)
        self._check_and_handle_client_finished(client_id)

    def _handle_movie_batch(self, client_id: str, movies) -> None:
        """Process a batch of movie protos."""
        if not movies:
            return

        for movie in movies:
            unmatched = self.state.add_movie(client_id, movie)
            if unmatched:
                self.join_strategy.process_unmatched_data(
                    unmatched, movie.id, movie.title, client_id, self.output_producer
                )
        self.state.persist_client(client_id)

        logging.debug("[Node] Added %d movies to buffer for client %s", len(movies), client_id)