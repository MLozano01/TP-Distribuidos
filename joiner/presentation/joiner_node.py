from __future__ import annotations

"""Presentation-layer entry point for the Joiner micro-service.

This module contains the orchestrator that wires together the AMQP protocol
handlers, the join strategies (domain layer) and the concrete persistence
repositories (data layer).  The implementation is **functionally identical**
to the original *joiner.joiner_node* but now relies on dependency‐injection
and the newly introduced repository abstraction so that I/O concerns are not
mixed with business rules.
"""

import logging
import signal
import threading
import time
import socket
from typing import Optional

from protocol.rabbit_protocol import RabbitMQ
from protocol.protocol import Protocol
from common.requeue import RequeueException

from ..data.joiner_state_repository import JoinerStateRepository
from ..state.joiner_state import JoinerState
from ..domain.state_repository import StateRepository
from joiner.logic.ring_eof_handler import EofRingHandler

logging.getLogger("RabbitMQ").setLevel(logging.ERROR)

class JoinerNode:  # pylint: disable=too-many-instance-attributes
    """High-level coordinator for the Joiner replica.

    The class is *stateless* regarding persistence; it simply orchestrates
    messages flowing between RabbitMQ and the domain logic whilst delegating
    stateful operations to the injected ``StateRepository``.
    """

    def __init__(
        self,
        config: dict,
        join_strategy,
        state_repository: Optional[StateRepository] = None,
    ) -> None:
        self.config = config
        self.replica_id = self.config["replica_id"]
        self.join_strategy = join_strategy

        # ------------------------------------------------------------------
        # Repository wiring (DI)
        # ------------------------------------------------------------------
        if state_repository is None:
            backup_file = self.config.get(
                "backup_file", f"joiner_state_{self.replica_id}.json"
            )
            node_tag = f"{self.config.get('joiner_name', 'joiner')}_{self.replica_id}"
            backup_dir = "/backup"

            state_repository = JoinerStateRepository(
                JoinerState(
                    backup_file=backup_file,
                    node_tag=node_tag,
                    base_dir=backup_dir,
                ),
                node_tag=node_tag
            )

        self.state: StateRepository = state_repository

        # ------------------------------------------------------------------
        # Protocol / transport helpers
        # ------------------------------------------------------------------
        self.protocol = Protocol()
        self.other_data_type = self.config.get("other_data_type", "RATINGS")

        self.output_producer: Optional[RabbitMQ] = None
        self.publisher = None
        self._stop_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._hc_sckt: Optional[socket.socket] = None

        # Publishers for ring EOF messages (per stream)
        self._movies_publisher: Optional[RabbitMQ] = None
        self._other_publisher: Optional[RabbitMQ] = None

        # ------------------------------------------------------------------
        # EOF-ring coordinator
        # ------------------------------------------------------------------
        self._ring_handler = EofRingHandler(
            self.replica_id,
            self.config.get("replicas_count", 1),
            self.config["queue_movies_base"],
            self.config["queue_other_base"],
        )

        # ------------------------------------------------------------------
        # Use-case layer
        # ------------------------------------------------------------------
        self._interactor = None

    # ------------------------------------------------------------------
    # Lifecycle helpers
    # ------------------------------------------------------------------
    def start(self) -> None:
        self._setup_signal_handlers()
        try:
            self._setup_producer()
            if not self.output_producer:
                logging.error("Output producer not initialized. Aborting.")
                return

            self._threads = [
                threading.Thread(
                    target=self._run_movies_consumer,
                    name="MoviesConsumerThread",
                    daemon=True,
                ),
                threading.Thread(
                    target=self._run_other_consumer,
                    name="OtherConsumerThread",
                    daemon=True,
                ),
                threading.Thread(
                    target=self._run_healthcheck_listener,
                    name="HealthCheckThread",
                    daemon=True,
                ),
            ]
            for t in self._threads:
                t.start()
                logging.info("Started thread: %s", t.name)

            # Keep main thread alive and wait for stop event
            self._stop_event.wait()

        except Exception as exc:  # pylint: disable=broad-except
            logging.error("Failed to start JoinerNode: %s", exc, exc_info=True)
        finally:
            self.stop()

    def stop(self) -> None:
        if self._stop_event.is_set():
            return

        self._stop_event.set()
        logging.info("Stopping Joiner Replica %s...", self.replica_id)

        # Wait for consumer threads to finish
        for t in self._threads:
            t.join(timeout=5.0)

        if self._hc_sckt:
            self._hc_sckt.close()
            self._hc_sckt = None

        logging.info("Joiner Replica %s shutdown complete.", self.replica_id)

    # ------------------------------------------------------------------
    # RabbitMQ wiring
    # ------------------------------------------------------------------
    def _setup_producer(self) -> None:
        try:
            rabbit_prod = RabbitMQ(
                exchange=self.config["exchange_output"],
                q_name=None,
                key=self.config["routing_key_output"],
                exc_type="direct",
            )
            from ..data.rabbit_publisher import RabbitPublisher
            self.output_producer = rabbit_prod
            self.publisher = RabbitPublisher(rabbit_prod)
            logging.info("Output producer set up successfully.")

            # Publishers for ring handler (movies / other streams)
            # TODO: Check set up, later overrided for now, but cant it be done at this moment actually?
            if self._movies_publisher is None:
                self._movies_publisher = RabbitMQ(
                    exchange="",  # default exchange
                    q_name=None,
                    key="",       # overridden per publish
                    exc_type="direct",
                )
            if self._other_publisher is None:
                self._other_publisher = RabbitMQ(
                    exchange="",  # default exchange
                    q_name=None,
                    key="",
                    exc_type="direct",
                )

            if self._interactor is None:
                from ..domain.joiner_interactor import JoinerInteractor
                self._interactor = JoinerInteractor(
                    self.state,
                    self.join_strategy,
                    self.publisher,
                    ring_handler=self._ring_handler,
                    movies_publisher=self._movies_publisher,
                    other_publisher=self._other_publisher,
                    replica_id=self.replica_id,
                    protocol=self.protocol,
                )

        except Exception as exc:  # pylint: disable=broad-except
            logging.error("Error setting up RabbitMQ Producer: %s", exc, exc_info=True)
            self.stop()
            raise

    # Consumer helpers --------------------------------------------------
    def _run_movies_consumer(self) -> None:
        """Runs the movie consumer loop using *rabbit_protocol*."""
        while not self._stop_event.is_set():
            try:
                consumer = RabbitMQ(
                    exchange=self.config["exchange_movies"],
                    q_name=self.config["queue_movies_name"],
                    key="1",
                    exc_type="x-consistent-hash",
                    prefetch_count=100,
                )
                consumer.consume(self._process_movie_message, stop_event=self._stop_event)
            except Exception as exc:  # pylint: disable=broad-except
                logging.error(
                    "Error in movies consumer loop: %s. Reconnecting...", exc, exc_info=True
                )

            if not self._stop_event.is_set():
                time.sleep(5)
        logging.info("Movies consumer thread finished.")

    def _run_other_consumer(self) -> None:
        """Runs the other data consumer loop using *rabbit_protocol*."""
        while not self._stop_event.is_set():
            try:
                consumer = RabbitMQ(
                    exchange=self.config["exchange_other"],
                    q_name=self.config["queue_other_name"],
                    key="1",
                    exc_type="x-consistent-hash",
                    prefetch_count=100,
                )
                consumer.consume(self._process_other_message, stop_event=self._stop_event)
            except Exception as exc:
                logging.error(
                    "Error in other consumer loop: %s. Reconnecting...", exc, exc_info=True
                )

            if not self._stop_event.is_set():
                time.sleep(5)
        logging.info("Other consumer thread finished.")

    def _run_healthcheck_listener(self) -> None:
        self._hc_sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._hc_sckt.bind(("", self.config["hc_port"]))
        self._hc_sckt.listen(self.config["listen_backlog"])
        logging.info("Healthcheck listener started")

        while not self._stop_event.is_set():
            try:
                conn, _addr = self._hc_sckt.accept()
                conn.close()
            except socket.error as exc:
                if self._stop_event.is_set():
                    logging.info("Socket closed for shutdown.")
                    break
                logging.error("Socket error in healthcheck listener: %s", exc)
            except Exception as exc:  # pylint: disable=broad-except
                if not self._stop_event.is_set():
                    logging.error(
                        "An unexpected error occurred in healthcheck listener: %s", exc, exc_info=True
                    )

        logging.info("Healthcheck listener stopped.")

    # ------------------------------------------------------------------
    # Message processors
    # ------------------------------------------------------------------
    def _process_movie_message(self, _ch, _method, _properties, body):  # noqa: D401
        if self._should_requeue():
            raise RequeueException()

        # --- Ring EOF fast-path -----------------------------------------
        try:
            if self.protocol.is_stream_eof(body):
                self._ring_handler.handle_incoming_eof(
                    body,
                    stream="movies",
                    joiner_state=self.state,
                    join_strategy=self.join_strategy,
                    publisher=self._movies_publisher,
                    output_producer=self.output_producer,
                    seq_gen=getattr(self.join_strategy, "_seqgen", None),
                    seq_mon=getattr(self.join_strategy, "_seq_monitor", None),
                )
                return
        except Exception as exc:  # pylint: disable=broad-except
            logging.error("[Node] Error processing movie message: %s", exc, exc_info=True)
            pass

        try:
            self._interactor.handle_movie_body(body)

        except RequeueException:
            raise  # Re-raise so RabbitMQ requeues it
        except Exception as exc:  # pylint: disable=broad-except
            logging.error("[Node] Error processing movie message: %s", exc, exc_info=True)
            raise

    def _process_other_message(self, _ch, _method, _properties, body):
        if self._should_requeue():
            raise RequeueException()

        # Ring EOF path first
        try:
            if self.protocol.is_stream_eof(body):
                self._ring_handler.handle_incoming_eof(
                    body,
                    stream="other",
                    joiner_state=self.state,
                    join_strategy=self.join_strategy,
                    publisher=self._other_publisher,
                    output_producer=self.output_producer,
                    seq_gen=getattr(self.join_strategy, "_seqgen", None),
                    seq_mon=getattr(self.join_strategy, "_seq_monitor", None),
                )
                return
        except Exception as exc:  # pylint: disable=broad-except
            logging.error("[Node] Error processing other message: %s", exc, exc_info=True)
            pass

        try:
            self._interactor.handle_other_body(body)

        except RequeueException:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            logging.error("[Node] Error processing other message: %s", exc, exc_info=True)
            raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _setup_signal_handlers(self) -> None:
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, _frame):
        logging.warning("Received signal %s. Initiating shutdown...", signum)
        self.stop()

    # ------------------------------------------------------------------
    # High-level movie helpers
    # ------------------------------------------------------------------
    def _should_requeue(self) -> bool:
        """Return ``True`` when node is stopping and delivery must be requeued."""
        return self._stop_event.is_set()