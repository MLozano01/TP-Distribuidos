from abc import ABC, abstractmethod
import logging
from protocol.protocol import Protocol
from common.sequence_number_monitor import SequenceNumberMonitor

class JoinStrategy(ABC):
    """
    Abstract base class for defining a join strategy in a streaming manner.
    """
    def __init__(self, state_manager):
        self.protocol = Protocol()
        self._seq_monitor = SequenceNumberMonitor(state_manager)

    @abstractmethod
    def process_other_message(self, body, state, producer, eof_protocol_producer):
        """
        Process a message of the 'other' data type.

        Args:
            body: The message body.
            state: The shared state manager.
            producer: The RabbitMQ producer for sending results.
        """
        pass
    
    @abstractmethod
    def _join_and_batch(self, other_data_list, movie_id, title, client_id, producer):
        """Transform *other_data_list* + movie info into a protobuf record and
        add it to the internal batcher.
        """
        pass

    @abstractmethod
    def process_unmatched_data(self, unmatched_data, movie_id, title, client_id, producer):
        """
        Process data that was buffered and is now matched with an arriving movie.
        """
        pass

    @abstractmethod
    def handle_client_finished(self, client_id, state, producer, highest_sn_produced):
        """Called exactly once per client when *both* streams have signalled EOF.

        The method is responsible for emitting the consolidated EOF message
        downstream (if required by the protocol).
        """
        pass 