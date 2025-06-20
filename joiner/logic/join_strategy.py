from abc import ABC, abstractmethod
import logging
from protocol.protocol import Protocol

class JoinStrategy(ABC):
    """
    Abstract base class for defining a join strategy in a streaming manner.
    """
    def __init__(self):
        self.protocol = Protocol()

    @abstractmethod
    def process_other_message(self, body, state, producer):
        """
        Process a message of the 'other' data type.

        Args:
            body: The message body.
            state: The shared state manager.
            producer: The RabbitMQ producer for sending results.
        """
        pass
    
    @abstractmethod
    def _join_and_send(self, other_data_list, movie_data, client_id, producer):
        """
        Joins the 'other' data with movie data and sends it downstream.
        """
        pass

    @abstractmethod
    def process_unmatched_data(self, unmatched_data, movie_data, client_id, producer):
        """
        Process data that was buffered and is now matched with an arriving movie.
        """
        pass

    @abstractmethod
    def handle_movie_eof(self, client_id, state):
        """Called when the movies stream finishes for *client_id*.
        The default implementation may purge now-orphan *other* data.
        """
        pass

    @abstractmethod
    def handle_client_finished(self, client_id, state, producer):
        """Called exactly once per client when *both* streams have signalled EOF.

        The method is responsible for emitting the consolidated EOF message
        downstream (if required by the protocol).
        """
        pass 