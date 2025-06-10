from abc import ABC, abstractmethod

class JoinStrategy(ABC):
    """
    Abstract base class for defining a join strategy.
    This strategy is triggered only when all data for a client has arrived.
    """

    @abstractmethod
    def trigger_final_processing(self, client_id, state, producer):
        """
        Trigger the final join processing for a given client.

        Args:
            client_id: The ID of the client for which to process data.
            state: The shared state manager.
            producer: The RabbitMQ producer for sending results.
        """
        pass 