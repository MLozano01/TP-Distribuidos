from abc import ABC, abstractmethod

class Publisher(ABC):
    @abstractmethod
    def setup_queues(self):
        pass

    @abstractmethod
    def publish(self, filtered_batch, client_id, secuence_number, discarded_count: int = 0):
        pass

    @abstractmethod
    def publish_finished_signal(self, msg):
        pass

    @abstractmethod
    def close(self):
        pass 