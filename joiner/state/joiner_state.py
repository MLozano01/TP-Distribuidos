import threading
import logging

class JoinerState:
    def __init__(self):
        self.movies_buffer = {}  # {client_id: {movie_id: movie_data}}
        self.other_buffer = {}   # {client_id: {movie_id: other_data}}
        self.movies_eof_received = set()
        self.other_eof_received = set()
        self._lock = threading.Lock()
        logging.info("JoinerState initialized.")

    def add_movie(self, client_id, movie):
        with self._lock:
            if client_id not in self.movies_buffer:
                self.movies_buffer[client_id] = {}
                logging.info(f"[State] Created movies_buffer for new client {client_id}")
            self.movies_buffer[client_id][movie.id] = movie

    def set_movies_eof(self, client_id):
        with self._lock:
            logging.info(f"[State] Setting movies EOF for client {client_id}. Current state: movies_eof={self.movies_eof_received}, other_eof={self.other_eof_received}")
            self.movies_eof_received.add(client_id)
            is_other_eof_present = client_id in self.other_eof_received
            logging.info(f"[State] Movies EOF for client {client_id} set. Other EOF present: {is_other_eof_present}")
            return is_other_eof_present

    def set_other_eof(self, client_id):
        with self._lock:
            logging.info(f"[State] Setting other EOF for client {client_id}. Current state: movies_eof={self.movies_eof_received}, other_eof={self.other_eof_received}")
            if client_id not in self.other_eof_received:
                self.other_eof_received.add(client_id)
                is_movies_eof_present = client_id in self.movies_eof_received
                logging.info(f"[State] Other EOF for client {client_id} set. Movies EOF present: {is_movies_eof_present}")
                return is_movies_eof_present
            logging.warning(f"[State] Duplicate other EOF received for client {client_id}")
            return False

    def get_movies_buffer(self, client_id):
        with self._lock:
            return self.movies_buffer.get(client_id, {})

    def get_other_buffer(self, client_id):
        with self._lock:
            return self.other_buffer.get(client_id, {})
            
    def get_and_lock_buffers(self, client_id):
        """
        Context manager to safely access both buffers for a client.
        """
        self._lock.acquire()
        try:
            yield self.movies_buffer.get(client_id, {}), self.other_buffer.get(client_id, {})
        finally:
            self._lock.release()

    def pop_buffers_for_processing(self, client_id):
        """
        Atomically pops all data associated with a client ID for final processing.
        This includes buffers and EOF flags.
        """
        with self._lock:
            movies_buffer = self.movies_buffer.pop(client_id, {})
            other_buffer = self.other_buffer.pop(client_id, {})
            
            self.movies_eof_received.discard(client_id)
            self.other_eof_received.discard(client_id)
            
            logging.info(f"[State] Popped and cleaned buffers for client {client_id}. Remaining clients in movies_eof: {self.movies_eof_received}, in other_eof: {self.other_eof_received}")
            return movies_buffer, other_buffer

    def add_other_data(self, client_id, movie_id, data, aggregate=False):
        with self._lock:
            if client_id not in self.other_buffer:
                self.other_buffer[client_id] = {}
                logging.info(f"[State] Created other_buffer for new client {client_id}")
            
            if aggregate:
                # Assuming data is a tuple (value, 1) for aggregation
                current_sum, current_count = self.other_buffer[client_id].get(movie_id, (0.0, 0))
                new_sum = current_sum + data[0]
                new_count = current_count + 1
                self.other_buffer[client_id][movie_id] = (new_sum, new_count)
            else:
                # Assuming data is an object to be appended to a list
                if movie_id not in self.other_buffer[client_id]:
                    self.other_buffer[client_id][movie_id] = []
                self.other_buffer[client_id][movie_id].append(data) 