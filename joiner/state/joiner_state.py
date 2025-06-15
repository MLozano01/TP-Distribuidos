import threading
import logging

class JoinerState:
    def __init__(self):
        self.movies_buffer = {}  # {client_id: {movie_id: movie_data}}
        self.unmatched_other_buffer = {}  # {client_id: {movie_id: [data, data, ...]}}
        self.movies_eof_received = set()
        self._lock = threading.Lock()
        logging.info("JoinerState initialized.")

    def add_movie_and_process_unmatched(self, client_id, movie):
        with self._lock:
            if client_id not in self.movies_buffer:
                self.movies_buffer[client_id] = {}
            self.movies_buffer[client_id][movie.id] = movie

            unmatched_data = []
            if client_id in self.unmatched_other_buffer:
                if movie.id in self.unmatched_other_buffer[client_id]:
                    unmatched_data = self.unmatched_other_buffer[client_id].pop(movie.id)
                    if not self.unmatched_other_buffer[client_id]:
                        del self.unmatched_other_buffer[client_id]
        return unmatched_data

    def get_movie(self, client_id, movie_id):
        with self._lock:
            if client_id in self.movies_buffer:
                return self.movies_buffer[client_id].get(movie_id)
            return None

    def add_unmatched_other(self, client_id, movie_id, data):
        with self._lock:
            if client_id not in self.unmatched_other_buffer:
                self.unmatched_other_buffer[client_id] = {}
            
            if movie_id not in self.unmatched_other_buffer[client_id]:
                self.unmatched_other_buffer[client_id][movie_id] = []
            
            self.unmatched_other_buffer[client_id][movie_id].append(data)

    def set_movies_eof(self, client_id):
        with self._lock:
            self.movies_eof_received.add(client_id)
            logging.info(f"Movies EOF for client {client_id} received. Cleaning up unmatched ratings.")
            
            if client_id in self.unmatched_other_buffer:
                movies_for_client = self.movies_buffer.get(client_id, {})
                unmatched_ratings_for_client = self.unmatched_other_buffer[client_id]
                
                movie_ids_to_purge = [
                    movie_id for movie_id in unmatched_ratings_for_client
                    if movie_id not in movies_for_client
                ]
                
                if movie_ids_to_purge:
                    logging.info(f"Purging {len(movie_ids_to_purge)} unmatched ratings for client {client_id} that have no matching movie.")
                    for movie_id in movie_ids_to_purge:
                        del unmatched_ratings_for_client[movie_id]

                if not unmatched_ratings_for_client:
                    del self.unmatched_other_buffer[client_id]

    def has_movies_eof(self, client_id):
        # No lock needed for simple set lookup (atomic)
        return client_id in self.movies_eof_received

    def clear_client_state(self, client_id):
        """
        Clears all state associated with a client ID.
        This could be called, for example, after the "other" stream's EOF is received.
        """
        with self._lock:
            self.movies_buffer.pop(client_id, None)
            self.unmatched_other_buffer.pop(client_id, None)
            self.movies_eof_received.discard(client_id)
            logging.info(f"Cleared all state for client {client_id}") 