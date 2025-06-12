import logging
from logic.join_strategy import JoinStrategy
from protocol.protocol import Protocol
from messaging.messaging_utils import send_finished_signal
from protocol import files_pb2

class CreditsJoinStrategy(JoinStrategy):
    """
    Joining strategy for movie credits in a streaming fashion.
    """
    def __init__(self):
        super().__init__()

    def process_other_message(self, body, state, producer):
        credits_msg = self.protocol.decode_credits_msg(body)
        if not credits_msg:
            logging.warning("[Node] Could not decode credits message.")
            return

        client_id = credits_msg.client_id
        
        if credits_msg.finished:
            logging.info(f"Credits EOF received for client {client_id}.")
            self.process_other_eof(client_id, state)
            # Forward the EOF signal
            send_finished_signal(producer, client_id, self.protocol)
            return

        for credit in credits_msg.credits:
            movie_data = state.get_movie(client_id, credit.id)
            if movie_data:
                # Movie is already in our buffer, join and send immediately
                self._join_and_send([credit], movie_data, client_id, producer)
            else:
                # Movie not seen yet. Check if movie stream has ended.
                if state.has_movies_eof(client_id):
                    logging.warning(f"Credit for movie {credit.id} for client {client_id} arrived after movies EOF, and movie not in buffer. Discarding.")
                else:
                    # Movie stream is still active, so buffer the credit.
                    state.add_unmatched_other(client_id, credit.id, credit)

    def _join_and_send(self, credits, movie_data, client_id, producer):
        if not credits:
            return
        
        credit = credits[0] # Process one by one
        participations = []
        for cast_member in credit.cast:
            actor_name = cast_member.name.strip()
            if not actor_name or actor_name in {'\\N', 'NULL', 'null', 'N/A', '-'}:
                continue
            participation = self._create_participation(movie_data, actor_name)
            participations.append(participation)

        if not participations:
            return
            
        try:
            # This will probably send a list of ActorParticipation
            msg = self.protocol.encode_actor_participations_msg(participations, client_id)
            producer.publish(msg)

        except Exception as e:
            logging.error(f"Error sending actor participations for client {client_id}, movie {movie_data.id}: {e}", exc_info=True)

    def process_unmatched_data(self, unmatched_credits, movie_data, client_id, producer):
        logging.info(f"Processing {len(unmatched_credits)} unmatched credits for movie {movie_data.id}, client {client_id}")
        # In credits, we can batch the previously unmatched credits for a movie
        self._join_and_send(unmatched_credits, movie_data, client_id, producer)

    def process_other_eof(self, client_id, state):
        logging.info(f"Processing credits EOF for client {client_id}. Clearing all related state.")
        state.clear_client_state(client_id)

    def _create_participation(self, movie, actor_name):
        """Return an ActorParticipation with just actor_name and movie_id."""
        return files_pb2.ActorParticipation(
            actor_name=actor_name,
            movie_id=movie.id,
        ) 