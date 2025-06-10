import logging
from logic.join_strategy import JoinStrategy
from protocol.protocol import Protocol
from messaging.messaging_utils import send_movie_batch, send_actor_participations_batch, send_finished_signal, BATCH_SIZE
from protocol import files_pb2

class CreditsJoinStrategy(JoinStrategy):
    """
    Joining strategy for movie credits.
    """
    def __init__(self):
        self.protocol = Protocol()

    def process_other_message(self, body, state, producer):
        credits_msg = self.protocol.decode_credits_msg(body)
        if not credits_msg:
            logging.warning("[Node] Could not decode credits message.")
            return

        client_id = credits_msg.client_id
        logging.info(f"[Node] Processing credits message for client {client_id}. Finished: {credits_msg.finished}, Items: {len(credits_msg.credits)}")

        if credits_msg.finished:
            if state.set_other_eof(client_id):
                self.trigger_final_processing(client_id, state, producer)
            return

        for credit in credits_msg.credits:
            state.add_other_data(client_id, credit.id, credit)

    def trigger_final_processing(self, client_id, state, producer):
        logging.info(f"Triggering final processing for CREDITS join, client {client_id}")
        
        movies_buffer, other_buffer = state.pop_buffers_for_processing(client_id)
        if not movies_buffer:
            logging.warning(f"No movie data to process for client {client_id}, ending.")
            send_finished_signal(producer, client_id, self.protocol)
            return
            
        participations_batch = []

        for movie_id, credits_list in other_buffer.items():
            if movie_id in movies_buffer:
                for credit in credits_list:
                    for cast_member in credit.cast:
                        actor_name = cast_member.name.strip()
                        if not actor_name or actor_name in {'\\N', 'NULL', 'null', 'N/A', '-'}:
                            continue
                        participation = self._create_participation(movies_buffer[movie_id], actor_name)
                        participations_batch.append(participation)

                if len(participations_batch) >= BATCH_SIZE:
                    send_actor_participations_batch(producer, participations_batch, client_id, self.protocol)
                    participations_batch = []

        if participations_batch:
            send_actor_participations_batch(producer, participations_batch, client_id, self.protocol)

        send_finished_signal(producer, client_id, self.protocol)
        logging.info(f"Finished processing for CREDITS join, client {client_id}")

    def _create_participation(self, movie, actor_name):
        """Return an ActorParticipation with just actor_name and movie_id."""

        return files_pb2.ActorParticipation(
            actor_name=actor_name,
            movie_id=movie.id,
        ) 