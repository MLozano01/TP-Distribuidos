import logging
from logic.join_strategy import JoinStrategy
from protocol.protocol import Protocol
from messaging.messaging_utils import send_finished_signal
from protocol import files_pb2

class CreditsJoinStrategy(JoinStrategy):
    """Join strategy that produces ActorParticipations from movie credits."""

    def __init__(self):
        super().__init__()

    # ------------------------------------------------------------------
    # Incoming CREDITS stream
    # ------------------------------------------------------------------
    def process_other_message(self, body, state, producer):
        credits_msg = self.protocol.decode_credits_msg(body)
        if not credits_msg:
            logging.warning("[CreditsJoinStrategy] Could not decode credits message.")
            return None

        client_id = str(credits_msg.client_id)
        logging.debug(
            f"[CreditsJoinStrategy] client={client_id} finished={credits_msg.finished} items={len(credits_msg.credits)}"
        )

        # EOF path --------------------------------------------------------
        if credits_msg.finished:
            state.set_stream_eof(client_id, "other")
            return client_id

        # Data path -------------------------------------------------------
        for credit in credits_msg.credits:
            actor_names = [cm.name.strip() for cm in credit.cast if cm.name.strip() not in {"\\N", "NULL", "null", "N/A", "-"}]

            movie_title = state.get_movie(client_id, credit.id)
            if movie_title:
                # We can directly join and send
                self._join_and_send(actor_names, credit.id, movie_title, client_id, producer)
                continue

            if state.has_eof(client_id, "movies"):
                logging.debug(
                    f"[CreditsJoinStrategy] Discarding credit for movie {credit.id} (client {client_id}) – movies EOF already received."
                )
                continue

            # Buffer each actor name individually for finer granularity.
            state.buffer_other(client_id, credit.id, actor_names)
        return None

    # ------------------------------------------------------------------
    def _join_and_send(self, actor_names, movie_id, title, client_id, producer):
        if not actor_names:
            return
        participations = [
            files_pb2.ActorParticipation(actor_name=name, movie_id=movie_id)
            for name in actor_names
            if name
        ]

        if not participations:
            return
        try:
            msg = self.protocol.encode_actor_participations_msg(participations, int(client_id))
            producer.publish(msg)
        except Exception as exc:
            logging.error(
                f"Failed to emit actor participations – client {client_id} movie {movie_id}: {exc}",
                exc_info=True,
            )

    def process_unmatched_data(self, unmatched_actor_names, movie_id, title, client_id, producer):
        self._join_and_send(unmatched_actor_names, movie_id, title, client_id, producer)

    # ------------------------------------------------------------------
    # EOF hooks
    # ------------------------------------------------------------------
    def handle_movie_eof(self, client_id, state):
        # Credits that correspond to movies that never arrived can be discarded.
        state.purge_orphan_other_after_movie_eof(client_id)

    def handle_client_finished(self, client_id, state, producer):
        send_finished_signal(producer, client_id, self.protocol) 