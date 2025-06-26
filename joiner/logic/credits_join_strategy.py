from __future__ import annotations
import logging
import os
from logic.join_strategy import JoinStrategy
from messaging.messaging_utils import send_finished_signal
from protocol import files_pb2
from common.batcher import PerClientBatcher
from common.requeue import RequeueException

class CreditsJoinStrategy(JoinStrategy):
    """Join strategy that produces ActorParticipations from movie credits."""

    def __init__(self, replica_id: int, replicas_count: int, state_manager):
        super().__init__(state_manager)
        from common.sequence_generator import SequenceGenerator
        self._replica_id = replica_id
        self._seqgen = SequenceGenerator(replica_id, replicas_count, namespace="actors")
        self._batcher: PerClientBatcher | None = None

    def process_other_message(self, body, state, producer):
        credits_msg = self.protocol.decode_credits_msg(body)
        if not credits_msg:
            logging.warning("[CreditsJoinStrategy] Could not decode credits message.")
            return None

        client_id = str(credits_msg.client_id)
        seq = credits_msg.secuence_number
        movie_id_val = credits_msg.credits[0].id if credits_msg.credits else "-"
        logging.info(
            f"[CreditsJoinStrategy] Received msg client={client_id} seq={seq} movie_id={movie_id_val} items={len(credits_msg.credits)}"
        )

        if self._seq_monitor.is_duplicate(client_id, seq):
            logging.info(
                f"[CreditsJoinStrategy] Dropping duplicate OTHER message seq={seq} client={client_id}"
            )
            return None

        # EOF path --------------------------------------------------------
        if credits_msg.finished:
            processed_total = self._seq_monitor.get_num_unique(client_id)

            logging.info(
                f"[RatingsJoinStrategy] FINISHED received – client={client_id} total_to_process={seq} processed={processed_total}"
            )

            if seq is not None and int(seq) != processed_total:
                logging.warning(
                    f"[RatingsJoinStrategy] Mismatch total_to_process (expected={seq}, processed={processed_total}) – requeuing EOF."
                )
                raise RequeueException()


            state.set_stream_eof(client_id, "other")
            return client_id

        # Data path -------------------------------------------------------
        for credit in credits_msg.credits:
            actor_names = [cm.name.strip() for cm in credit.cast if cm.name.strip() not in {"\\N", "NULL", "null", "N/A", "-"}]

            movie_title = state.get_movie(client_id, credit.id)
            if movie_title:
                self._join_and_batch(actor_names, credit.id, movie_title, client_id, producer)
                continue

            if state.has_eof(client_id, "movies"):
                logging.debug(
                    f"[CreditsJoinStrategy] Discarding credit for movie {credit.id} (client {client_id}) – movies EOF already received."
                )
                continue

            state.buffer_other(client_id, credit.id, actor_names)

        self._seq_monitor.record(client_id, seq)
        self._snapshot_if_needed(client_id)
        return None

    def _join_and_batch(self, actor_names, movie_id, title, client_id, producer):
        if not actor_names:
            return
        # *actor_names* may be a list OR a tuple of lists (unmatched buffer).
        flat_names = []
        for n in actor_names:
            if isinstance(n, list):
                flat_names.extend(n)
            else:
                flat_names.append(n)

        participations = [
            files_pb2.ActorParticipation(actor_name=name, movie_id=movie_id)
            for name in flat_names
            if name
        ]

        if not participations:
            return
        try:
            self._ensure_batcher(producer)

            for part in participations:
                self._batcher.add(part, client_id)
        except Exception as exc:
            logging.error(
                f"Failed to batch actor participations – client {client_id} movie {movie_id}: {exc}",
                exc_info=True,
            )
            raise

    def process_unmatched_data(self, unmatched_actor_names, movie_id, title, client_id, producer):
        # Flatten tuple of lists into single list
        flat = []
        for item in unmatched_actor_names:
            if isinstance(item, list):
                flat.extend(item)
            else:
                flat.append(item)
        self._join_and_batch(flat, movie_id, title, client_id, producer)


    def handle_movie_eof(self, client_id, state):
        # Credits that correspond to movies that never arrived can be discarded.
        state.purge_orphan_other_after_movie_eof(client_id)

    def handle_client_finished(self, client_id, state, producer):
        if self._batcher:
            self._batcher.flush_key(client_id)
            self._batcher.clear(client_id)
        last_seq = self._seqgen.current(client_id)
        send_finished_signal(producer, client_id, self.protocol, secuence_number=last_seq)
        self._seqgen.clear(client_id)

        # Cleaning sequence numbers for client
        self._seq_monitor.clear_client(client_id)

    def _ensure_batcher(self, producer):
        if self._batcher is not None:
            return

        batch_size = int(os.getenv("JOINER_BATCH_SIZE", "100"))

        from protocol.protocol import Protocol
        proto = Protocol()

        def _encode_batch(parts, cid):
            from protocol import files_pb2
            batch_pb = files_pb2.ActorParticipationsBatch(client_id=cid)
            batch_pb.participations.extend(parts)
            batch_pb.secuence_number = self._seqgen.next(str(cid))
            return batch_pb.SerializeToString()

        self._batcher = PerClientBatcher(
            producer,
            _encode_batch,
            max_items=batch_size,
            namespace=f"actors_r{self._replica_id}",
        )

    def _snapshot_if_needed(self, client_id):
        if self._batcher is not None:
            try:
                self._batcher.snapshot_key(client_id)
            except Exception as exc:
                logging.error("Error snapshotting batch for client %s: %s", client_id, exc)
                raise