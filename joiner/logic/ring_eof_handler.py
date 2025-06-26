import logging
from typing import Dict, Optional, Any

from protocol import files_pb2
from protocol.rabbit_protocol import RabbitMQ
from joiner.state.joiner_state import JoinerState
from joiner.logic.join_strategy import JoinStrategy
from messaging.messaging_utils import send_finished_signal


class EofRingHandler:
    def __init__(
        self,
        replica_id: int,
        replicas_count: int,
        movies_queue_base: str,
        other_queue_base: str,
        state_dir: str = "/backup",
    ):
        self.replica_id = replica_id
        self.replicas_count = replicas_count
        self.state_dir = state_dir
        self._movies_q_base = str(movies_queue_base)
        self._other_q_base = str(other_queue_base)


    def on_local_eof(
        self, stream: str, client_id: str,
        total_processed_local: int,
        total_to_process: Optional[int],
        highest_sn_local: int,
        publisher: RabbitMQ,
    ) -> None:
        """Called when *this* replica sees a .finished message."""
        logging.info(f"Replica {self.replica_id} saw local EOF for client {client_id} on stream {stream} total_to_process={total_to_process} highest_sn_local={highest_sn_local}")
        
        # Build and inject the first StreamEOF for this stream.
        is_for_movies = (stream == "movies")
        eof_msg = files_pb2.StreamEOF(
            client_id=client_id,
            total_to_process=total_to_process,
            highest_sn_produced=highest_sn_local,
            for_movies=is_for_movies
        )
        
        # Add self to processed list
        processed_count = files_pb2.ProcessedCount(
            replica_id=self.replica_id,
            total_processed=total_processed_local
        )
        eof_msg.processed.append(processed_count)

        self._send_to_next_replica(eof_msg, stream, publisher)
        logging.info(
            "Replica %s initiated EOF ring for client %s, stream %s",
            self.replica_id,
            client_id,
            stream,
        )

    def handle_incoming_eof(
        self,
        body: bytes,
        stream: str,
        joiner_state: JoinerState,
        join_strategy: JoinStrategy,
        publisher: RabbitMQ,
        output_producer: RabbitMQ,
        seq_gen,
        seq_mon
    ) -> None:
        """Merge/forward StreamEOFs arriving from the ring."""
        eof_msg = files_pb2.StreamEOF()
        eof_msg.ParseFromString(body)
        client_id = eof_msg.client_id
        stream_key = "movies" if stream == "movies" else "other"

        # Always refresh (upsert) our ProcessedCount entry so the message carries
        # up-to-date numbers
        if stream_key == "movies":
            my_processed = joiner_state.get_movies_processed(client_id)
        else:
            my_processed = joiner_state.get_processed_count(client_id)

        # Remove any previous entry for this replica and append the updated ProcessedCount.
        # Direct slice assignment on a protobuf repeated composite field container is not
        # supported (raises TypeError).  Instead, build a filtered list, clear the field,
        # and extend it with the new contents.

        # Build filtered list without our own replica entry
        filtered = [p for p in eof_msg.processed if p.replica_id != self.replica_id]

        # Clear current contents and re-populate
        eof_msg.ClearField("processed")
        eof_msg.processed.extend(filtered)

        # Finally add (or refresh) our own up-to-date counters
        eof_msg.processed.append(
            files_pb2.ProcessedCount(replica_id=self.replica_id, total_processed=my_processed)
        )

        # Update highest sequence number if we have one
        current_sn = seq_gen.current(client_id)
        if current_sn > eof_msg.highest_sn_produced:
            eof_msg.highest_sn_produced = current_sn

        # Inverse-stream confirmation logic
        inverse_stream_key = "other" if stream_key == "movies" else "movies"
        inverse_done = joiner_state.has_eof(client_id, inverse_stream_key)
        if inverse_done and self.replica_id not in eof_msg.inverse_stream_confirmed:
            eof_msg.inverse_stream_confirmed.append(self.replica_id)

        # Check if the total_to_process matches the sum of all the processed counts
        # It means the EOF is in the correct SN order for the client
        total_processed_distributed = sum(p.total_processed for p in eof_msg.processed)
        logging.debug(
            "Replica %s incoming EOF client=%s stream=%s total_to_process=%s total_processed_distributed=%s",
            self.replica_id,
            client_id,
            stream,
            eof_msg.total_to_process,
            total_processed_distributed,
        )
        if eof_msg.total_to_process == total_processed_distributed:
            joiner_state.set_stream_eof(client_id, stream_key)
            if stream_key == "movies":
                joiner_state.purge_orphan_other_after_movie_eof(client_id)
            
            logging.info(f"Replica {self.replica_id} set stream eof for client {client_id} on stream {stream_key}")

            if joiner_state.has_both_eof(client_id):
                join_strategy.handle_client_finished(client_id, joiner_state)

            # Register that *this* replica has fully processed the stream.
            if self.replica_id not in eof_msg.replicas_confirmed:
                eof_msg.replicas_confirmed.append(self.replica_id)

            local_batches = join_strategy.get_flushed_batches(client_id) or 0

            filtered_bc = [b for b in eof_msg.batch_counts if b.replica_id != self.replica_id]
            eof_msg.ClearField("batch_counts")
            eof_msg.batch_counts.extend(filtered_bc)
            eof_msg.batch_counts.append(
                files_pb2.BatchCount(replica_id=self.replica_id, batches_sent=local_batches)
            )
            
            if joiner_state.has_both_eof(client_id):
                join_strategy.clear_flushed_batches(client_id)

        current_stream_all_confirmed = len(eof_msg.replicas_confirmed) == self.replicas_count
        inverse_stream_all_confirmed = len(eof_msg.inverse_stream_confirmed) == self.replicas_count
        
        if current_stream_all_confirmed and inverse_stream_all_confirmed:
            logging.info(
                "EOF ring fully closed for client %s both streams. The last stream was %s. and highest_sn_produced was %s",
                client_id,
                stream,
                eof_msg.highest_sn_produced
            )
            # Derive the correct final sequence number: we only need +1 when
            # the last *produced* sequence belongs to the **last** replica in
            # the stride (i.e. highest_sn % R == R-1).  Otherwise the current
            # highest is already the (N-1)th item and represents exactly the
            # total batch count.

            R = self.replicas_count
            highest = eof_msg.highest_sn_produced
            if (highest % R) == (R - 1):
                final_seq = highest + 1
            else:
                final_seq = highest

            # Total batches = sum over replicas
            expected_batches = sum(b.batches_sent for b in eof_msg.batch_counts)

            send_finished_signal(
                output_producer,
                str(client_id),
                final_seq,
                expected_batches
            )
            # Ring termination – do NOT forward further.
            return
        
        if current_stream_all_confirmed:
            return

        # Forward to next replica so the ring continues.
        self._send_to_next_replica(eof_msg, stream, publisher)
        return

    def _send_to_next_replica(self, eof_msg: files_pb2.StreamEOF, stream: str, publisher: RabbitMQ) -> None:
        """Forward *eof_msg* to the physical queue of the next replica.

        The publisher MUST be configured for the **default exchange** ("") so
        that the *routing_key* equals the destination queue name.
        """
        next_replica_id = self.replica_id + 1 if self.replica_id < self.replicas_count else 1

        base = self._movies_q_base if stream == "movies" else self._other_q_base
        next_queue = f"{base}{next_replica_id}"

        publisher.publish(eof_msg.SerializeToString(), routing_key=next_queue)
        #logging.info(
        #    "Replica %s forwarded EOF to client %s to queue %s (stream %s)",
        #    self.replica_id,
        #    eof_msg.client_id,
        #    next_queue,
        #    stream,
        #)
