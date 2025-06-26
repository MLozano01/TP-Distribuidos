import logging
from protocol import files_pb2

BATCH_SIZE = 60000

def send_movie_batch(producer, movie_list, client_id, protocol):
    if not movie_list:
        return
    try:
        encoded_batch = protocol.encode_movies_msg(movie_list, int(client_id))
        producer.publish(encoded_batch)
        logging.info(f"Sent batch of {len(movie_list)} movies for client {client_id}")
    except Exception as e:
        logging.error(f"Failed to send movie batch: {e}")
        raise

def send_actor_participations_batch(producer, participations_list, client_id, protocol):
    """Sends a batch of actor participations."""
    if not participations_list:
        return
    try:
        encoded_batch = protocol.encode_actor_participations_msg(participations_list, int(client_id))
        producer.publish(encoded_batch)
        logging.info(f"Sent batch of {len(participations_list)} actor participations for client {client_id}")
    except Exception as e:
        logging.error(f"Failed to send actor participations batch: {e}")
        raise

def send_finished_signal(
    producer,
    client_id: str,
    secuence_number: int,
    expected_batches: int,
):
    """Build and emit the FINISHED protobuf that matches *join_strategy*.

    • *secuence_number* – last global SN generated for *client_id* (duplicate
      detection downstream).
    • *expected_batches* – total number of data batches produced for that
      client (stride-aware completeness check in reducer).
    """
    try:
        msg = files_pb2.MoviesCSV()
        msg.client_id = int(client_id)
        msg.finished = True
        msg.secuence_number = int(secuence_number)
        msg.expected_batches = int(expected_batches)

        producer.publish(msg.SerializeToString())
        logging.info(
            "Sent FINISHED signal for client %s seq=%s expected_batches=%s",
            client_id,
            secuence_number,
            expected_batches,
        )
    except Exception as e:
        logging.error("Failed to send FINISHED signal: %s", e)
        raise 