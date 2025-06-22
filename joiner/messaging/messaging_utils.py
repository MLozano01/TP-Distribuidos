import logging
from protocol.protocol import Protocol

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

def send_finished_signal(producer, client_id, protocol, secuence_number=None):
    """Sends a finished signal for a specific client.

    The joiner must propagate the *last* sequence number it used for that
    client so that downstream components (aggregators, reducers) can apply the
    duplicate-detection logic.
    """
    try:
        if secuence_number is None:
            secuence_number = 0

        from protocol import files_pb2
        movies_pb = files_pb2.MoviesCSV()
        movies_pb.client_id = int(client_id)
        movies_pb.finished = True
        movies_pb.secuence_number = int(secuence_number)
        finished_msg = movies_pb.SerializeToString()
        producer.publish(finished_msg)
        logging.info(f"Sent FINISHED signal for client {client_id}.")
    except Exception as e:
        logging.error(f"Failed to send FINISHED signal: {e}")
        raise 