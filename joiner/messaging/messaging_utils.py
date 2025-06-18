import logging
from protocol.protocol import Protocol

BATCH_SIZE = 60000

def send_movie_batch(producer, movie_list, client_id, protocol):
    if not movie_list:
        return
    try:
        encoded_batch = protocol.encode_movies_msg(movie_list, client_id)
        producer.publish(encoded_batch)
        logging.info(f"Sent batch of {len(movie_list)} movies for client {client_id}")
    except Exception as e:
        logging.error(f"Error sending movie batch for client {client_id}: {e}", exc_info=True)

def send_actor_participations_batch(producer, participations_list, client_id, protocol):
    """Sends a batch of actor participations."""
    if not participations_list:
        return
    try:
        encoded_batch = protocol.encode_actor_participations_msg(participations_list, client_id)
        producer.publish(encoded_batch)
        logging.info(f"Sent batch of {len(participations_list)} actor participations for client {client_id}")
    except Exception as e:
        logging.error(f"Error sending actor participations batch for client {client_id}: {e}", exc_info=True)

def send_finished_signal(producer, client_id, protocol):
    """Sends a finished signal for a specific client."""
    try:
        finished_msg = protocol.encode_movies_msg([], client_id, finished=True)
        producer.publish(finished_msg)
        logging.info(f"Sent FINISHED signal for client {client_id}.")
    except Exception as e:
        logging.error(f"Error sending FINISHED signal for client {client_id}: {e}", exc_info=True) 