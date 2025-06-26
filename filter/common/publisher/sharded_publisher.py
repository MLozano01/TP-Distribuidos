import logging
from .publisher import Publisher
from protocol.rabbit_protocol import RabbitMQ
from protocol import files_pb2

class ShardedPublisher(Publisher):
    def __init__(self, protocol, exchange_snd_ratings, exc_snd_type_ratings, exchange_snd_credits, exc_snd_type_credits):
        self.protocol = protocol
        self.exchange_snd_ratings = exchange_snd_ratings
        self.exc_snd_type_ratings = exc_snd_type_ratings
        self.exchange_snd_credits = exchange_snd_credits
        self.exc_snd_type_credits = exc_snd_type_credits
        self.queue_snd_movies_to_ratings_joiner = None
        self.queue_snd_movies_to_credits_joiner = None

    def setup_queues(self):
        self.queue_snd_movies_to_ratings_joiner = RabbitMQ(
            self.exchange_snd_ratings, None, "", self.exc_snd_type_ratings
        )
        self.queue_snd_movies_to_credits_joiner = RabbitMQ(
            self.exchange_snd_credits, None, "", self.exc_snd_type_credits
        )

        # Fan-out list for easy iteration when publishing
        self._targets = [
            self.queue_snd_movies_to_ratings_joiner,
            self.queue_snd_movies_to_credits_joiner,
        ]

    def _fanout(self, payload: bytes, routing_key: str) -> None:
        # Trace every message leaving the filter with its routing key and size.
        logging.info(
            "[ShardedPublisher] Fan-out payload bytes=%s routing_key=%s",
            len(payload),
            routing_key,
        )
        for q in self._targets:
            q.publish(payload, routing_key=routing_key)

    def publish(self, result_list, client_id, secuence_number, discarded_count: int = 0):
        """Publish *result_list* (list of MovieCSV) sharded by movie_id.

        If *result_list* is empty we still send **one** MoviesCSV message with
        an empty list and the *discarded_count* so downstream joiners can keep
        accurate counters.
        """

        if result_list:
            first = True
            for mv in result_list:
                msg = files_pb2.MoviesCSV(
                    client_id=client_id,
                    secuence_number=secuence_number,
                    discarded_count=discarded_count if first else 0,
                )
                msg.movies.append(mv)

                self._fanout(msg.SerializeToString(), routing_key=str(mv.id))
                if first and discarded_count:
                    logging.info(
                        "[ShardedPublisher] Client %s seq=%s discarded_count=%s",
                        client_id,
                        secuence_number,
                        discarded_count,
                    )
                first = False

            logging.info(
                "Published %s movie batches (seq=%s, discarded=%s) to joiners.",
                len(result_list),
                secuence_number,
                discarded_count,
            )
        else:
            # All movies were discarded for that batch – still send the discarded amount info
            msg = files_pb2.MoviesCSV(
                client_id=client_id,
                secuence_number=secuence_number,
                discarded_count=discarded_count,
            )
            self._fanout(msg.SerializeToString(), routing_key="discard")
            logging.info(
                "Published discard-only batch (seq=%s, discarded=%s) to joiners.",
                secuence_number,
                discarded_count,
            )

    def publish_finished_signal(self, msg):
        msg_to_send = msg.SerializeToString()
        pub_routing_key = str(msg.client_id)

        # Publish finished signal to RATINGS joiner
        self.queue_snd_movies_to_ratings_joiner.publish(msg_to_send, pub_routing_key)

        # Publish finished signal to CREDITS joiner
        self.queue_snd_movies_to_credits_joiner.publish(msg_to_send, pub_routing_key)

        logging.info(
            f"Published movie finished signal for client {msg.client_id} to both joiners with routing_key={pub_routing_key}."
        )

    def close(self):
        if self.queue_snd_movies_to_ratings_joiner:
            try:
                self.queue_snd_movies_to_ratings_joiner.close_channel()
            except Exception as e:
                logging.error(f"Error closing ratings sender channel: {e}")

        if self.queue_snd_movies_to_credits_joiner:
            try:
                self.queue_snd_movies_to_credits_joiner.close_channel()
            except Exception as e:
                logging.error(f"Error closing credits sender channel: {e}") 