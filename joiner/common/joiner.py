import logging
import signal
import threading
import pika
from protocol.protocol import Protocol, FileType
from protocol.rabbit_wrapper import RabbitMQConsumer, RabbitMQProducer
from protocol import files_pb2 # Import files_pb2 to access new message types

class Joiner:
    def __init__(self, **kwargs):
        # Configuration
        self.config = kwargs
        self.replica_id = self.config['replica_id']
        # Store other_data_type read from config
        self.other_data_type = kwargs.get('other_data_type', '').upper() # e.g., "RATINGS" or "CREDITS"
        logging.info(f"Initializing Joiner Replica ID: {self.replica_id} for joining with {self.other_data_type}")

        # Protocol and State
        self.protocol = Protocol()
        self.movies_buffer = {}  # {movie_id: movie_data (MovieCSV object)}
        self.other_buffer = {}   # {movie_id: [other_data_item_1, ...]}
        # EOF flags - Need signal from control channel when ALL data of a type is sent globally
        self.movies_eof_received = False
        self.other_eof_received = False
        # self.can_start_joining = False # Removed - join triggered by receiving both EOFs
        self._lock = threading.Lock() # To protect shared buffers and EOF flags

        # RabbitMQ Connections
        self.movies_consumer = None
        self.other_consumer = None
        self.control_consumer = None
        self.output_producer = None

        # Stop event for graceful shutdown
        self._stop_event = threading.Event()

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)

    def _handle_shutdown(self, signum, frame):
        logging.warning(f"Received signal {signum}. Initiating shutdown...")
        self.stop()

    def _settle_connections(self):
        """Instantiate and setup RabbitMQ consumers and producer using new classes."""
        host = self.config['rabbit_host']
        # Calculate positive, 1-based binding key for consistent hash
        # --- Using fixed weight "1" for even distribution --- 
        consistent_hash_binding_key = "1" # Use weight "1" for all replicas
        try:
            # --- Consumers ---
            # Movies Consumer (Consistent Hash Queue)
            self.movies_consumer = RabbitMQConsumer(
                host=host,
                exchange=self.config['exchange_movies'],
                exchange_type='x-consistent-hash',
                queue_name=self.config['queue_movies_name'],
                routing_key=consistent_hash_binding_key # Use fixed weight "1"
                # durable=True by default
            )
            # Other Data Consumer (Consistent Hash Queue)
            self.other_consumer = RabbitMQConsumer(
                host=host,
                exchange=self.config['exchange_other'],
                exchange_type='x-consistent-hash',
                queue_name=self.config['queue_other_name'],
                routing_key=consistent_hash_binding_key # Use fixed weight "1"
                # durable=True by default
            )
            # Control Consumer (Fanout Queue)
            # Queue name is None -> generates exclusive, auto-delete, non-durable queue
            self.control_consumer = RabbitMQConsumer(
                host=host,
                exchange=self.config['exchange_control'],
                exchange_type='fanout',
                queue_name=None,
                routing_key='' # Fanout ignores routing key
            )

            # --- Producer ---
            self.output_producer = RabbitMQProducer(
                host=host,
                exchange=self.config['exchange_output'],
                exchange_type='direct', # Or topic, depending on downstream needs
                routing_key=self.config['routing_key_output'] # Default routing key
                # Producer declares exchange but not queue by default
            )
            logging.info("RabbitMQ connections settled using rabbit_wrapper classes.")

        except Exception as e:
             logging.error(f"Fatal error during RabbitMQ connection setup: {e}", exc_info=True)
             # If setup fails, ensure stop is called to prevent start_consuming on None
             self.stop()
             raise # Reraise to prevent application from continuing in broken state


    def start(self):
        """Start the Joiner consuming messages."""
        self._setup_signal_handlers()
        try:
            self._settle_connections() # Setup connections first

            # Check if connections were successfully settled
            if not all([self.movies_consumer, self.other_consumer, self.control_consumer, self.output_producer]):
                 logging.error("Not all RabbitMQ connections were initialized. Aborting start.")
                 return

            # Setup consumer callbacks (before starting threads)
            # Note: auto_ack=True used as requested
            self.movies_consumer.consume(self._process_movie_message, auto_ack=True)
            self.other_consumer.consume(self._process_other_message, auto_ack=True)
            self.control_consumer.consume(self._process_control_message, auto_ack=True)

            # Start consumers in separate threads by calling start_consuming
            threads = []
            threads.append(threading.Thread(target=self.movies_consumer.start_consuming, daemon=True))
            threads.append(threading.Thread(target=self.other_consumer.start_consuming, daemon=True))
            threads.append(threading.Thread(target=self.control_consumer.start_consuming, daemon=True))

            for t in threads:
                t.start()

            logging.info("Joiner started and consuming...")
            # Keep main thread alive until stop signal
            self._stop_event.wait()

        except pika.exceptions.AMQPConnectionError as e:
            # This might be caught during _settle_connections, but also possible later
            logging.error(f"AMQP Connection Error: {e}. Check RabbitMQ.")
        except Exception as e:
            logging.error(f"Failed to start Joiner: {e}", exc_info=True)
        finally:
            # Ensure stop is called regardless of where the error occurred
            self.stop()

    def _process_movie_message(self, ch, method, properties, body):
        """Callback for processing movie messages."""
        if self._stop_event.is_set(): return
        try:
            movies_msg = self.protocol.decode_movies_msg(body)
            if not movies_msg or not movies_msg.movies:
                logging.warning("Received empty or invalid movie batch.")
                return

            with self._lock:
                for movie in movies_msg.movies:
                    self.movies_buffer[movie.id] = movie
                    logging.info(f"Buffered movie ID {movie.id}")

        except Exception as e:
            logging.error(f"Error processing movie message: {e}", exc_info=True)
            # Consider if this error should trigger a shutdown
            # self._stop_event.set()

    def _process_other_message(self, ch, method, properties, body):
        """Callback for processing other data (ratings/credits)."""
        if self._stop_event.is_set(): return
        try:
            other_msg = None
            data_list = []
            movie_id_field = 'movieId' # Default for ratings

            is_ratings = self.other_data_type == "RATINGS" # Check type once

            if is_ratings:
                other_msg = self.protocol.decode_ratings_msg(body)
                if other_msg and hasattr(other_msg, 'ratings'):
                    data_list = other_msg.ratings
                movie_id_field = 'movieId'
            elif self.other_data_type == "CREDITS":
                other_msg = self.protocol.decode_credits_msg(body)
                if other_msg and hasattr(other_msg, 'credits'):
                    data_list = other_msg.credits
                movie_id_field = 'id' # Credits uses 'id' for movie id
            else:
                logging.error(f"Unsupported OTHER_DATA_TYPE: {self.other_data_type}")
                return

            if not other_msg or not data_list:
                 logging.warning(f"Received empty or invalid {self.other_data_type} batch.")
                 return

            with self._lock: # Acquire lock to modify shared buffer
                for item in data_list:
                    movie_id = getattr(item, movie_id_field, None)
                    if movie_id is None:
                         logging.warning(f"Could not extract movie ID using field '{movie_id_field}' from {self.other_data_type} item.")
                         continue

                    if is_ratings:
                        # Get current sum and count, default to (0.0, 0)
                        current_sum, current_count = self.other_buffer.get(movie_id, (0.0, 0))
                        # Update sum and count
                        new_sum = current_sum + item.rating # Assuming item is RatingCSV
                        new_count = current_count + 1
                        # Store the updated tuple
                        self.other_buffer[movie_id] = (new_sum, new_count)
                        logging.info(f"Buffered movie ID {movie_id} with sum: {new_sum} and count: {new_count}")
                    # --- Modification End ---
                    else: # Handle Credits (buffering the object)
                        if movie_id not in self.other_buffer:
                            self.other_buffer[movie_id] = []
                        self.other_buffer[movie_id].append(item)
                        logging.debug(f"Buffered {self.other_data_type} data for movie ID {movie_id}")

        except Exception as e:
            logging.error(f"Error processing {self.other_data_type} message: {e}", exc_info=True)
            # Consider if this error should trigger a shutdown
            # self._stop_event.set()

    def _process_control_message(self, ch, method, properties, body):
        """Callback for processing control messages (EOF signals)."""
        if self._stop_event.is_set(): return
        try:
            control_signal = body.decode()
            logging.info(f"Received control signal: [{control_signal}]")

            trigger_processing = False
            expected_other_eof = f"EOF_{self.other_data_type}" # e.g., EOF_RATINGS or EOF_CREDITS

            with self._lock: # Acquire lock to modify shared EOF flags and check state
                if control_signal == "EOF_MOVIES" and not self.movies_eof_received:
                    logging.warning("EOF signal received for MOVIES.")
                    self.movies_eof_received = True
                elif control_signal == expected_other_eof and not self.other_eof_received:
                    logging.warning(f"EOF signal received for {self.other_data_type}.")
                    self.other_eof_received = True
                else:
                    logging.warning(f"Received unknown or duplicate control signal: {control_signal}")

                # Check if time to process (both EOFs received)
                if self.movies_eof_received and self.other_eof_received:
                    logging.warning("Both EOF signals received. Triggering final processing.")
                    trigger_processing = True

            if trigger_processing:
                # Perform the final processing step based on joiner type
                if self.other_data_type == "RATINGS":
                    self._process_ratings_join()
                elif self.other_data_type == "CREDITS":
                    self._process_credits_join()
                else:
                     logging.error(f"No final processing logic defined for type: {self.other_data_type}")

        except Exception as e:
            logging.error(f"Error processing control message: {e}", exc_info=True)
            # Consider if this error should trigger a shutdown
            # self._stop_event.set()

    def _process_ratings_join(self):
        """Processes buffered data for RATINGS join, modifying MovieCSV."""
        processed_movies_for_batch = []
        joined_ids = set()
        with self._lock:
            logging.info(f"Processing Ratings Join. Movies: {len(self.movies_buffer)}, Ratings Stats: {len(self.other_buffer)}") # Modified log
            for movie_id, movie_data in list(self.movies_buffer.items()):
                if movie_id in self.other_buffer:
                    # --- Modification Start: Use pre-calculated stats ---
                    try:
                        # Retrieve the pre-calculated sum and count directly
                        ratings_sum, rating_count = self.other_buffer[movie_id]

                        if rating_count > 0:
                            avg_rating = ratings_sum / rating_count
                            movie_data.average_rating = avg_rating
                            logging.debug(f"Joined movie ID {movie_id} using pre-calculated stats. Count={rating_count}, Avg={avg_rating:.2f}")
                        else:
                            # Should not happen if we only store movies with count > 0, but handle defensively
                            movie_data.average_rating = 0.0
                            logging.debug(f"Matched movie ID {movie_id} but count is zero in stats.")

                        processed_movies_for_batch.append(movie_data)
                        joined_ids.add(movie_id)
                    # --- Modification End ---
                    except Exception as e:
                        logging.error(f"Error during ratings join for movie ID {movie_id}: {e}", exc_info=True)
            # Cleanup
            for movie_id in joined_ids:
                if movie_id in self.movies_buffer: del self.movies_buffer[movie_id]
                if movie_id in self.other_buffer: del self.other_buffer[movie_id]
            logging.info(f"Ratings join pass complete. Removed {len(joined_ids)}. Remaining Movies: {len(self.movies_buffer)}, Ratings Stats: {len(self.other_buffer)}") # Modified log

        # Send results if any
        if processed_movies_for_batch:
            self._send_movie_batch(processed_movies_for_batch)

    def _process_credits_join(self):
        """Processes buffered data for CREDITS join, emitting ActorParticipation messages."""
        participations_for_batch = [] # Collect participation records here
        processed_ids = set() # Track which movies/credits were processed in this pass

        with self._lock:
            logging.info(f"Processing Credits Join. Movies: {len(self.movies_buffer)}, Credits: {len(self.other_buffer)}")
            for movie_id, movie_data in list(self.movies_buffer.items()): # Iterate over filtered movies
                if movie_id in self.other_buffer: # Check if corresponding credits exist
                    credits_list = self.other_buffer[movie_id]
                    try:
                        # Assumes only one CreditCSV item per movie ID in other_buffer
                        if credits_list:
                            credit_data = credits_list[0]
                            for cast_member in credit_data.cast:
                                if not cast_member.name or cast_member.id <= 0: continue # Skip if name or ID is missing/invalid

                                # Create an ActorParticipation message
                                participation = files_pb2.ActorParticipation()
                                participation.actor_id = cast_member.id
                                participation.actor_name = cast_member.name
                                participation.movie_id = movie_id # Use the movie ID from the join key

                                participations_for_batch.append(participation)

                            processed_ids.add(movie_id)
                            logging.debug(f"Generated {len(credit_data.cast)} participation records for movie ID {movie_id}")
                        else:
                            logging.debug(f"Matched movie ID {movie_id} but no credits data found.")
                            processed_ids.add(movie_id) # Mark as processed even if no credits

                    except Exception as e:
                        logging.error(f"Error processing credits for movie ID {movie_id}: {e}", exc_info=True)

            # Cleanup processed movies/credits (prevent reprocessing if EOF comes late)
            for movie_id in processed_ids:
                if movie_id in self.movies_buffer: del self.movies_buffer[movie_id]
                if movie_id in self.other_buffer: del self.other_buffer[movie_id]

            logging.info(f"Credits join pass complete. Generated {len(participations_for_batch)} participation records. Processed {len(processed_ids)} movies. Remaining Movies: {len(self.movies_buffer)}, Credits: {len(self.other_buffer)}")

        # Send the collected participation records
        if participations_for_batch:
            self._send_actor_participations_batch(participations_for_batch)

    def _send_movie_batch(self, movie_list):
        """Serializes and sends a batch of MovieCSV objects."""
        if not self.output_producer:
            logging.error("Output producer not initialized.")
            return
        if not movie_list:
            logging.info("No movie batch data to send.")
            return

        try:
            # Use the existing protocol method to send a batch of MovieCSV
            serialized_batch = self.protocol.create_movie_list(movie_list)
            log_msg = f"Sent batch of {len(movie_list)} processed MovieCSV items (joined with {self.other_data_type})."

            if serialized_batch:
                self.output_producer.publish(serialized_batch)
                logging.info(log_msg)
            else:
                 logging.error("Failed to serialize processed MovieCSV batch.")

        except Exception as e:
            logging.error(f"Failed to send processed MovieCSV batch: {e}", exc_info=True)

    def _send_actor_participations_batch(self, participations_list):
        """Serializes and sends a batch of ActorParticipation objects."""
        if not self.output_producer:
            logging.error("Output producer not initialized.")
            return
        if not participations_list:
            logging.info("No actor participation data to send.")
            return
        try:
            # Use the protocol method for the correct batch type
            serialized_batch = self.protocol.create_actor_participations_batch(participations_list)
            log_msg = f"Sent batch of {len(participations_list)} ActorParticipation items."

            if serialized_batch:
                self.output_producer.publish(serialized_batch)
                logging.info(log_msg)
            else:
                logging.error("Failed to serialize ActorParticipation batch.")
        except Exception as e:
            logging.error(f"Failed to send ActorParticipation batch: {e}", exc_info=True)

    def stop(self):
        """Stops the Joiner gracefully."""
        if not self._stop_event.is_set():
            logging.info("Stopping Joiner...")
            self._stop_event.set() # Signal loops/threads to stop if they check

            # Stop consumers and producer (which closes channels/connections)
            consumers = [self.movies_consumer, self.other_consumer, self.control_consumer]
            for consumer in consumers:
                 if consumer: consumer.stop()
            if self.output_producer: self.output_producer.stop()

            logging.info("Joiner Stopped") 