import logging
import signal
import threading
import pika
from protocol.protocol import Protocol, FileType, MOVIES_FILE_CODE, RATINGS_FILE_CODE, CREDITS_FILE_CODE, CODE_LENGTH, INT_LENGTH # Import constants
from protocol.rabbit_wrapper import RabbitMQConsumer, RabbitMQProducer
from protocol import files_pb2 # Import files_pb2 to access new message types

# Define batch size for outgoing messages
BATCH_SIZE = 60000
logging.getLogger("pika").setLevel(logging.ERROR)

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
        self.movies_buffer = {}  # {client_id: {movie_id: movie_data (MovieCSV object)}}
        self.other_buffer = {}   # {client_id: {movie_id: [other_data_item_1, ...]}}
        # Track EOF signals per client
        self.movies_eof_received = set()  # Set of client_ids that have received movies EOF
        self.other_eof_received = set()   # Set of client_ids that have received other EOF
        self._lock = threading.Lock() # To protect shared buffers and EOF flags

        # RabbitMQ Connections
        self.movies_consumer = None
        self.other_consumer = None
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

            # --- Producer ---
            self.output_producer = RabbitMQProducer(
                host=host,
                exchange=self.config['exchange_output'],
                exchange_type='direct', # Or topic, depending on downstream needs
                routing_key=self.config['routing_key_output'] # Default routing key
                # Producer declares exchange but not queue by default
            )

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
            if not all([self.movies_consumer, self.other_consumer, 
                        self.output_producer]):
                 logging.error("Not all required RabbitMQ connections were initialized. Aborting start.")
                 return

            # Setup consumer callbacks (before starting threads)
            # Note: auto_ack=True used as requested
            self.movies_consumer.consume(self._process_movie_message, auto_ack=True)
            self.other_consumer.consume(self._process_other_message, auto_ack=True)

            # Start consumers in separate threads by calling start_consuming
            threads = []
            threads.append(threading.Thread(target=self.movies_consumer.start_consuming, daemon=True))
            threads.append(threading.Thread(target=self.other_consumer.start_consuming, daemon=True))

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

            if not movies_msg or not movies_msg.client_id:
                logging.warning("Received empty or invalid movie message.")
                return

            client_id = movies_msg.client_id
            # Handle finished signal
            if movies_msg.finished:
                self._handle_movies_finished_signal(client_id)
                return
            
            if not movies_msg.movies:
                logging.warning("Received empty movie batch.")
                return

            with self._lock:
                if client_id not in self.movies_buffer:
                    self.movies_buffer[client_id] = {}
                
                for movie in movies_msg.movies:
                    self.movies_buffer[client_id][movie.id] = movie

        except Exception as e:
            logging.error(f"Error processing movie message: {e}", exc_info=True)

    def _process_other_message(self, ch, method, properties, body):
        """Callback for processing other data (ratings/credits)."""
        if self._stop_event.is_set(): return
        try:
            other_msg = None
            data_list = []
            movie_id_field = 'movieId' # Default for ratings

            is_ratings = self.other_data_type == "RATINGS" # Check type once

            # Decode message
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

            # Check if this is a finished signal
            if other_msg and other_msg.finished:
                client_id = other_msg.client_id
                should_trigger_processing = False
                with self._lock:
                    if client_id not in self.other_eof_received:
                        self.other_eof_received.add(client_id)
                        logging.info(f"Processed {self.other_data_type} finished signal for client {client_id}.")
                        if client_id in self.movies_eof_received:
                            should_trigger_processing = True
                    else:
                        logging.info(f"Duplicate {self.other_data_type} finished signal for client {client_id}.")
                
                if should_trigger_processing:
                    self._trigger_final_processing(client_id)
                return

            # Original data processing logic
            if not other_msg or not data_list:
                 logging.warning(f"Received empty or invalid {self.other_data_type} batch.")
                 return

            with self._lock: # Acquire lock to modify shared buffer
                client_id = other_msg.client_id
                if client_id not in self.other_buffer:
                    self.other_buffer[client_id] = {}

                for item in data_list:
                    movie_id = getattr(item, movie_id_field, None)
                    if movie_id is None:
                         logging.warning(f"Could not extract movie ID using field '{movie_id_field}' from {self.other_data_type} item.")
                         continue

                    if is_ratings:
                        # Get current sum and count, default to (0.0, 0)
                        current_sum, current_count = self.other_buffer[client_id].get(movie_id, (0.0, 0))
                        # Update sum and count
                        new_sum = current_sum + item.rating # Assuming item is RatingCSV
                        new_count = current_count + 1
                        # Store the updated tuple
                        self.other_buffer[client_id][movie_id] = (new_sum, new_count)
                    else: # Handle Credits (buffering the object)
                        if movie_id not in self.other_buffer[client_id]:
                            self.other_buffer[client_id][movie_id] = []
                        self.other_buffer[client_id][movie_id].append(item)

        except Exception as e:
            logging.error(f"Error processing {self.other_data_type} message: {e}", exc_info=True)

    def _process_ratings_join(self, client_id):
        """Processes buffered data for RATINGS join for a specific client, modifying MovieCSV."""
        processed_movies_batch = [] # Accumulate movies for the current batch
        joined_ids = set()
        initial_movie_count = 0
        initial_ratings_count = 0
        total_movies_sent = 0

        with self._lock:
            if client_id not in self.movies_buffer or client_id not in self.other_buffer:
                logging.warning(f"No data found for client {client_id}")
                return

            initial_movie_count = len(self.movies_buffer[client_id])
            initial_ratings_count = len(self.other_buffer[client_id])
            logging.info(f"Processing Ratings Join for client {client_id}. Movies: {initial_movie_count}, Ratings Stats: {initial_ratings_count}")

            # Iterate over a copy of movie IDs to allow safe deletion
            movie_ids_to_check = list(self.movies_buffer[client_id].keys())

            for movie_id in movie_ids_to_check:
                # Check for match only if movie_id is still in buffer
                if movie_id not in self.movies_buffer[client_id]:
                    continue
                
                if movie_id in self.other_buffer[client_id]:
                    movie_data = self.movies_buffer[client_id][movie_id]
                    try:
                        # Retrieve the pre-calculated sum and count directly
                        ratings_sum, rating_count = self.other_buffer[client_id][movie_id]

                        if rating_count > 0:
                            avg_rating = ratings_sum / rating_count
                            movie_data.average_rating = avg_rating
                            logging.debug(f"Joined movie ID {movie_id} using pre-calculated stats. Count={rating_count}, Avg={avg_rating:.2f}")
                        else:
                            # Should not happen if we only store movies with count > 0, but handle defensively
                            movie_data.average_rating = 0.0
                            logging.debug(f"Matched movie ID {movie_id} but count is zero in stats.")

                        processed_movies_batch.append(movie_data)
                        joined_ids.add(movie_id)

                        # Check if batch is full
                        if len(processed_movies_batch) >= BATCH_SIZE:
                            logging.info(f"Movie batch size reached ({len(processed_movies_batch)}). Sending batch...")
                            # Send batch (needs _send_movie_batch helper which handles producer check)
                            # Sending here holds the lock longer, but simplifies batch logic.
                            self._send_movie_batch(processed_movies_batch, client_id)
                            total_movies_sent += len(processed_movies_batch)
                            processed_movies_batch.clear() # Reset for next batch

                    except Exception as e:
                        logging.error(f"Error during ratings join for movie ID {movie_id}: {e}", exc_info=True)

            # --- Batching: Send any remaining movies after the loop --- 
            if processed_movies_batch:
                logging.info(f"Sending final batch of {len(processed_movies_batch)} processed movies...")
                self._send_movie_batch(processed_movies_batch, client_id)
                total_movies_sent += len(processed_movies_batch)
                processed_movies_batch.clear()

            # Send finished message after all batches   
            logging.info(f"Sending finished message for client {client_id} after processing {total_movies_sent} movies")
            finished_msg = self.protocol.create_movie_finished_msg(client_id)
            self.output_producer.publish(finished_msg)

            # Cleanup processed movies/ratings for this client
            for mid in joined_ids:
                if mid in self.movies_buffer[client_id]: del self.movies_buffer[client_id][mid]
                if mid in self.other_buffer[client_id]: del self.other_buffer[client_id][mid]

            final_movie_count = len(self.movies_buffer[client_id])
            final_ratings_count = len(self.other_buffer[client_id])
            # Use total_movies_sent for the final log
            logging.info(f"Ratings join pass complete for client {client_id}. Sent {total_movies_sent} processed MovieCSV records in batches. Processed {len(joined_ids)} movie IDs. Initial state: ({initial_movie_count} M, {initial_ratings_count} R). Final state: ({final_movie_count} M, {final_ratings_count} R).")

    def _process_credits_join(self, client_id):
        """Processes buffered data for CREDITS join for a specific client, emitting ActorParticipation messages in batches."""
        participations_batch = [] # Accumulate participations for the current batch
        processed_movie_ids = set()
        initial_movie_count = 0
        initial_credits_count = 0
        total_participations_sent = 0

        with self._lock:
            if client_id not in self.movies_buffer or client_id not in self.other_buffer:
                logging.warning(f"No data found for client {client_id}")
                return

            initial_movie_count = len(self.movies_buffer[client_id])
            initial_credits_count = len(self.other_buffer[client_id])
            logging.info(f"Starting Credits Join for client {client_id}. Movies: {initial_movie_count}, Credits: {initial_credits_count}")

            # Iterate over a copy of movie IDs to allow safe deletion
            movie_ids_to_check = list(self.movies_buffer[client_id].keys())

            for movie_id in movie_ids_to_check:
                # Check for match only if movie_id is still in buffer
                if movie_id not in self.movies_buffer[client_id]:
                    continue

                if movie_id in self.other_buffer[client_id]:
                    movie_data = self.movies_buffer[client_id][movie_id]
                    credit_csv = self.other_buffer[client_id][movie_id][0]
                    try:
                        if credit_csv:
                            # Assume only one CreditCSV item per movie ID
                            movie_participations, skipped = self._create_participations_for_movie(movie_id, credit_csv)
                            if movie_participations:
                                participations_batch.extend(movie_participations)
                                # Check if batch is full
                                if len(participations_batch) >= BATCH_SIZE:
                                    logging.info(f"Participation batch size reached ({len(participations_batch)}). Sending batch...")
                                    self._send_actor_participations_batch(participations_batch, client_id)
                                    total_participations_sent += len(participations_batch)
                                    participations_batch.clear()

                            logging.debug(f"Processed movie {movie_id}: Found {len(credit_csv.cast)} cast, skipped {skipped}, added {len(movie_participations)} participations.")
                            processed_movie_ids.add(movie_id)
                        else:
                            logging.warning(f"Matched movie ID {movie_id} but the credits list in buffer was empty. Marking as processed.")
                            processed_movie_ids.add(movie_id)

                    except Exception as e:
                        logging.error(f"Error processing credits join for movie ID {movie_id}: {e}", exc_info=True)

            # Send any remaining participations for this client
            if participations_batch:
                logging.info(f"Sending final batch of {len(participations_batch)} participations for client {client_id}...")
                self._send_actor_participations_batch(participations_batch, client_id)
                total_participations_sent += len(participations_batch)
                participations_batch.clear()

            # Send finished message after all batches
            logging.info(f"Sending finished message for client {client_id} after processing {total_participations_sent} participations")
            finished_msg = self.protocol.create_actor_participations_finished_msg(client_id)
            self.output_producer.publish(finished_msg)

            # Cleanup processed movies/credits for this client
            for mid in processed_movie_ids:
                if mid in self.movies_buffer[client_id]: del self.movies_buffer[client_id][mid]
                if mid in self.other_buffer[client_id]: del self.other_buffer[client_id][mid]

            final_movie_count = len(self.movies_buffer[client_id])
            final_credits_count = len(self.other_buffer[client_id])
            # Use total_participations_sent for the final log
            logging.info(f"Credits join pass complete for client {client_id}. Generated and sent {total_participations_sent} participation records in batches. Processed {len(processed_movie_ids)} movie IDs. Initial state: ({initial_movie_count} M, {initial_credits_count} C). Final state: ({final_movie_count} M, {final_credits_count} C).")

    def _create_participations_for_movie(self, movie_id, credit_csv):
        """Helper function to create ActorParticipation messages for a single movie's credits."""
        participations = []
        cast_skipped_count = 0
        if not credit_csv or not credit_csv.cast:
            logging.warning(f"No cast data found for movie {movie_id} within provided credit_data.")
            return participations, cast_skipped_count

        logging.debug(f"Creating participations (name-based) for movie {movie_id}. Found {len(credit_csv.cast)} cast members.")
        for i, cast_member in enumerate(credit_csv.cast):
            # Log only name now
            logging.debug(f"  Checking cast member {i+1}/{len(credit_csv.cast)}: Name='{cast_member.name}'")
            # Only check for name, ignore ID
            if not cast_member.name:
                logging.debug(f"    -> Skipping cast member for movie {movie_id}: Name='{cast_member.name}' (Empty Name)")
                cast_skipped_count += 1
                continue # Skip if name is missing

            # Create an ActorParticipation message (without actor_id)
            participation = files_pb2.ActorParticipation()
            participation.actor_name = cast_member.name
            participation.movie_id = movie_id # Use the movie ID from the join key
            participations.append(participation)
            logging.debug(f"    -> Added participation for actor name '{cast_member.name}'")

        logging.debug(f"Finished creating participations for movie {movie_id}. Skipped {cast_skipped_count}. Added {len(participations)} participations.")
        return participations, cast_skipped_count

    def _send_movie_batch(self, movie_list, client_id):
        """Serializes and sends a batch of MovieCSV objects."""
        if not self.output_producer:
            logging.error("Output producer not initialized.")
            return
        if not movie_list:
            logging.info("No movie batch data to send.")
            return

        try:
            # Create batch with client_id
            batch = files_pb2.MoviesCSV()
            batch.movies.extend(movie_list)
            batch.client_id = client_id
            
            # Use the existing protocol method to send a batch of MovieCSV
            serialized_batch = batch.SerializeToString()
            log_msg = f"Sent batch of {len(movie_list)} processed MovieCSV items (joined with {self.other_data_type}) for client {client_id}."

            if serialized_batch:
                self.output_producer.publish(serialized_batch)
                logging.info(log_msg)
            else:
                 logging.error("Failed to serialize processed MovieCSV batch.")

        except Exception as e:
            logging.error(f"Failed to send processed MovieCSV batch: {e}", exc_info=True)

    def _send_actor_participations_batch(self, participations_list, client_id):
        """Serializes and sends a batch of ActorParticipation objects."""
        if not self.output_producer:
            logging.error("Output producer not initialized.")
            return
        if not participations_list:
            logging.info("No actor participation data to send.")
            return
        try:
            serialized_batch = self.protocol.create_actor_participations_batch(participations_list, client_id)
            log_msg = f"Sent batch of {len(participations_list)} ActorParticipation items for client {client_id}."

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
            consumers = [self.movies_consumer, self.other_consumer, 
                         self.movies_control_consumer]
            for consumer in consumers:
                 if consumer: consumer.stop()
            if self.output_producer: self.output_producer.stop()

            logging.info("Joiner Stopped")

    def _handle_movies_finished_signal(self, client_id):
        """Helper method to handle movies finished signal."""
        should_trigger_processing = False
        with self._lock:
            if client_id not in self.movies_eof_received:
                self.movies_eof_received.add(client_id)
                logging.info(f"Processed MOVIES finished signal for client {client_id}.")
                if client_id in self.other_eof_received:
                    should_trigger_processing = True
            else:
                logging.info(f"Duplicate MOVIES finished signal for client {client_id}.")

        if should_trigger_processing:
            logging.info(f"Both EOF signals received for client {client_id}. Triggering final processing.")
            self._trigger_final_processing(client_id)


    def _trigger_final_processing(self, client_id):
        if self.other_data_type == "RATINGS":
            self._process_ratings_join(client_id)
        elif self.other_data_type == "CREDITS":
            self._process_credits_join(client_id)
        else:
            logging.error(f"No final processing logic defined for type: {self.other_data_type}") 