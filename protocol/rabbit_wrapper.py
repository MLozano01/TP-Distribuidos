import pika
import time
import logging

class RabbitMQBase:
    """Base class handling RabbitMQ connection and channel setup with retries."""
    def __init__(self, host):
        self.host = host
        self._connection = None
        self._channel = None
        self._connect_with_retry()

    def _connect_with_retry(self, max_retries=5, delay=5):
        """Establishes connection with RabbitMQ using retries."""
        retries = 0
        while retries < max_retries:
            try:
                # Ensure previous connection/channel are closed if retry occurs
                if self._channel and self._channel.is_open:
                    self._channel.close()
                if self._connection and self._connection.is_open:
                    self._connection.close()

                self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
                self._channel = self._connection.channel()
                logging.info(f"Successfully connected to RabbitMQ at {self.host}")
                return # Success
            except pika.exceptions.AMQPConnectionError as e:
                retries += 1
                logging.warning(f"Connection attempt {retries}/{max_retries} failed: {e}. Retrying in {delay}s...")
                if retries >= max_retries:
                    logging.error("Max connection retries reached. Could not connect to RabbitMQ.")
                    raise # Reraise the last exception
                time.sleep(delay)
            except Exception as e: # Catch other potential errors during connection setup
                logging.error(f"An unexpected error occurred during connection attempt: {e}")
                raise # Reraise other critical errors

    @property
    def channel(self):
        """Ensures channel is active, reconnects if necessary."""
        # Check connection status first
        if not self._connection or self._connection.is_closed:
             logging.warning("Connection is closed. Attempting to reconnect...")
             self._connect_with_retry()
        # Then check channel status
        elif not self._channel or self._channel.is_closed:
            logging.warning("Channel is closed, but connection seems open. Recreating channel...")
            try:
                 # Attempt to recreate channel on existing connection
                 self._channel = self._connection.channel()
                 logging.info("Channel successfully recreated.")
            except Exception as e:
                 # If recreating channel fails, maybe connection is bad too
                 logging.error(f"Failed to recreate channel: {e}. Attempting full reconnect...")
                 self._connect_with_retry() # Fallback to full reconnect

        return self._channel

    def stop(self):
        """Closes the channel and connection gracefully."""
        closed_channel = False
        closed_connection = False
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
                closed_channel = True
            if self._connection and self._connection.is_open:
                self._connection.close()
                closed_connection = True

            if closed_channel: logging.info("RabbitMQ channel closed.")
            if closed_connection: logging.info("RabbitMQ connection closed.")

        except Exception as e:
            # Log error but don't prevent setting resources to None
            logging.error(f"Error closing RabbitMQ resources: {e}", exc_info=True)
        finally:
            # Ensure these are set to None even if closing failed
            self._channel = None
            self._connection = None


class RabbitMQConsumer(RabbitMQBase):
    """Generic RabbitMQ Consumer with non-blocking setup and exclusive queue support."""
    def __init__(self, host, exchange, exchange_type, routing_key, queue_name=None, durable=True, exclusive=False, auto_delete=False):
        super().__init__(host)
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key
        # Store the requested name; actual name might be generated
        self.requested_queue_name = queue_name
        self.actual_queue_name = None
        self.consumer_tag = None
        # Store queue properties
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete

        # Handle case where exclusive queue is requested without dynamic name
        if self.exclusive and self.requested_queue_name:
            logging.warning(f"Queue '{self.requested_queue_name}' requested as exclusive. Setting auto_delete=True and durable=False.")
            self.auto_delete = True
            self.durable = False
        elif self.requested_queue_name is None:
            self.exclusive = True
            self.auto_delete = True
            self.durable = False

        self._setup_consumer()

    def _setup_consumer(self):
        """Declares exchange, queue, and binds them."""
        try:
            ch = self.channel # Use property to get/ensure active channel
            # Declare exchange
            ch.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type, durable=self.durable)
            logging.info(f"Exchange '{self.exchange}' ({self.exchange_type}) declared.")

            # Declare queue
            q_name_to_declare = self.requested_queue_name if self.requested_queue_name is not None else ''
            result = ch.queue_declare(
                queue=q_name_to_declare,
                exclusive=self.exclusive,
                auto_delete=self.auto_delete,
                durable=self.durable
            )
            self.actual_queue_name = result.method.queue # Store the actual name
            logging.info(f"Declared queue: {self.actual_queue_name} (Durable: {self.durable}, Exclusive: {self.exclusive}, AutoDelete: {self.auto_delete})")

            # Bind queue
            ch.queue_bind(exchange=self.exchange, queue=self.actual_queue_name, routing_key=self.routing_key)
            logging.info(f"Queue '{self.actual_queue_name}' bound to exchange '{self.exchange}' with key '{self.routing_key}'")

        except Exception as e:
            q_name_log = self.requested_queue_name or "(dynamic exclusive)"
            logging.error(f"Error setting up consumer for queue {q_name_log}: {e}", exc_info=True)
            self.stop() # Clean up connection if setup fails
            raise

    @property
    def queue_name(self):
        """Returns the actual name of the queue being consumed (could be generated)."""
        return self.actual_queue_name

    def consume(self, callback, auto_ack=True):
        """Sets up the consumer callback but doesn't start the blocking loop."""
        if self.consumer_tag is not None:
             logging.warning(f"Consumer already setup for queue '{self.actual_queue_name}'. Ignoring duplicate consume() call.")
             return
        try:
            ch = self.channel # Use property to get/ensure active channel
            self.consumer_tag = ch.basic_consume(
                queue=self.actual_queue_name,
                on_message_callback=callback,
                auto_ack=auto_ack
            )
            logging.info(f"Consumer set up for queue '{self.actual_queue_name}' with tag '{self.consumer_tag}'. Ready to start consuming.")
        except Exception as e:
            logging.error(f"Error setting up basic_consume for queue {self.actual_queue_name}: {e}", exc_info=True)
            self.stop()
            raise

    def start_consuming(self):
        """Starts the blocking consumption loop. Should be run in a dedicated thread."""
        if self.consumer_tag is None:
            logging.error(f"Consumer not set up for queue {self.actual_queue_name}. Call consume() first.")
            raise RuntimeError(f"Consumer not set up for queue {self.actual_queue_name}")

        logging.info(f"Starting blocking consumer loop for queue '{self.actual_queue_name}'...")
        try:
            ch = self.channel
            ch.start_consuming()
        except KeyboardInterrupt:
             logging.info(f"KeyboardInterrupt received, stopping consumer loop for {self.actual_queue_name}.")
        except Exception as e:
            logging.error(f"Error during start_consuming for queue {self.actual_queue_name}: {e}", exc_info=True)
            raise # Reraise to signal failure to the calling thread
        finally:
            logging.info(f"Consumer loop terminated for queue '{self.actual_queue_name}'.")

    def stop(self):
        """Stops consuming and closes resources."""
        if self._channel and self._channel.is_open and self.consumer_tag:
            try:
                logging.info(f"Attempting to cancel consumer for queue '{self.actual_queue_name}' (tag: {self.consumer_tag})")
                self._channel.basic_cancel(self.consumer_tag)
                logging.info(f"Successfully cancelled consumer for queue '{self.actual_queue_name}'.")
            except Exception as e:
                logging.error(f"Error cancelling consumer for {self.actual_queue_name}: {e}")
            finally:
                 self.consumer_tag = None
        super().stop()


class RabbitMQProducer(RabbitMQBase):
    """Generic RabbitMQ Producer."""
    def __init__(self, host, exchange, exchange_type, routing_key="", declare_exchange=True):
        # routing_key is the default, can be overridden in publish
        # queue_name is removed, producer typically doesn't declare queues
        super().__init__(host)
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.default_routing_key = routing_key
        if declare_exchange:
            self._setup_producer()

    def _setup_producer(self):
        """Declares exchange."""
        try:
            ch = self.channel # Use property to get/ensure active channel
            # Declare exchange (idempotent)
            ch.exchange_declare(exchange=self.exchange, exchange_type=self.exchange_type, durable=True)
            logging.info(f"Exchange '{self.exchange}' ({self.exchange_type}) declared for producer.")
        except Exception as e:
            logging.error(f"Error setting up producer for exchange {self.exchange}: {e}", exc_info=True)
            self.stop() # Clean up connection if setup fails
            raise

    def publish(self, message, routing_key=None, persistent=True):
        """Publishes a message to the configured exchange."""
        pub_routing_key = routing_key if routing_key is not None else self.default_routing_key
        delivery_mode = 2 if persistent else 1

        try:
            ch = self.channel # Use property to get active channel
            ch.basic_publish(
                exchange=self.exchange,
                routing_key=pub_routing_key,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=delivery_mode,
                )
            )
            logging.debug(f"Published message to exchange '{self.exchange}' with key '{pub_routing_key}'")
        except Exception as e:
            logging.error(f"Failed to publish message to exchange {self.exchange}: {e}", exc_info=True)
            raise

    # stop() method is inherited from RabbitMQBase 