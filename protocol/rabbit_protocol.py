import pika
import time
import logging

rabbit_logger = logging.getLogger("RabbitMQ")

class RabbitMQ:
    def __init__(self, exchange, q_name, key, exc_type, auto_ack=False, prefetch_count=None):
        self.exchange = exchange
        self.q_name = q_name
        self.key = key
        self.exc_type = exc_type
        self.auto_ack = auto_ack
        self.prefetch_count = prefetch_count
        self.channel = self.create_channel()
        self.callback_func = None


    def create_channel(self):
        """Used to create a channel for the queue."""

        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=500))

            channel = connection.channel()
            channel.basic_qos(prefetch_count=1)
            channel.exchange_declare(exchange=self.exchange, exchange_type=self.exc_type, durable=True)

            rabbit_logger.debug(f"Channel created with exchange {self.exchange} of type {self.exc_type}")

            return channel
        except Exception as e:
            rabbit_logger.error(f"Failed to create channel: {e}")
            if 'connection' in locals():
                connection.close()
                rabbit_logger.info("Connection closed")
            if 'channel' in locals():
                channel.close()
                rabbit_logger.info("Channel closed")

    def publish(self, message, routing_key=None):
        """Used to publish messages to the queue.
        Allows overriding the default routing key.
        """
        # Determine the routing key to use
        key_to_use = routing_key if routing_key is not None else self.key

        try:
            self.channel.basic_publish(exchange=self.exchange,
                                routing_key=key_to_use, # Use the determined key
                                body=message,
                                properties=pika.BasicProperties(
                                    delivery_mode=2,
                                ))

            rabbit_logger.info(f"Sent message with routing key: {key_to_use}") # Log the actual key used
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            raise e


    def consume(self, callback_func, routing_key=None):
        """The callback function is defined by the different nodes."""

        try:
            key_to_use = routing_key if routing_key is not None else self.key

            self.channel.queue_declare(queue=self.q_name, durable=True)
            if self.prefetch_count is not None:
                self.channel.basic_qos(prefetch_count=self.prefetch_count)
            self.channel.queue_bind(exchange=self.exchange, queue=self.q_name, routing_key=key_to_use)

            self.callback_func = callback_func
            self.channel.basic_consume(queue=self.q_name, on_message_callback=self.callback, auto_ack=self.auto_ack)

            rabbit_logger.debug(f"Waiting for messages in {self.q_name}, with routing_key {self.key}. To exit press CTRL+C")

            self.channel.start_consuming()

        except KeyboardInterrupt:
            rabbit_logger.info("Exiting...")

        except Exception as e:
            rabbit_logger.error(f"Failed to consume from {self.q_name}: {e}")
            raise e
        
    def callback(self, ch, method, properties, body):
        try:
            self.callback_func(ch, method,properties,body)
            self.channel.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logging.error(f"Failed to process message: {e}")
            self.channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    def close_channel(self):
        """Used to close the channel."""
        if self.channel.is_open:
            self.channel.stop_consuming()
            self.channel.close()
            rabbit_logger.info("Stopped consuming messages")
        else:
            rabbit_logger.info("Channel is already closed")
        rabbit_logger.info("Channel closed")
