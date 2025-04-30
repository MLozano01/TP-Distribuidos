import pika
import time
import logging


class RabbitMQ:
    def __init__(self, exchange, q_name, key, exc_type):
        self.exchange = exchange
        self.q_name = q_name
        self.key = key
        self.exc_type = exc_type
        self.channel = self.create_channel()


    def create_channel(self):
        """Used to create a channel for the queue."""

        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))

            channel = connection.channel()
            channel.exchange_declare(exchange=self.exchange, exchange_type=self.exc_type, durable=True)

            logging.info(f"Channel created with exchange {self.exchange} of type {self.exc_type}")

            return channel
        except Exception as e:
            logging.error(f"Failed to create channel: {e}")
            if 'connection' in locals():
                connection.close()
                logging.info("Connection closed")
            if 'channel' in locals():
                channel.close()
                logging.info("Channel closed")

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

            logging.info(f"Sent message with routing key: {key_to_use}") # Log the actual key used
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            raise e


    def consume(self, callback):
        """The callback function is defined by the different nodes."""

        try:
            self.channel.queue_declare(queue=self.q_name, durable=True)

            self.channel.queue_bind(exchange=self.exchange, queue=self.q_name, routing_key=self.key)
            self.channel.basic_consume(queue=self.q_name, on_message_callback=callback, auto_ack=True)

            logging.info(f"Waiting for messages in {self.q_name}, with routing_key {self.key}. To exit press CTRL+C")

            self.channel.start_consuming()

        except KeyboardInterrupt:
            logging.info("Exiting...")

        except Exception as e:
            logging.error(f"Failed to consume from {self.q_name}: {e}")
            raise e

    def close_channel(self):
        """Used to close the channel."""
        if self.channel.is_open:
            self.channel.stop_consuming()
            self.channel.close()
            logging.info("Stopped consuming messages")
        else:
            logging.info("Channel is already closed")
        logging.info("Channel closed")
