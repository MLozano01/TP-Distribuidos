import pika 
import logging


class RabbitMQ:
    def __init__(self, exchange, q_name, key, type):
        self.exchange = exchange
        self.q_name = q_name
        self.key = key
        self.type = type
        self.channel = None



    def create_channel(self):
        """Used to create a channel for the queue."""

        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
            channel = connection.channel()
            channel.exchange_declare(exchange=self.exchange, exchange_type=type)
            logging.info(f"Channel created with exchange {self.exchange} of type {type}")
        except Exception as e:
            logging.error(f"Failed to create channel: {e}")
            raise e
        finally:
            if 'connection' in locals():
                connection.close()
                logging.info("Connection closed")
            if 'channel' in locals():
                channel.close()
                logging.info("Channel closed")
        self.channel = channel

    def create_queue(self):
        """Used to create a queue for the channel."""
        try:
            self.channel.queue_declare(queue=self.q_name, durable=True)
            self.channel.queue_bind(exchange=self.exchange, queue=self.q_name, routing_key=self.key)
            logging.info(f"Queue {self.q_name} created and bound to exchange {self.exchange} with routing key {self.key}")
        except Exception as e:
            logging.error(f"Failed to create queue {self.q_name}: {e}")
            raise e
        finally:
            self.close_channel(self.channel)



    def publish(self):

        """Used to publish messages to the queue."""

        try:
            self.channel.basic_publish(exchange=self.topic,
                                routing_key=self.key,
                                body=self.message,
                                properties=pika.BasicProperties(
                                    delivery_mode=2,
                                ))

            logging.info(f"Sent {self.message} to {self.name}")
        except Exception as e:
            logging.error(f"Failed to send {self.message} to {self.name}: {e}")
            raise e
        finally:
            self.close_channel(self.channel)


    def consume(self, callback):
        """The callback function is defined by the different nodes."""

        try:
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue

            self.channel.queue_bind(exchange=self.topic, queue=queue_name, routing_key=self.topic)
            self.channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

            logging.info(f"Waiting for messages in {queue_name}. To exit press CTRL+C")

            self.channel.start_consuming()

        except KeyboardInterrupt:
            logging.info("Exiting...")

        except Exception as e:
            logging.error(f"Failed to consume from {queue_name}: {e}")
            raise e
        
        finally:
            self.channel.stop_consuming()
            self.close_channel(self.channel)

    def close_channel(self):
        """Used to close the channel."""
        self.channel.connection.close()
        self.channel.close()
        logging.info("Channel closed")
