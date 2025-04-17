import pika 
import logging


def create_channel(exchange, type):
    """Used to create a channel for the queue."""

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        channel = connection.channel()
        channel.exchange_declare(exchange=exchange, exchange_type=type)
        logging.info(f"Channel created with exchange {exchange} of type {type}")
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
    return channel

def create_queue(exchange, q_name, key, channel):
    """Used to create a queue for the channel."""
    try:
        channel.queue_declare(queue=q_name, durable=True)
        channel.queue_bind(exchange=exchange, queue=q_name, routing_key=key)
        logging.info(f"Queue {q_name} created and bound to exchange {exchange} with routing key {key}")
    except Exception as e:
        logging.error(f"Failed to create queue {q_name}: {e}")
        raise e
    finally:
        close_channel(channel)

    return channel


def publish(channel, name, key, topic, message):

    """Used to publish messages to the queue."""

    try:
        channel.basic_publish(exchange=topic,
                            routing_key=key,
                            body=message,
                            properties=pika.BasicProperties(
                                delivery_mode=2,
                            ))

        logging.info(f"Sent {message} to {name}")
    except Exception as e:
        logging.error(f"Failed to send {message} to {name}: {e}")
        raise e
    finally:
        close_channel(channel)


def consume(channel, queue_name, topic, callback):
    """The callback function is defined by the different nodes."""

    try:
        result = channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue

        channel.queue_bind(exchange=topic, queue=queue_name, routing_key=topic)
        channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=True)

        logging.info(f"Waiting for messages in {queue_name}. To exit press CTRL+C")

        channel.start_consuming()

    except KeyboardInterrupt:
        logging.info("Exiting...")

    except Exception as e:
        logging.error(f"Failed to consume from {queue_name}: {e}")
        raise e
    
    finally:
        channel.stop_consuming()
        close_channel(channel)

def close_channel(channel):
    """Used to close the channel."""
    channel.connection.close()
    channel.close()
    logging.info("Channel closed")
