from multiprocessing import Process
import socket
import logging
from protocol.protocol import Protocol
from protocol.rabbit_protocol import RabbitMQ
from protocol.rabbit_wrapper import RabbitMQProducer, RabbitMQConsumer
from protocol.utils.socket_utils import recvall

class Client:
    def __init__(self, client_sock, client_id):
        self.socket = client_sock
        self.client_id = client_id
        self.protocol = Protocol()
        self.data_controller = None
        self.result_controller = None
        self.forward_queue = None
        self.result_queue = None

        self.columns_needed = {
            'movies': ["id", "title", "genres", "release_date", "overview", 
                      "production_countries", "spoken_languages", "budget", "revenue"],
            'ratings': ["movieId", "rating", "timestamp"],
            'credits': ["id", "cast"]
        }

    def run(self):
        self.data_controller = Process(target=self.handle_connection, args=[self.socket])
        self.data_controller.start()

        self.result_controller = Process(target=self.return_results, args=[self.socket])
        self.result_controller.start()

        self.data_controller.join()
        self.result_controller.join()

    def stop(self):
        if self.forward_queue:
            self.forward_queue.stop()
        if self.result_queue:
            self.result_queue.stop()
        if self.control_publisher:
            self.control_publisher.stop()
        
        if self.data_controller.is_alive():
            self.data_controller.terminate()
        if self.result_controller.is_alive():
            self.result_controller.terminate()

    def handle_connection(self, conn: socket.socket):
        # Initialize the forward queue for data messages
        self.forward_queue = RabbitMQProducer(
            host='rabbitmq',
            exchange="server_to_data_controller",
            exchange_type="direct",
            routing_key="forward"
        )
        
        closed_socket = False
        while not closed_socket:
            read_amount = self.protocol.define_initial_buffer_size()
            buffer = bytearray()
            closed_socket = recvall(conn, buffer, read_amount)
            if closed_socket:
                return
            read_amount = self.protocol.define_buffer_size(buffer)
            closed_socket = recvall(conn, buffer, read_amount)
            if closed_socket:
                return
            
            # JUST FOR DEBUGGING
            type, msg = self.protocol.decode_client_msg(buffer, self.columns_needed)
            logging.info(f"Received message from client of type: {type.name}")
            self._forward_to_data_controller(buffer)

    def _forward_to_data_controller(self, message):
        try:
            logging.info(f"Forwarding message to data controller")
            self.forward_queue.publish(message)
        except Exception as e:
            logging.error(f"Failed to forward message to data controller: {e}")

    def return_results(self, conn: socket.socket):
        self.result_queue = RabbitMQ('exchange_snd_results', 'result', 'results', 'direct')
        self.result_queue.consume(self.result_controller_func)

    def result_controller_func(self, ch, method, properties, body):
        try:
            msg = self.protocol.create_client_result(body)
            self.socket.sendall(msg)
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return