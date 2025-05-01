from multiprocessing import Process
import socket
import logging
from protocol.protocol import Protocol
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.socket_utils import recvall

class Client:
    def __init__(self, client_sock):
        self.socket = client_sock
        self.protocol = Protocol()
        self.data_controller = None
        self.result_controller = None
        self.forward_queue = RabbitMQ("server_to_data_controller", "forward", "forward_queue", "direct")
        self.result_queue = RabbitMQ('exchange_snd_results', 'result', 'results', 'direct')

    def run(self):
        self.data_controller = Process(target=self.handle_connection, args=[self.socket])
        self.data_controller.start()

        self.result_controller = Process(target=self.return_results, args=[self.socket])
        self.result_controller.start()

        self.data_controller.join()
        self.result_controller.join()

    def stop(self):
        self.forward_queue.stop()
        self.result_queue.stop()
        
        if self.data_controller.is_alive():
            self.data_controller.terminate()
        if self.result_controller.is_alive():
            self.result_controller.terminate()

    def handle_connection(self, conn: socket.socket):
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
            
            # Forward the raw message to data controller
            self._forward_to_data_controller(buffer)

    def _forward_to_data_controller(self, message):
        try:
            self.forward_queue.publish(message)
        except Exception as e:
            logging.error(f"Failed to forward message to data controller: {e}")

    def return_results(self, conn: socket.socket):
        self.result_queue.consume(self.result_controller_func)

    def result_controller_func(self, ch, method, properties, body):
        try:
            logging.info(f"got result: {body}")
            msg = self.protocol.create_client_result(body)
            logging.info(f"sending message: {msg}")
            self.socket.sendall(msg)
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}")
            return
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return