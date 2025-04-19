
import socket
import logging
import time
import json
from protocol.protocol import Protocol
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.socket_utils import recvall

class Server:
    def __init__(self, port, listen_backlog):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', port))
        self.socket.listen(listen_backlog)
        self.running = True
        self.protocol = Protocol()
        self.queue = RabbitMQ("exchange", "name", "key", "direct")

    def run(self):

        # self.test_queue()

        while self.running:
            try:
                conn, addr = self.socket.accept()
                logging.info(f"Connection accepted from {addr}")
                # Handle the connection
                self.handle_connection(conn)
            except Exception as e:
                logging.error(f"Error accepting connection: {e}")
                break

    def accept_connection(self):
        conn, addr = self.socket.accept()
        logging.info(f"Connection accepted from {addr}")
        return conn
    
    def test_queue(self):
        queue = RabbitMQ("exchange", "name", "key", "direct")
        # queue.create_queue()
        message = "Hello, World!"
        time.sleep(5)
        queue.publish(message)


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
            
            msg = self.protocol.decode_msg(buffer)
            # print(msg)

            json_msg = self.protocol.encode_movies_to_json(msg)

            self.queue.publish(json_msg)
            time.sleep(10)


    def start_queue(self):
        self.queue = RabbitMQ("exchange", "name", "key", "direct")
        
    def close_socket(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        logging.info("Socket Closed")

    def end_server(self):
        self.running = False
        self.close_socket()
        logging.info("Server Stopped")