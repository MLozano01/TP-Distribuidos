
import socket
import logging
import time
from protocol.rabbit_protocol import RabbitMQ

class Server:
    def __init__(self, port, listen_backlog):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', port))
        self.socket.listen(listen_backlog)
        self.running = True


    def run(self):

        self.test_queue()

        while self.running:
            try:
                conn, addr = self.socket.accept()
                logging.info(f"Connection accepted from {addr}")
                # Handle the connection
                # self.handle_connection(conn)
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

        
    def close_socket(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        logging.info("Socket Closed")

    def end_server(self):
        self.running = False
        self.close_socket()
        logging.info("Server Stopped")