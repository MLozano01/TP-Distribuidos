
import socket
import logging
from common.client import Client

class Server:
    def __init__(self, port, listen_backlog):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', port))
        self.socket.listen(listen_backlog)
        self.running = True
        self.clients = []

    def run(self):

        while self.running:
            try:
                conn, addr = self.socket.accept()
                logging.info(f"Connection accepted from {addr}")
                
                client = Client(conn)
                self.clients.append(client)
                logging.info(f"Client added")
                client.run()
                
            except Exception as e:
                logging.error(f"Error accepting connection: {e}")
                break

    def accept_connection(self):
        conn, addr = self.socket.accept()
        logging.info(f"Connection accepted from {addr}")
        return conn
        
    def close_socket(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        logging.info("Socket Closed")
    
    def close_clients(self):
        for client in self.clients:
            client.stop()

    def end_server(self):
        self.running = False
        self.close_socket()
        self.close_clients()
        logging.info("Server Stopped")