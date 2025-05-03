import socket
import logging
from common.client import Client

logging.getLogger('pika').setLevel(logging.WARNING) 

class Server:
    def __init__(self, port, listen_backlog):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', port))
        self.socket.listen(listen_backlog)
        self.running = True
        self.clients = []
        self.next_client_id = 1  # Start client IDs from 1

    def run(self):
        while self.running:
            try:
                conn, addr = self.socket.accept()
                logging.info(f"Connection accepted from {addr}")
                
                # Create client with next available ID
                client = Client(conn, self.next_client_id)
                self.next_client_id += 1  # Increment for next client
                
                self.clients.append(client)
                logging.info(f"Client {client.client_id} added")
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