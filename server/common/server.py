import socket
import logging
import signal
from multiprocessing import Process
from common.client import Client

logging.getLogger('pika').setLevel(logging.WARNING)
logging.getLogger('RabbitMQ').setLevel(logging.WARNING)

class Server:
    def __init__(self, port, listen_backlog):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', port))
        self.socket.listen(listen_backlog)
        self.running = True
        self.clients = []
        self.next_client_id = 1

    def run(self):
        self._setup_signal_handlers()
        while self.running:
            try:
                conn, addr = self.socket.accept()
                logging.info(f"Connection accepted from {addr}")
                
                client_process = Process(target=self._handle_client,args=(conn, self.next_client_id))
                client_process.start()
                self.clients.append(client_process)        

                logging.info(f"Client {self.next_client_id} added")
                self.next_client_id += 1
                
            except Exception as e:
                logging.error(f"Error accepting connection: {e}")
                break

    def _handle_client(self, client_socket, client_id):
        try:
            client = Client(client_socket, client_id)
            client.run()
        except Exception as e:
            logging.error(f"Error in client process {client_id}: {e}")
        finally:
            client_socket.close()

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.end_server)
        signal.signal(signal.SIGINT, self.end_server)

    def accept_connection(self):
        conn, addr = self.socket.accept()
        logging.info(f"Connection accepted from {addr}")
        return conn
        
    def close_socket(self):
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        logging.info("Socket Closed")

    
    def close_clients(self):
        for process in self.clients:
            try:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5.0)
            except Exception as e:
                logging.error(f"Error closing client process: {e}")

    def end_server(self):
        self.running = False
        self.close_socket()
        self.close_clients()
        logging.info("Server Stopped")