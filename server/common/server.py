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
        self.client_processes = []
        self.next_client_id = 1
        
        # Set up signal handlers
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame):
        logging.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.running = False

    def _handle_client(self, client_socket, client_id):
        try:
            client = Client(client_socket, client_id)
            client.run()
        except Exception as e:
            logging.error(f"Error in client process {client_id}: {e}")
        finally:
            client_socket.close()

    def run(self):
        while self.running:
            try:
                # Set socket timeout to allow checking self.running periodically
                self.socket.settimeout(1.0)
                try:
                    conn, addr = self.socket.accept()
                    logging.info(f"Connection accepted from {addr}")
                    
                    # Create a new process for this client
                    client_process = Process(
                        target=self._handle_client,
                        args=(conn, self.next_client_id)
                    )
                    client_process.start()
                    
                    self.client_processes.append(client_process)
                    logging.info(f"Client {self.next_client_id} added")
                    self.next_client_id += 1
                    
                except socket.timeout:
                    # This is expected when no connection arrives within the timeout
                    continue
                
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
        logging.info("Closing all client connections...")
        for process in self.client_processes:
            try:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5.0)
            except Exception as e:
                logging.error(f"Error closing client process: {e}")

    def end_server(self):
        logging.info("Initiating server shutdown...")
        self.running = False
        self.close_socket()
        self.close_clients()
        logging.info("Server Stopped")