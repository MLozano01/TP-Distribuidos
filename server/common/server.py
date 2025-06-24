import socket
import logging
import signal
from multiprocessing import Process
from common.client import Client
from common.state_persistence import StatePersistence

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

        self.state_manager = StatePersistence('backup_server.json', node_info='server', serializer="json")
        self.status = {}
        self.restore_backup()
        
    def restore_backup(self):
        self.status = self.state_manager.load(default_factory=dict)

        logging.info(f"Status Backup Info: {self.status}")
        self.status.setdefault("current_ids", [])

        self.next_client_id = self.status.setdefault("last_id", 0) + 1

        for id in self.status["current_ids"]:
            client_process = Process(target=self._handle_client,args=(None, id, True))
            client_process.start()
            self.clients.append(client_process)

    def run(self):
        self._setup_signal_handlers()
        while self.running:
            try:
                self.__remove_closed_processes()
                
                conn, addr = self.socket.accept()
                logging.info(f"Connection accepted from {addr}")
                
                client_process = Process(target=self._handle_client,args=(conn, self.next_client_id))
                client_process.start()
                self.clients.append(client_process)   
                self.status["current_ids"].append(self.next_client_id)     
                self.status["last_id"]=self.next_client_id     
                self.state_manager.save(self.status)

                logging.info(f"Client {self.next_client_id} added")
                self.next_client_id += 1
            except Exception as e:
                logging.error(f"Error accepting connection: {e}")
                break

    def _handle_client(self, client_socket, client_id, force_finish=False):
        try:
            client = Client(client_socket, client_id, force_finish)
            client.run()
        except Exception as e:
            logging.error(f"Error in client process {client_id}: {e}")
        finally:
            client_socket.close()

    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self._graceful_exit)
        signal.signal(signal.SIGINT, self._graceful_exit)

    def accept_connection(self):
        conn, addr = self.socket.accept()
        logging.info(f"Connection accepted from {addr}")
        return conn
        
    def close_socket(self):
        if not self.socket:
            return
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        self.socket = None
        logging.info("Socket Closed")

    def __remove_closed_processes(self):
        active_processes = []
        active_clients = []
        for idx, process in enumerate(self.clients):
            if not process.is_alive():
               process.join()
               logging.info("Removed a dead client")
            else:
                active_processes.append(process)
                active_clients.append(self.status["current_ids"][idx])

        self.clients = active_processes
        self.status["current_ids"] = active_clients
        self.state_manager.save(self.status)
    
    def close_clients(self):
        for process in self.clients:
            try:
                if process.is_alive():
                    process.terminate()
                    
            except Exception as e:
                logging.error(f"Error closing client process: {e}")

        for process in self.clients:
            process.join()
        process = []

    def end_server(self):
        if not self.running:
            logging.info("Already closed")
            return
        self.running = False
        self.close_socket()
        self.close_clients()
        logging.info("Server Stopped")
    
    def _graceful_exit(self, _sig, _frame):
        self.end_server()