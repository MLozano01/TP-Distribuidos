import logging
import signal
import socket

class Communicator:
    def __init__(self, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', port))
        self.socket.listen(10)
        self.running = True
        # Setup signal handler for SIGTERM
        signal.signal(signal.SIGTERM, self._handle_shutdown)
        signal.signal(signal.SIGINT, self._handle_shutdown)


    def _handle_shutdown(self, _sig, _frame):
        logging.info("[Communicator] Graceful exit")
        self.stop()

    
    def start(self):
        while self.running:
            try:
                _, _ = self.socket.accept()
            except Exception as e:
                logging.error(f"[Communicator] Error accepting connection: {e}")
                

    def stop(self):
        logging.info("[Communicator] Stopping")
        self.running = False
        if self.socket:
            self.socket.close()