import socket

class Communicator:
    def __init__(self, port):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', port))
        self.socket.listen(10)
        self.running = True

    
    def start(self):
        while self.running:
            _, _ = self.socket.accept()

    def stop(self):
        if self.socket:
            self.socket.close()