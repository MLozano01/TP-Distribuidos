
import logging
import socket
import subprocess
import time

from multiprocessing import Process

class HealthCheck:
    """
    Base class for health checks.
    """

    def __init__(self, config):
        self.port = config['port']
        self.id = config['hc_id']
        self.nodes = config['nodes_info']
        self.healthcheckers_count = config['hc_count']
        self.other_healthcheckers = []

        self.sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sckt.bind(('', self.port))

        self.running = True

        self.accept_process = Process(target=self.accept_check)

    def run(self):

        self.accept_process.start()

        while self.running:
            self._check_workers
            self._check_healthcheckers

            time.sleep(5)
    
    def _check_workers(self):
        to_delete = []
        i = 1
        for key, value in self.my_nodes:
            node = {key}-{i}
            try:
                socket.create_connection((key, self.porrt))
                logging.info(f"The node {node} is ok")
                i += 1

            except socket.error:
                logging.error(f"The node {node} needs to be revived")
                self.revive_node(node)
        
        for node in to_delete:
            node.end()
            self.my_nodes.remove(node)
    
    def _check_healthcheckers(self):
        for i in range(len(self.other_healthcheckers)):
            pass
            
    def revive_node(self, node_info):
        try:
            node = f"'TP-DISTRIBUIDOS'+-+ {node_info}"
            subprocess.run(['docker', 'start', node], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logging.info(f"Node: {node}, correctly revived")
        except Exception as e:
            logging.error(f"Error reviving node.")

    def accept_check(self):
        while self.running:
            try:
                _, addr = self.socket.accept()
                logging.info(f"The checker in addr {addr} has checked in")
            except socket.error as err:
                logging.info(f"Socket error: {err}")
            except Exception as e: 
                logging.info(f"An unexpected error has ocurred: {e}")

    def stop(self):
        self.running = False
        if not self.socket:
            return
        self.socket.shutdown(socket.SHUT_RDWR)
        self.socket.close()
        self.socket = None
        logging.info("Socket Closed")

        self.accept_process.terminate()
        self.accept_process.join()
