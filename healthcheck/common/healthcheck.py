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
        logging.info(f'Nodes: {self.nodes}')
        self.healthcheckers_count = config['hc_count']

        self.sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sckt.bind(('', self.port))
        self.sckt.listen(config['listen_backlog'])

        self.running = True

        self.check_process = Process(target=self.check)

    def run(self):
        logging.info(f"Starting the healthcheck-{self.id}")
        self.check_process.start()
        self.accept_check()

    def check(self):
        logging.info("Starting checkers")
        while self.running:
            time.sleep(5)
            try:
                self._check_workers()
                self._check_healthcheckers()
            except Exception as e:
                logging.error(f"Error {e}")

    def _check_workers(self):
        for node in self.nodes:
            try:
                con = socket.create_connection((node, self.port))
                logging.info(f"The node {node} is ok")
                con.shutdown(socket.SHUT_RDWR)
                con.close()

            except socket.error:
                logging.error(f"The node {node} needs to be revived")
                self.revive_node(node)

    def _check_healthcheckers(self):
        checking = True
        i = self.id
        while checking:
            hc_id_to_check = i % self.healthcheckers_count + 1
            if hc_id_to_check == self.id: checking= False

            hc_to_check = f"healthchecker-{hc_id_to_check}"
            logging.info(f"Checking on the node {hc_to_check}")

            try:
                socket.create_connection((hc_to_check, self.port))
                logging.info(f"The node {hc_to_check} is ok")
                checking= False

            except socket.error:
                logging.error(f"The node {hc_to_check} needs to be revived")
                self.revive_node(hc_to_check)
                i += 1

    def revive_node(self, node_info):
        try:
            node = f"TP-DISTRIBUIDOS-{node_info}"
            logging.info(f"Node to revive: {node}")
            result = subprocess.run(['docker', 'start', node_info], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            if result.returncode != 0: logging.error(f"Stderr {result.stderr}") 
            else: logging.info(f"Node: {node_info}, correctly revived")

        except Exception as e:
            logging.error(f"Error {e} with stderr {result.stderr}")

    def accept_check(self):
        logging.info("Starting Accepter")

        while self.running:
            try:
                _, addr = self.sckt.accept()
                logging.info(f"The checker in addr {addr} has checked in")
            except socket.error as err:
                logging.info(f"Socket error: {err}")
                return
            except Exception as e: 
                logging.info(f"An unexpected error has ocurred: {e}")

    def stop(self):
        self.running = False
        if not self.sckt:
            return
        self.sckt.shutdown(socket.SHUT_RDWR)
        self.sckt.close()
        self.sckt = None
        logging.info("Socket Closed")

        self.accept_process.terminate()
        self.accept_process.join()