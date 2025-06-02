
import logging
import socket
import subprocess
import time


class HealthCheck:
    """
    Base class for health checks.
    """

    def __init__(self, h_id, port, healthcheckers_count):
        self.port = port
        self.id = h_id
        self.other_healthcheckers = []
        self.nodes = []
        self.healthcheckers_count = healthcheckers_count
        self.running = True

    def run(self):

        while self.running:
            self._check_workers
            self._check_healthcheckers

            time.sleep(5)
    
    def _check_workers(self):
        to_delete = []
        for node in self.my_nodes:
            try:
                node_name, node_id, node_port = node.get_info()
                socket.create_connection((node_name, node_port))
                logging.info(f"The node {node_name}_{node_id} is ok")

            except socket.error:
                logging.error(f"The node {node_name}_{node_id} needs to be revived")
                self.revive_node(node_name, node_id)
        
        for node in to_delete:
            node.end()
            self.my_nodes.remove(node)
    
    def _check_healthcheckers(self):
        for i in range(len(self.other_healthcheckers)):
            pass
            
    def revive_node(self, node_name, node_id):
        try:
            node = f"'TP-DISTRIBUIDOS'+_+ {node_name} +_+ {node_id}"
            subprocess.run(['docker', 'start', node], check=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            logging.info(f"Node: {node}, correctly revived")
        except Exception as e:
            logging.error(f"Error reviving node.")

    def accept_check(self):
        pass