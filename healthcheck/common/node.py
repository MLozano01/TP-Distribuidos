

class Node:

    def __init__(self, node_name, node_id, port):
        self.node_name = node_name
        self.node_id = node_id
        self.port = port

    def get_info(self):
        return self.node_name, self.node_id, self.port