import socket
from protocol.rabbit_protocol import RabbitMQ
import logging
from multiprocessing import Process, Event
from protocol.comm_protocol import CommProtocol
import time

from protocol.utils.socket_utils import recvall

logging.getLogger("pika").setLevel(logging.ERROR)

ROUNDS_TOKEN = 3

class FilterCommunicator:
    """
    This class is responsible for communicating between different filter components.
    It handles the sending and receiving of messages and data between filters.
    """

    def __init__(self, config, finish_receive_ntc, finish_notify_ntc, finish_receive_ctn, finish_notify_ctn, stop_event):
        self.finish_receive_ntc = finish_receive_ntc
        self.finish_notify_ntc = finish_notify_ntc
        self.finish_receive_ctn = finish_receive_ctn
        self.finish_notify_ctn = finish_notify_ctn
        self.stop_event = stop_event
        self.config = config
        self.protocol = CommProtocol()
        self.filters_acked = {}
        self.continue_running = True

        self.filter_id = int(config["id"])
        self.name = config["filter_name"]
        self.comm_port = int(config["comm_port"])
        self.filter_replicas_count = int(config["filter_replicas_count"])
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind(('', self.comm_port))
        self.socket.listen(10)
        self.finished_ring = Event()
        self.processes = []

    def clean_processes(self):
        active_processes = []
        for process in self.processes:
            if not process.is_alive():
               process.join()
            else:
                active_processes.append(process)

        self.processes = active_processes
    def run(self):
        """
        Register a filter instance with a unique name.
        """
        
        while self.continue_running:
            self.clean_processes()
            conn, addr = self.socket.accept()
            comm = Process(target=self.manage_comm, args=(conn,addr))
            comm.start()
            self.processes.append(comm)


    def manage_comm(self, sock, addr):
        logging.info("manage_comm started")
        read_amount = self.protocol.define_initial_buffer_size()
        buffer = bytearray()
        closed_socket = recvall(sock, buffer, read_amount)
        if closed_socket:
            return
        read_amount = self.protocol.define_buffer_size(buffer)
        closed_socket = recvall(sock, buffer, read_amount)
        if closed_socket:
            return
        
        sock.close()
        msg = self.protocol.decode_msg(buffer)
        logging.info(f"msg comm: {msg}")
        if msg:
            self.manage_eof_ring(msg)

    def wait_eof_confirmation(self):
        if self.finished_ring.is_set():
            logging.info("AREADY FINISHED")
            return
        self.finished_ring.wait()
        logging.info("FINISHED WAIT")

    def start_token_ring(self, client_id):
        """
        Start the token ring to send EOF.
        """
        logging.info(f"STARTING RING: {client_id}")
        self.finished_ring.clear()
        msg = self.protocol.create_eof_token(self.filter_id, client_id)
    
        try:
            if not self.send_to_ring(msg):
                self.finished_ring.set()
                return

        except Exception as e:
            logging.error(f"Error in managing EOF: {e}")
            self.finished_ring.set()

    def send_to_ring(self, msg):
        sock = self.get_socket_ring()
        if not sock:
            return False
        
        sock.sendall(msg)
        return True

    def get_socket_ring(self):
        if self.filter_replicas_count == 1:
            return None
        
        current_step = self.filter_id + 1
        while current_step != self.filter_id:
            logging.info(f"trying: {current_step} | {self.filter_id}")
            if current_step > self.filter_replicas_count:
                current_step = 1
            if current_step == self.filter_id:
                logging.info("SAME")
                return None
            try:
                logging.info(f"trying {self.name}-{current_step}")
                sock = socket.create_connection((f"{self.name}-{current_step}", self.comm_port))
                logging.info(f"sending to: {self.name}-{current_step}")
                return sock
            except:
                current_step += 1
        logging.info("NONE")
        return None

    def validate_ring(self, msg):
        for step in msg.containers:
            if not step.ready:
                return False
        return True
    
    def manage_eof_ring(self, msg):
        logging.info("Got EOF from ring")
        is_mine = False
        if msg.started_by == self.filter_id:
            msg.round += 1
            is_mine = True
            if msg.round > ROUNDS_TOKEN and self.validate_ring(msg):
                self.finished_ring.set()
                logging.info("Finished EOF")
                return
            
        step_msg = None
        for step in msg.containers:
            if step.container_id == self.filter_id:
                step_msg = step
                break
        if not step_msg:
            step_msg = msg.containers.add()
            step_msg.container_id = self.filter_id

        self.finish_notify_ctn.put([msg.client_id, False])
        done_with_client = self.finish_notify_ntc.get()
        if done_with_client == "STOP_EVENT":
            logging.info("Received STOP_EVENT in callback")
            return

        logging.info(f"{done_with_client}")
        step_msg.ready = done_with_client[1]
        logging.info(f"ROUND: {msg}")
        msg_to_send = self.protocol.complete_eof_token(msg)
        if not self.send_to_ring(msg_to_send) and is_mine:
            self.finished_ring.set()
            logging.info("Finished EOF")
            return
