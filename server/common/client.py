from multiprocessing import Process
import socket
import logging
from protocol.protocol import Protocol
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.socket_utils import recvall
import signal

class Client:
    def __init__(self, client_sock, client_id):
        self.socket = client_sock
        self.client_id = client_id
        self.protocol = Protocol()
        self.data_controller = None
        self.result_controller = None
        self.forward_queue = None
        self.result_queue = None

        self.results_received = 0
        self.total_expected = 5


    def run(self):
        
        self.data_controller = Process(target=self.handle_connection, args=[self.socket])
        self.data_controller.start()

        self.result_controller = Process(target=self.return_results, args=[self.socket])
        self.result_controller.start()

        self._setup_signal_handlers()

        self.data_controller.join()
        self.result_controller.join()
        logging.info(f"Finished client {self.client_id}")

    
    def _setup_signal_handlers(self):
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)


    def stop(self, _sig, _frame):
        try:
            if self.forward_queue:
                self.forward_queue.close_channel()
            if self.result_queue:
                self.result_queue.close_channel()

        except Exception as e:
            logging.error(f"Error stopping client IN QUEUES: {e}")

        try:    
            if self.data_controller and self.data_controller.is_alive():
                self.data_controller.terminate()
            if self.result_controller and self.result_controller.is_alive():
                self.result_controller.terminate()

        except Exception as e:
            logging.error(f"Error stopping client IN PROCESSES: {e}")

        if self.socket:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
                self.socket.close()
                self.socket = None
            except Exception as e:
                logging.error(f"Error closing socket: {e}")


        logging.info(f"Client {self.client_id} stopped.")

    def stop_consumer(self):
        logging.info("Stoping consumers")
        try:
            if self.forward_queue:
                self.forward_queue.close_channel()
            if self.result_queue:
                self.result_queue.close_channel()

        except Exception as e:
            logging.error(f"Error stopping client IN QUEUES: {e}")
        
        if self.socket:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
                self.socket.close()
                self.socket = None
            except Exception as e:
                logging.error(f"Error closing socket: {e}")


        logging.info(f"Client {self.client_id} stopped consuming.")

    def handle_connection(self, conn: socket.socket):
        # Initialize the forward queue for data messages
        self.forward_queue = RabbitMQ("server_to_data_controller", "", "forward", "direct")
    
        closed_socket = False
        while not closed_socket:
            read_amount = self.protocol.define_initial_buffer_size()
            buffer = bytearray()
            closed_socket = recvall(conn, buffer, read_amount)
            if closed_socket:
                return
            read_amount = self.protocol.define_buffer_size(buffer)
            closed_socket = recvall(conn, buffer, read_amount)
            if closed_socket:
                return
            
            self._forward_to_data_controller(buffer)
        self.stop_consumer()

    def _forward_to_data_controller(self, message):
        try:
            # Add client ID to the message
            message_with_client_id = self.protocol.add_client_id(message, self.client_id)
            self.forward_queue.publish(message_with_client_id)
        except Exception as e:
            logging.error(f"Failed to forward message to data controller: {e}")

    def return_results(self, conn: socket.socket):
        try:
            self.result_queue = RabbitMQ('exchange_snd_results', f'result_{self.client_id}', f'results_{self.client_id}', 'direct')
            self.result_queue.consume(self.result_controller_func)
        except Exception as e:
            logging.error(f"Failed to consume results: {e}")

    def result_controller_func(self, ch, method, properties, body):
        try:
            msg = self.protocol.create_client_result(body)
            self.socket.sendall(msg)

            decoded_msg = self.protocol.decode_result(body)

            if decoded_msg.query_id == 1 and not decoded_msg.final:
                return

            self.results_received += 1
            logging.info(f"Results received: {self.results_received}.")
            if self.results_received == self.total_expected:
                logging.info(f"{self.results_received} results received for client {self.client_id} with total {self.total_expected}.")
                logging.info(f"All results received for client {self.client_id}. Closing connection.")
                self.stop_consumer()
                return
        except socket.error as err:
            self.stop_consumer()
            return
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            return