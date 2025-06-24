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

        self.results_received = set()
        self.total_expected = 5
        self.secuence_number = {"movies": 0, "ratings": 0, "credits": 0}
        self._entries_counter = {"ratings": 0, "credits": 0}


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
        self.forward_queue = RabbitMQ("server_to_data_controller", "forward", "", "direct")
    
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
            file_type = self.protocol.get_file_type(message)

            # Count rows in ratings/credits batches
            if file_type in ("ratings", "credits") and not self.protocol.is_end_file(message):
                added = self.protocol.count_csv_rows(message)
                self._entries_counter[file_type] += added

            # Build finished message with total count
            if self.protocol.is_end_file(message) and file_type in ("ratings", "credits"):
                total = self._entries_counter[file_type]
                enum_ft = self.protocol.string_to_file_type(file_type)
                message = self.protocol.create_inform_end_file(enum_ft, total_to_process=total)

            # Finally, attach metadata (client_id + sequence number) and send.
            message_with_metadata = self.protocol.add_metadata(
                message, self.client_id, self.secuence_number[file_type]
            )
            self.secuence_number[file_type] += 1
            self.forward_queue.publish(message_with_metadata, file_type)
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

            _type, result = self.protocol.decode_msg(msg)
            self.results_received.add(result.query_id)
            logging.info(f"Results received: {len(self.results_received)}.")
            if len(self.results_received) == self.total_expected:
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