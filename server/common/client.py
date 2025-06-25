from multiprocessing import Process, Event
import socket
import logging
from protocol.protocol import FileType, Protocol
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.socket_utils import recvall
import signal

AMOUNT_FILES = 3

MOVIES_KEY = 'movies'
CREDITS_KEY = 'credits'
RATINGS_KEY = 'ratings'

class Client:
    def __init__(self, client_sock, client_id, config, force_finish=False):
        self.socket = client_sock
        self.client_id = client_id
        self.force_finish = force_finish
        self.protocol = Protocol()
        self.data_controller = None
        self.result_controller = None
        self.result_queue = None

        self.stop_event = Event()
        self.results_received = set()
        self.total_expected = 5
        self.eof_sent = 0
        self.config = config

        self.secuence_number= {MOVIES_KEY:0, RATINGS_KEY:0, CREDITS_KEY:0}

        self.queues = {MOVIES_KEY:None, RATINGS_KEY:None, CREDITS_KEY:None}


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
        self.stop_consumer()
    
        try:    
            if self.data_controller and self.data_controller.is_alive():
                self.data_controller.terminate()
            if self.result_controller and self.result_controller.is_alive():
                self.result_controller.terminate()

        except Exception as e:
            logging.error(f"Error stopping client IN PROCESSES: {e}")

        logging.info(f"Client {self.client_id} stopped.")

    def stop_consumer(self):
        logging.info("Stoping consumers")
        if self.stop_event.is_set(): 
            return
        self.stop_event.set()
        
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
        self._settle_queues()
    
        closed_socket = False
        while not closed_socket:
            read_amount = self.protocol.define_initial_buffer_size()
            buffer = bytearray()
            closed_socket = recvall(conn, buffer, read_amount)
            if closed_socket:
                break
            read_amount = self.protocol.define_buffer_size(buffer)
            closed_socket = recvall(conn, buffer, read_amount)
            if closed_socket:
                break
            
            self._forward_to_data_controller(buffer)
            if self.eof_sent == AMOUNT_FILES:
                return
        if self.eof_sent < AMOUNT_FILES:
            self.handle_client_left()
        self.stop_consumer()

    def _forward_to_data_controller(self, message):
        try:
            is_eof, file_type = self.protocol.get_file_type(message)
            if is_eof:
                self.eof_sent+=1
            message_with_metadata = self.protocol.add_metadata(message, self.client_id, self.secuence_number[file_type])
            self.secuence_number[file_type] += 1 
            self.queues[file_type].publish(message_with_metadata)
        except Exception as e:
            logging.error(f"Failed to forward message to data controller: {e}")


    def handle_client_left(self):
        logging.info(f"Client disconnected. Sent {self.eof_sent} files, completing the rest")
        for i in range(self.eof_sent, AMOUNT_FILES):
            message = self.protocol.create_inform_end_file(FileType(i +1), self.force_finish)
            self._forward_to_data_controller(message)

    def _settle_queues(self):
        self.queues[MOVIES_KEY] = RabbitMQ(self.config["q_rcv_exc_movies"], self.config['q_rcv_name_movies'], self.config['q_rcv_key_movies'], self.config['type_rcv'])
        self.queues[CREDITS_KEY] = RabbitMQ(self.config["q_rcv_exc_credits"], self.config['q_rcv_name_credits'], self.config['q_rcv_key_credits'], self.config['type_rcv'])
        self.queues[RATINGS_KEY] = RabbitMQ(self.config["q_rcv_exc_raitings"], self.config['q_rcv_name_ratings'], self.config['q_rcv_key_raitings'], self.config['type_rcv'])

    def return_results(self, conn: socket.socket):
        try:
            self.result_queue = RabbitMQ('exchange_snd_results', f'result_{self.client_id}', f'results_{self.client_id}', 'direct')
            self.result_queue.consume(self.result_controller_func, stop_event=self.stop_event)
        except Exception as e:
            logging.error(f"Failed to consume results: {e}")

    def result_controller_func(self, ch, method, properties, body):
        try:
            msg = self.protocol.create_client_result(body)
            if self.socket:
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