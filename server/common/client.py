
from multiprocessing import Process
import socket
import logging
import time
import json
from protocol.protocol import Protocol
from protocol.rabbit_protocol import RabbitMQ
from protocol.utils.socket_utils import recvall

class Client:
  def __init__(self, client_sock):
    self.socket = client_sock
    self.data_controller = None
    self.result_controller = None

  def run(self):
    self.data_controller = Process(target=self.handle_connection, args=[self.socket])
    self.data_controller.start()

    self.result_controller = Process(target=self.return_results, args=[self.socket])
    self.result_controller.start()

    self.data_controller.join()
    self.result_controller.join()

  def stop(self):
    if self.data_controller.is_alive():
      self.data_controller.terminate()
    if self.result_controller.is_alive():
      self.result_controller.terminate()
  

  def handle_connection(self, conn: socket.socket):
    queue = RabbitMQ("exchange_rcv_movies", "rcv_movies", "movies_plain", "direct")
    protocol = Protocol()

    closed_socket = False
    time.sleep(10)
    while not closed_socket:
      read_amount = protocol.define_initial_buffer_size()
      buffer = bytearray()
      closed_socket = recvall(conn, buffer, read_amount)
      if closed_socket:
        return
      read_amount = protocol.define_buffer_size(buffer)
      closed_socket = recvall(conn, buffer, read_amount)
      if closed_socket:
        return
      
      msg = protocol.decode_msg(buffer)

      json_msg = protocol.encode_movies_to_json(msg)

      queue.publish(json_msg)
      # time.sleep(5)
  
  def return_results(self, conn: socket.socket):
    queue = RabbitMQ('exchange_snd_movies', 'snd_movies', 'filtered_by_2000', 'direct')
    queue.consume(self.result_controller_func)
  
  def result_controller_func(self, ch, method, properties, body):
    logging.info(f"Received result: {body}")
    protocol = Protocol()
    try:
      # data = json.loads(body)
      # msg = protocol.create_result(data)

      self.socket.sendall(body)
    except json.JSONDecodeError as e:
      logging.error(f"Failed to decode JSON: {e}")
      return
    except Exception as e:
      logging.error(f"Error processing message: {e}")
      return