from multiprocessing import Process
import socket
import logging
import csv

from protocol.protocol import Protocol, FileType
from protocol.utils.socket_utils import recvall

class Client:
  def __init__(self, server_port, id, max_batch_size):
    self.server_port = server_port
    self.id = id
    self.protocol = Protocol(max_batch_size)
    self.client_socket = None

  def run(self):
    try:
      self.client_socket = socket.create_connection(('server', self.server_port))
      
      results_process = Process(target=self.receive_results)
      results_process.start()
      self.send_data("movies_metadata.csv", FileType.MOVIES)
      self.send_data("credits.csv", FileType.CREDITS)
      self.send_data("ratings.csv", FileType.RATINGS)

      results_process.join()
    except socket.error as err:
      logging.info(f"A socket error occurred: {err}")
    except Exception as err:
      logging.info(f"An unexpected error occurred: {err}")

    finally:
      if self.client_socket:
        self.client_socket.close()
        logging.info("Connection closed")
  
  
  def send_data(self, path, type):
    with open(path, "r") as file:
      for data in csv.DictReader(file):
        ready_to_send = self.protocol.add_to_batch(type, data)

        if ready_to_send:
          self.send_batch(False, type)
      self.send_batch(True, type)
  
  def send_batch(self, last_send, type):
    message = self.protocol.get_batch_msg(last_send, type)
    if len(message) == 0:
      return
    
    self.client_socket.sendall(message)
  

  def receive_results(self):
    protocol = Protocol()
    closed_socket = False
    while not closed_socket:
      read_amount = protocol.define_initial_buffer_size()
      buffer = bytearray()
      closed_socket = recvall(self.client_socket, buffer, read_amount)
      if closed_socket:
        return
      read_amount = protocol.define_buffer_size(buffer)
      closed_socket = recvall(self.client_socket, buffer, read_amount)
      if closed_socket:
        return
      
      msg = protocol.decode_result(buffer)
      logging.info(f"RESULT: {msg}")
