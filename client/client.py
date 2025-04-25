from multiprocessing import Process
import socket
import logging

from protocol.protocol import Protocol, FileType
from protocol.utils.socket_utils import recvall


class Client:
  def __init__(self, server_port, id, max_batch_size):
    self.server_port = server_port
    self.id = id
    self.max_batch_size = int(max_batch_size)
    self.protocol = Protocol()
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
    logging.info(f"Sending file of type {type.name}")
    with open(path, "r") as file:
      lines = []
      current_size = 0
      for line in file:
        size = len(line)
        if (current_size + size) > self.max_batch_size:
          self.send_batch(lines, type)
          lines = []
          current_size = 0

        lines.append(line)
        current_size += size
        
      self.send_batch(lines, type)
      # Send the final "finished" message
      finished_message = self.protocol.create_inform_end_file(type)
      if finished_message:
          logging.info(f"Sending finished message for type {type.name}")
          self.client_socket.sendall(finished_message)
      else:
          logging.warning(f"Could not generate finished message for type {type.name}")

  def send_batch(self, lines, type):
    message = self.protocol.create_batch(type, lines)
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
      # logging.info(f"RESULT: {msg}")
