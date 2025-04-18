import socket
import logging
import csv

from protocol.protocol import Protocol, FileType

class Client:
  def __init__(self, server_port, id, max_batch_size):
    self.server_port = server_port
    self.id = id
    self.protocol = Protocol(max_batch_size)
    self.client_socket = None

  def run(self):
    try:
      # self.client_socket = socket.create_connection(self.server_addr)
      
      # self.send_data("movies_metadata.csv", FileType.MOVIES)
      # self.send_data("ratings.csv", FileType.RATINGS)
      self.send_data("credits.csv", FileType.CREDITS)
    
    except socket.error as err:
      logging.info(f"A socket error occurred: {err}")
    # except Exception as err:
    #   logging.info(f"An unexpected error occurred: {err}")

    finally:
      pass
        # if self.client_socket:
        #     self.client_socket.close()
        #     logging.info("Connection closed")
  
  
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
    
    print(F"ENVIANDO {len(message)}")
    
    #  self.client_socket.sendall(message)

