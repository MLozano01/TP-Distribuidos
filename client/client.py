from multiprocessing import Process
import signal
import socket
import logging

from protocol.protocol import Protocol, FileType
from protocol.utils.socket_utils import recvall
from google.protobuf.json_format import MessageToJson
import json

class Client:
  def __init__(self, server_port, id, max_batch_size):
    self.server_port = server_port
    self.id = id
    self.max_batch_size = int(max_batch_size)
    self.protocol = Protocol()
    self.client_socket = None
    self.continue_running = True
  
  def graceful_exit(self, _sig, _frame):
    logging.info("Graceful exit")
    if (self.client_socket):
      self.client_socket.close()
    self.continue_running = False

  def run(self):
    signal.signal(signal.SIGTERM, self.graceful_exit)
    signal.signal(signal.SIGINT, self.graceful_exit)
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
      self.continue_running = False
      if self.client_socket:
        self.client_socket.close()
        logging.info("Connection closed")
      if results_process.is_alive():
        results_process.terminate()
        results_process.join()
  
  
  def send_data(self, path, type):
    logging.info(f"Sending file of type {type.name}")
    with open(path, "r") as file:
      lines = []
      current_size = 0
      next(file) #header
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
    results = [None] * 5
    cant_results = 0
    while self.continue_running and cant_results < 5:
      read_amount = protocol.define_initial_buffer_size()
      buffer = bytearray()
      try:
        closed_socket = recvall(self.client_socket, buffer, read_amount)
        if closed_socket:
          return
        read_amount = protocol.define_buffer_size(buffer)
        closed_socket = recvall(self.client_socket, buffer, read_amount)
        if closed_socket:
          return
      except socket.error as err:
        logging.info(f"A socket error occurred: {err}")
        self.continue_running = False
        break
      except Exception as err:
        logging.info(f"An unexpected error occurred: {err}")
        self.continue_running = False
        break
      
      _, msg = protocol.decode_msg(buffer)
      logging.info(f"RESULT {msg}")
      if msg.query_id == 1:
        if not len(msg.result_row):
          cant_results += 1
          continue
        if not results[msg.query_id-1]:
          results[msg.query_id-1] = msg
        else:
          results[msg.query_id-1].result_row.extend(msg.result_row)
      else:
        results[msg.query_id-1] = msg
        cant_results += 1
    
    self.write_results(results)

  def write_results(self, results):
    logging.info("Writing results")
    results_filtered = list(filter(lambda x: x!= None,results))
    serialized_results = [json.loads(MessageToJson(result, preserving_proto_field_name=True)) for result in results_filtered]

    result_string = json.dumps(serialized_results, indent=2)
    with open("results.json", "w") as file:
      file.write(result_string)