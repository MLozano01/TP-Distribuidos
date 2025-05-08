import logging
import socket

def recvall(sock: socket, buffer: bytearray, amount_to_read: int) -> bool:
  try:
    amount_read = 0
    while amount_read < amount_to_read:
      msg = sock.recv(amount_to_read - amount_read)
      if len(msg) == 0:
        return True
      buffer.extend(msg)
      amount_read += len(msg)
  except socket.error as err:
      return True
  except Exception as e:
    logging.error(f"Error reading from socket: {e}")
    return True
  return False