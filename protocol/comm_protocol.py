from protocol import comm_pb2

INT_LENGTH = 4
CODE_LENGTH = 1
BOOL_LENGTH = 1


FINISHED_CODE = 1
HEALTHCHECK = 2

class CommProtocol:
    def __init__(self):
        pass

    def define_initial_buffer_size(self):
        return CODE_LENGTH + INT_LENGTH

    def define_buffer_size(self, msg):
        return int.from_bytes(msg[CODE_LENGTH::], byteorder='big')


    def create_bytes(self, code, data):
        message = bytearray()
        len_msg = len(data)
        message.extend(code.to_bytes(CODE_LENGTH, byteorder='big'))
        message.extend(len_msg.to_bytes(INT_LENGTH, byteorder='big'))
        message.extend(data)
        return message
    
    def create_eof_token(self, id, client_id):
        comm_pb = comm_pb2.TokenFinished()
        comm_pb.round = 1
        comm_pb.started_by = id
        comm_pb.client_id = client_id
        token_step_pb = comm_pb.containers.add()
        token_step_pb.container_id = id
        token_step_pb.ready = True
        return self.create_bytes(FINISHED_CODE, comm_pb.SerializeToString())
    
    def complete_eof_token(self, token):
        return self.create_bytes(FINISHED_CODE, token.SerializeToString())

    def decode_msg(self, msg_buffer):
        code = int.from_bytes(msg_buffer[:CODE_LENGTH], byteorder='big')

        msg = msg_buffer[CODE_LENGTH + INT_LENGTH::]
        if code == FINISHED_CODE:
            return self.decode_token(msg)
        else:
            return None
    
    def decode_token(self, msg):
        token_msg = comm_pb2.TokenFinished()
        token_msg.ParseFromString(msg)
        return token_msg