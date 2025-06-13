
from protocol.rabbit_protocol import RabbitMQ
from common.aux import parse_reduce_funct, parse_final_result
import logging
import json
from protocol.protocol import Protocol
from backup import partial_results_backup


logging.getLogger("pika").setLevel(logging.ERROR)   
logging.getLogger("RabbitMQ").setLevel(logging.DEBUG)

class Reducer:
    def __init__(self, config, backup):
        self.config = config
        self.queue_rcv = None
        self.queue_snd = None
        self.is_alive = True
        self.partial_result = backup
        self.query_id = config["query_id"]
        self.reduce_by = config['reduce_by']

    def _settle_queues(self):
        self.queue_rcv = RabbitMQ(self.config["exchange_rcv"], self.config['queue_rcv_name'], self.config['routing_rcv_key'], self.config['exc_rcv_type'])
        self.queue_snd = RabbitMQ(self.config['exchange_snd'], self.config['queue_snd_name'], self.config['routing_snd_key'], self.config['exc_snd_type'])
        
    def start(self):
        """Start the reduce to consume messages from the queue."""
        self._settle_queues()
        self.queue_rcv.consume(self.callback)

    def callback(self, ch, method, properties, body):
        """Callback function to process messages."""
        logging.debug(f"Received message, with routing key: {method.routing_key}")
        self.reducer(body)

    def reducer(self, data):
        try:
            protocol = Protocol()


            msg = protocol.decode_movies_msg(data)

            if msg.finished:
                logging.info(f"ALL Finished msg received on query_id {self.query_id}")
                result = parse_final_result(self.reduce_by, self.partial_result, str(msg.client_id))
                res_proto = protocol.create_result(result, msg.client_id)

                key = f'{self.config["routing_snd_key"]}_{msg.client_id}'
                
                self.queue_snd.publish(res_proto, key)

                res_decoded = protocol.decode_result(res_proto)
                logging.info(f"Final result: {res_decoded}")

                return
            
            self.partial_result = parse_reduce_funct(data, self.reduce_by, self.partial_result, str(msg.client_id))
            partial_results_backup.make_new_backup(self.partial_result, self.config['backup_file'])
            

        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}")
            return
        except Exception as e:
            logging.error(f"ERROR processing message in {self.config['queue_rcv_name']}: {e}")
            return

    def stop(self):
        """End the reduce and close the queue."""
        if self.queue_rcv:
            self.queue_rcv.close_channel()
        if self.queue_snd:
            self.queue_snd.close_channel()
        self.is_alive = False
        logging.info("reduce Stopped")