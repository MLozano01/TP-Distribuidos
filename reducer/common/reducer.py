
from protocol.rabbit_protocol import RabbitMQ
from common.aux import parse_reduce_funct, parse_final_result
import logging
import json
from protocol.protocol import Protocol
from backup import partial_results_backup
from backup import secuence_numbers_backup

logging.getLogger("pika").setLevel(logging.ERROR)   
logging.getLogger("RabbitMQ").setLevel(logging.DEBUG)

class Reducer:
    def __init__(self, config, partial_reult_backup, batches_seen_backup):
        self.config = config
        self.queue_rcv = None
        self.queue_snd = None
        self.is_alive = True
        self.partial_result = partial_reult_backup
        self.query_id = config["query_id"]
        self.reduce_by = config['reduce_by']
        self.batches_seen = batches_seen_backup


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

            msg = self.__get_msg(data, protocol)

            if self._is_repeated(str(msg.client_id), msg.secuence_number):
                logging.info(f"Discading a repeated package of secuence number {msg.secuence_number}")
                return

            if msg.finished:
                logging.info(f"ALL Finished msg received on query_id {self.query_id}")
                result = parse_final_result(self.reduce_by, self.partial_result, str(msg.client_id))
                res_proto = protocol.create_result(result, msg.client_id)

                key = f'{self.config["routing_snd_key"]}_{msg.client_id}'
                
                self.queue_snd.publish(res_proto, key)

                self.partial_result.pop(str(msg.client_id))
                partial_results_backup.make_new_backup(self.partial_result, self.config['backup_file'])

                res_decoded = protocol.decode_result(res_proto)
                logging.info(f"Final result: {res_decoded}")

                return
            
            self.partial_result = parse_reduce_funct(data, self.reduce_by, self.partial_result, str(msg.client_id))
            partial_results_backup.make_new_backup(self.partial_result, self.config['backup_file'])
            self.batches_seen[str(msg.client_id)].append(msg.secuence_number)
            secuence_numbers_backup.make_new_secuence_number_backup(self.partial_result, str(msg.client_id))

            #TODO: Agregar un file con los clients que tienen backup actualmente de secuence_numbers -> ir actualizando igual q partial_result.


        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}")
            return
        except Exception as e:
            logging.error(f"ERROR processing message in {self.config['queue_rcv_name']}: {e}")
            return

    def _is_repeated(self, client_id, secuence_number):
        self.batches_seen.setdefault(client_id, [])

        return secuence_number in self.batches_seen[client_id]
        
        
    def _get_msg(self, data, protocol):
        aggr_msg = protocol.decode_aggr_batch(data)

        is_aggr_msg = (len(aggr_msg.aggr_row) > 0) or aggr_msg.finished

        if is_aggr_msg:
            msg = aggr_msg
        else:
            msg = protocol.decode_movies_msg(data)

        return msg

    def stop(self):
        """End the reduce and close the queue."""
        if self.queue_rcv:
            self.queue_rcv.close_channel()
        if self.queue_snd:
            self.queue_snd.close_channel()
        self.is_alive = False
        logging.info("reduce Stopped")