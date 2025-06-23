from protocol.rabbit_protocol import RabbitMQ
from common_reducer.aux import parse_reduce_funct, parse_final_result
import logging
import json
from protocol.protocol import Protocol
from common.state_persistence import StatePersistence

logging.getLogger("pika").setLevel(logging.ERROR)   
logging.getLogger("RabbitMQ").setLevel(logging.DEBUG)

class Reducer:

    def __init__(self, config, partial_status_backup, secuence_number_backup, state_manager: StatePersistence):
        self.config = config
        self.queue_rcv = None
        self.queue_snd = None
        self.is_alive = True
        self.query_id = config["query_id"]
        self.reduce_by = config['reduce_by']

        self.partial_status = partial_status_backup
        self.batches_seen = secuence_number_backup
        self._state_manager = state_manager

        self.protocol = Protocol()

        # Setup signal handler for SIGTERM
        # signal.signal(signal.SIGTERM, self._handle_shutdown)

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
            msg = self._get_msg(data)

            if self._is_repeated(str(msg.client_id), str(msg.secuence_number)):
                logging.info(f"Discading a repeated package of secuence number {msg.secuence_number}")
                return

            if msg.finished:
                logging.info(f"Finished msg received on query_id {self.query_id}, with final secuence number {msg.secuence_number}")
                self.partial_status[str(msg.client_id)]["final_secuence"] = msg.secuence_number
                self._save_info(str(msg.client_id), str(msg.secuence_number))
                self._handle_finished(str(msg.client_id))
                return
            
            self._manage_msg(data, str(msg.client_id), str(msg.secuence_number))

        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON: {e}")
            return
        except Exception as e:
            logging.error(f"ERROR processing message in {self.config['queue_rcv_name']}: {e}")
            return

    def _is_repeated(self, client_id, secuence_number):
        self.batches_seen.setdefault(client_id, [])
        logging.info(f"Batches Seen Client {client_id} {self.batches_seen[client_id]}")
        logging.info(f"Secuence Number to eval {secuence_number}")
        return secuence_number in self.batches_seen[client_id]

    def _save_info(self, client_id, secuence_number):
        self.batches_seen[client_id].append(secuence_number)
        
        self._state_manager.save(self.partial_status)
        self._state_manager.save_secuence_number_data(secuence_number, client_id)

    def _manage_msg(self, data, client_id, secuence_number):
        self.partial_status.setdefault(client_id, {"results": {}, "final_secuence": None})
        self.partial_status[client_id]['results'] = parse_reduce_funct(data, self.reduce_by, self.partial_status[client_id]['results'])

        self._save_info(client_id, secuence_number)

        if self.partial_status[client_id]['final_secuence']:
            logging.info("Got a delayed message")
            self._handle_finished(client_id)

    def _clean_up(self, client_id):
        self.partial_status.pop(client_id)
        self._state_manager.save(self.partial_status)

        self.batches_seen.pop(client_id)
        self._state_manager.clean_client(client_id)

    def _handle_finished(self, client_id):
        
        if len(self.batches_seen[client_id]) != self.partial_status[client_id]["final_secuence"] + 1:
            logging.info(f'Not all messages have arrived = Batches Seen: {len(self.batches_seen[client_id])} vs Finished Secuence Number: {self.partial_status[str(client_id)]["final_secuence"]}')
            return

        result = parse_final_result(self.reduce_by, self.partial_status[client_id]['results'])
        
        res_proto = self.protocol.create_result(result, int(client_id))
        key = f'{self.config["routing_snd_key"]}_{client_id}'
        self.queue_snd.publish(res_proto, key)

        self._clean_up(client_id)

        res_decoded = self.protocol.decode_result(res_proto)
        logging.info(f"Final result: {res_decoded}")

        return

    def _get_msg(self, data):
        aggr_msg = self.protocol.decode_aggr_batch(data)
        is_aggr_msg = (len(aggr_msg.aggr_row) > 0) or aggr_msg.finished

        if is_aggr_msg:
            msg = aggr_msg
        else:
            msg = self.protocol.decode_movies_msg(data)

        return msg

    def stop(self):
        """End the reduce and close the queue."""
        if self.queue_rcv:
            self.queue_rcv.close_channel()
        if self.queue_snd:
            self.queue_snd.close_channel()
        self.is_alive = False
        logging.info("reduce Stopped")