from multiprocessing import Process, Queue
import logging
from common.aggregator import Aggregator
from common.aggr_communicator import AggregatorCommunicator

logging.getLogger("pika").setLevel(logging.ERROR)


class Controller:
    def __init__(self, config, communication_config):
        self.aggregator = None
        self.aggregator_communicator = None
        self.config = config
        self.communication_config = communication_config

    def start(self):
        logging.info("Starting Aggregator")

        try:

            comm_queue = Queue()


            aggregator_instance = Aggregator(comm_queue, **self.config)

            self.aggregator = Process(target=aggregator_instance.run, args=())
        
            self.aggregator.start()

            communicator_instance = AggregatorCommunicator(self.communication_config, comm_queue)

            self.aggregator_communicator = Process(target=communicator_instance.run, args=())
            self.aggregator_communicator.start()
            logging.info(f"Aggregator communicator started with PID: {self.aggregator_communicator.pid}")


            self.aggregator.join()        
            self.aggregator_communicator.join()

        except KeyboardInterrupt:
            logging.info("Aggregator stopped by user")
        except Exception as e:
            logging.error(f"Aggregator error: {e}")
        

    
    def stop(self):
        if self.aggregator:
            self.aggregator.terminate()
            logging.info("Aggregator process terminated")
        else:
            logging.warning("No aggregator process to terminate")
        self.aggregator = None
    