from multiprocessing import Process
import logging
from common.filter import Filter, FilterCommunicator
import queue


class Controller:
    def __init__(self, config, communication_config):
        self.filter = None
        self.filter_communicator = None
        self.config = config
        self.communication_config = communication_config

    def start(self):
        logging.info("Starting Filters")

        try:

            comm_queue = queue.Queue()

            filter_name = self.config["filter_name"]

            logging.info(f"Starting filter {filter_name}  with config {self.config}")

            filter_instance = Filter( comm_queue, **self.config)

            self.filter = Process(target=filter_instance.run, args=())
        
            self.filter.start()
            logging.info(f"Filter {filter_name} started with PID: {self.filter.pid}")

            # Initialize the filter communicator
            self.filter_communicator = FilterCommunicator(self.communication_config, comm_queue)

        except KeyboardInterrupt:
            logging.info("Filter stopped by user")
        except Exception as e:
            logging.error(f"Filter error: {e}")
        
        self.filter.join()        

    
    def stop(self):
        if self.filter:
            self.filter.terminate()
            self.filter.join()
            logging.info("Filter process terminated")
        else:
            logging.warning("No filter process to terminate")
        self.filter = None
    