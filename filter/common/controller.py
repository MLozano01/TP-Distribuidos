from multiprocessing import Process
import logging
import common.config_init
from utils.utils import config_logger
from common.filter import Filter


class Controller:
    def __init__(self, config):
        self.all_filters = []
        self.config = config

    def start(self):
        logging.info("Starting Filters")

        try:

            filter_name = self.config["filter_name"]

            logging.info(f"Starting filter {filter_name}  with config {self.config}")

            filter_instance = Filter(**self.config)

            new_filter = Process(target=filter_instance.run, args=())
            self.all_filters.append(new_filter)

            new_filter.start()
            logging.info(f"Filter {filter_name} started with PID: {new_filter.pid}")

        except KeyboardInterrupt:
            logging.info("Filter stopped by user")
        except Exception as e:
            logging.error(f"Filter error: {e}")
        
        for filt in self.all_filters:
            filt.join()
    
    def stop(self):
        for filter in self.all_filters:
            if filter.is_alive():
                filter.terminate()

    