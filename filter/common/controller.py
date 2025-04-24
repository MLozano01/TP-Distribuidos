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

        for i in range(self.config["filter_num"]):

            filter_config = self.config[f"filter_{i}"]

            filter_path = filter_config.split(",")[0]

            filter_config_params = common.config_init.config_filter(f'/{filter_path}.ini')

            try:
                logging.info(f"Starting filter {i} with config: {filter_config_params}")

                filter_instance = Filter(**filter_config_params)

                new_filter = Process(target=filter_instance.run, args=())
                self.all_filters.append(new_filter)

                new_filter.start()
                logging.info(f"Filter {i} started with PID: {new_filter.pid}")

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