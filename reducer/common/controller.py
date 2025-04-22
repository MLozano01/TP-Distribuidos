from multiprocessing import Process
import logging
import common.config_init
from utils.utils import config_logger
from reducer.common.reducer import Reducer


class Controller:
    def __init__(self, config):
        self.all_reducers = []
        self.config = config

    def start(self):
        logging.info("Starting Reducers")

        for i in range(self.config["reducer_num"]):

            reducer_config = self.config[f"reducer_{i}"]

            reducer_path = reducer_config.split(",")[0]

            reducer_config_params = common.config_init.config_reducer(f'/{reducer_path}.ini')

            try:
                logging.info(f"Starting reducer {i} with config: {reducer_config_params}")

                reducer_instance = Reducer(**reducer_config_params)

                new_reducer = Process(target=reducer_instance.run, args=())
                self.all_reducers.append(new_reducer)

                new_reducer.start()
                logging.info(f"reducer {i} started with PID: {new_reducer.pid}")

            except KeyboardInterrupt:
                logging.info("reducer stopped by user")
            except Exception as e:
                logging.error(f"reducer error: {e}")
    
    def stop(self):
        for reducer in self.all_reducers:
            if reducer.is_alive():
                reducer.terminate()
                reducer.join()